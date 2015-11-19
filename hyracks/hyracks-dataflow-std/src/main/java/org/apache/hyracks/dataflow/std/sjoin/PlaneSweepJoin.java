/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.sjoin;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;

/**
 * Performs a plane sweep spatial join algorithm between two datasets of
 * rectangles. We assume that both datasets are sorted by left edge of the
 * rectangle (each one is sorted separately). If a duplicate-avoidance step is
 * needed, we assume that it will run in a different stage, i.e., we do not
 * worry about duplicates in this class.
 * @author Ahmed Eldawy
 *
 */
public class PlaneSweepJoin {
	/**Used to iterate over the left dataset*/
    private final FrameTupleAccessor accessorLeft;
    /**Used to iterate over the right dataset*/
    private final FrameTupleAccessor accessorRight;
    /**Used to write output records (pairs)*/
    private final FrameTupleAppender appender;
    /***/
    private final ITuplePairComparator tpComparator;
    /**One frame of the left dataset*/
    private final IFrame leftFrame;
    /**One frame of the right dataset*/
    private final IFrame rightFrame;
    /**A list of frames cached in-memory to improve performance*/
    private final List<ByteBuffer> outBuffers;
    /**Maximum number of frames that we can keep in-memory*/
    private final int memCapacity;
    private final IHyracksTaskContext ctx;
    private RunFileReader runFileReader;
    private int currentMemSize = 0;
    private final RunFileWriter runFileWriter;
    /**A left outer join query is needed*/
    private final boolean isLeftOuter;
    private final ArrayTupleBuilder nullTupleBuilder;
    private final IPredicateEvaluator predEvaluator;
	/**
	 * Reverse the output records upon write. Used if the inputs have been
	 * reversed to increase efficiency
	 */
    private boolean isReversed;

    /**
     * 
     * @param ctx - Hyracks task context
     * @param accessorL Tuple accessor for the left relation
     * @param accessorR Tuple accessor for the right relation
     * @param comparator 
     * @param memCapacity Maximum number of frames that can be loaded in memory
     * @param predEval Evaluator of the spatial join predicate
     * @param isLeftOuter Execute left outer join
     * @param nullWriters1
     * @throws HyracksDataException
     */
    public PlaneSweepJoin(IHyracksTaskContext ctx, FrameTupleAccessor accessorL, FrameTupleAccessor accessorR,
            ITuplePairComparator comparator, int memCapacity, IPredicateEvaluator predEval, boolean isLeftOuter,
            INullWriter[] nullWriters1) throws HyracksDataException {
        this.accessorLeft = accessorL;
        this.accessorRight = accessorR;
        this.appender = new FrameTupleAppender();
        this.tpComparator = comparator;
        this.leftFrame = new VSizeFrame(ctx);
        this.rightFrame = new VSizeFrame(ctx);
        this.appender.reset(leftFrame, true);
        this.outBuffers = new ArrayList<ByteBuffer>();
        this.memCapacity = memCapacity;
        if (memCapacity < 3) {
            throw new HyracksDataException("Not enough memory is available for Plane Sweep Spatial Join");
        }
        this.predEvaluator = predEval;
        this.isReversed = false;
        this.ctx = ctx;

        this.isLeftOuter = isLeftOuter;
        if (isLeftOuter) {
        	// Initialize nullTupleBuilder that generates null records for the left dataset
            int leftFieldCount = accessorLeft.getFieldCount();
            nullTupleBuilder = new ArrayTupleBuilder(leftFieldCount);
            DataOutput out = nullTupleBuilder.getDataOutput();
            for (int i = 0; i < leftFieldCount; i++) {
                nullWriters1[i].writeNull(out);
                nullTupleBuilder.addFieldEndOffset();
            }
        } else {
            nullTupleBuilder = null;
        }

        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                this.getClass().getSimpleName() + this.toString());
        runFileWriter = new RunFileWriter(file, ctx.getIOManager());
        runFileWriter.open();
    }

    public void cache(ByteBuffer buffer) throws HyracksDataException {
        runFileWriter.nextFrame(buffer);
    }

    public void join(ByteBuffer outerBuffer, IFrameWriter writer) throws HyracksDataException {
        if (outBuffers.size() < memCapacity - 3) {
            createAndCopyFrame(outerBuffer);
            return;
        }
        if (currentMemSize < memCapacity - 3) {
            reloadFrame(outerBuffer);
            return;
        }
        runFileReader = runFileWriter.createReader();
        runFileReader.open();
        while (runFileReader.nextFrame(rightFrame)) {
            for (ByteBuffer outBuffer : outBuffers) {
                blockJoin(outBuffer, rightFrame.getBuffer(), writer);
            }
        }
        runFileReader.close();
        currentMemSize = 0;
        reloadFrame(outerBuffer);
    }

    private void createAndCopyFrame(ByteBuffer outerBuffer) throws HyracksDataException {
        ByteBuffer outerBufferCopy = ctx.allocateFrame(outerBuffer.capacity());
        FrameUtils.copyAndFlip(outerBuffer, outerBufferCopy);
        outBuffers.add(outerBufferCopy);
        currentMemSize++;
    }

    private void reloadFrame(ByteBuffer outerBuffer) throws HyracksDataException {
        outBuffers.get(currentMemSize).clear();
        if (outBuffers.get(currentMemSize).capacity() != outerBuffer.capacity()) {
            outBuffers.set(currentMemSize, ctx.allocateFrame(outerBuffer.capacity()));
        }
        FrameUtils.copyAndFlip(outerBuffer, outBuffers.get(currentMemSize));
        currentMemSize++;
    }

    private void blockJoin(ByteBuffer outerBuffer, ByteBuffer innerBuffer, IFrameWriter writer)
            throws HyracksDataException {
        accessorRight.reset(outerBuffer);
        accessorLeft.reset(innerBuffer);
        int tupleCount0 = accessorRight.getTupleCount();
        int tupleCount1 = accessorLeft.getTupleCount();

        for (int i = 0; i < tupleCount0; ++i) {
            boolean matchFound = false;
            for (int j = 0; j < tupleCount1; ++j) {
                int c = compare(accessorRight, i, accessorLeft, j);
                if (c == 0) {
                	boolean prdEval = evaluatePredicate(i, j);
                	if (prdEval) {
                		matchFound = true;
                		appendToResults(i, j, writer);
                	}
                }
            }

            if (!matchFound && isLeftOuter) {
                final int[] ntFieldEndOffsets = nullTupleBuilder.getFieldEndOffsets();
                final byte[] ntByteArray = nullTupleBuilder.getByteArray();
                final int ntSize = nullTupleBuilder.getSize();
                FrameUtils.appendConcatToWriter(writer, appender, accessorRight, i, ntFieldEndOffsets, ntByteArray, 0,
                        ntSize);
            }
        }
    }

    private boolean evaluatePredicate(int tIx1, int tIx2) {
        if (isReversed) { //Role Reversal Optimization is triggered
            return ((predEvaluator == null) || predEvaluator.evaluate(accessorLeft, tIx2, accessorRight, tIx1));
        } else {
            return ((predEvaluator == null) || predEvaluator.evaluate(accessorRight, tIx1, accessorLeft, tIx2));
        }
    }

    private void appendToResults(int outerTupleId, int innerTupleId, IFrameWriter writer) throws HyracksDataException {
        if (!isReversed) {
            appendResultToFrame(accessorRight, outerTupleId, accessorLeft, innerTupleId, writer);
        } else {
            //Role Reversal Optimization is triggered
            appendResultToFrame(accessorLeft, innerTupleId, accessorRight, outerTupleId, writer);
        }
    }

    private void appendResultToFrame(FrameTupleAccessor accessor1, int tupleId1, FrameTupleAccessor accessor2,
            int tupleId2, IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, appender, accessor1, tupleId1, accessor2, tupleId2);
    }

    public void closeCache() throws HyracksDataException {
        if (runFileWriter != null) {
            runFileWriter.close();
        }
    }

    public void closeJoin(IFrameWriter writer) throws HyracksDataException {
        runFileReader = runFileWriter.createDeleteOnCloseReader();
        runFileReader.open();
        while (runFileReader.nextFrame(rightFrame)) {
            for (int i = 0; i < currentMemSize; i++) {
                blockJoin(outBuffers.get(i), rightFrame.getBuffer(), writer);
            }
        }
        runFileReader.close();
        outBuffers.clear();
        currentMemSize = 0;

        appender.flush(writer, true);
    }

    private int compare(FrameTupleAccessor accessor0, int tIndex0, FrameTupleAccessor accessor1, int tIndex1)
            throws HyracksDataException {
        int c = tpComparator.compare(accessor0, tIndex0, accessor1, tIndex1);
        if (c != 0) {
            return c;
        }
        return 0;
    }

    public void setIsReversed(boolean b) {
        this.isReversed = b;
    }
}
