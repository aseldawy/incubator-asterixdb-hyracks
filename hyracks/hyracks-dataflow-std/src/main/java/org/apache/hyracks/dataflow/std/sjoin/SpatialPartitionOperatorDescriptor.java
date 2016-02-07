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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * This operator partitions input records according to a given {@link ISpatialPartitioner}.
 * It replicates each input record to all overlapping cells as defined by the partitioner. The input is a set of records
 * and the output is the input with an additional column that indicates all matching cells for
 * each input record. If a record matches multiple cells, this record is replicated in the
 * output for each matching cell.
 * 
 * @author Ahmed Eldawy
 */
public class SpatialPartitionOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = -4136989373733113932L;

    /** The single activity in this operator that adds a column with matching cell IDs */
    private static final int PARTITION_ACTIVITY_ID = 0;

    /** Spatial partition function */
    private ISpatialPartitioner partitionFunction;

    public SpatialPartitionOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outputDescriptor,
            ISpatialPartitioner partitionFunction) {
        super(spec, 1, 1);
        this.recordDescriptors[0] = outputDescriptor;
        this.partitionFunction = partitionFunction;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId partitionID = new ActivityId(getOperatorId(), PARTITION_ACTIVITY_ID);
        PartitionActivityNode partitionActivity = new PartitionActivityNode(partitionID);
        builder.addActivity(this, partitionActivity);
        builder.addSourceEdge(0, partitionActivity, 0);
        builder.addTargetEdge(0, partitionActivity, 0);
    }

    public class PartitionActivityNode extends AbstractActivityNode {

        private static final long serialVersionUID = -4812345575031990929L;

        /** Context of the running Hyracks task */
        private IHyracksTaskContext ctx;

        public PartitionActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                        throws HyracksDataException {
            this.ctx = ctx;

            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                /** FrameTupleAccessor to write output records */
                private FrameTupleAccessor ftaOut;
                /** FrameTupleAccessor for parsing input records */
                private FrameTupleAccessor ftaIn;
                private int[] matchingCells;
                /** All matching cells in a byte array buffer to make it easier to append to the output */
                private ByteBuffer matchingCellBuffer;
                /** The appender used to write output records */
                private FrameTupleAppender appender;

                @Override
                public void open() throws HyracksDataException {
                    this.ftaIn = new FrameTupleAccessor(
                            recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
                    this.ftaOut = new FrameTupleAccessor(recordDescriptors[0]);
                    this.writer.open();
                    // Initial capacity for number of matching cells
                    this.matchingCells = new int[16];
                    this.matchingCellBuffer = ByteBuffer.allocate(4);
                    this.appender = new FrameTupleAppender(new VSizeFrame(PartitionActivityNode.this.ctx));
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ftaIn.reset(buffer);
                    int count = ftaIn.getTupleCount();
                    for (int iTuple = 0; iTuple < count; iTuple++) {
                        int size = partitionFunction.getMatchingCells(ftaIn, iTuple, matchingCells);
                        if (size > matchingCells.length) {
                            // Need to resize the array of matching cells
                            matchingCells = new int[size * 2];
                            size = partitionFunction.getMatchingCells(ftaIn, iTuple, matchingCells);
                        }
                        final int[] cellIDField = { 0 };
                        for (int iCell = 0; iCell < size; iCell++) {
                            this.matchingCellBuffer.putInt(0, matchingCells[iCell]);
                            if (!appender.appendConcat(cellIDField, matchingCellBuffer.array(), 0, 4, ftaIn, iTuple)) {
                                // Flush and write next
                                appender.flush(writer, true);
                                if (!appender.appendConcat(cellIDField, matchingCellBuffer.array(), 0, 4, ftaIn,
                                        iTuple)) {
                                    throw new HyracksDataException("The output cannot be fit into a frame.");
                                }
                            }
                            //FrameUtils.appendToWriter(writer, appender, matchingCellBuffer.array(), 0, 4);
                            //FrameUtils.appendToWriter(writer, appender, ftaIn, iTuple);
                        }
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                @Override
                public void close() throws HyracksDataException {
                    // Flush any remainder in the appender
                    appender.flush(writer, false);
                    writer.close();
                }
            };
        }
    }
}
