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
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;

/**
 * This Hyracks operator performs a spatial join between two inputs using the
 * plane-sweep spatial join algorithm as described in:
 * Edwin H. Jacox and Hanan Samet. 2007. Spatial join techniques.
 * ACM Trans. Database Syst. 32, 1, Article 7 (March 2007).
 * DOI=http://dx.doi.org/10.1145/1206049.1206056
 * 
 * @author Ahmed Eldawy
 */
public class PlaneSweepJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int JOIN_ACTIVITY_ID = 0;

    private static final long serialVersionUID = 7908488449729834977L;

    /** The spatial join predicate */
    private IPredicateEvaluator predEvaluator;

    /** Compares the left edge of two records r and s */
    private ITuplePairComparator rx1sx1;

    /** Compares the left edge of an R record to the right edge of an S record */
    private ITuplePairComparator rx1sx2;

    /** Compares the left edge of an S record to the right edge of an R record */
    private ITuplePairComparator sx1rx2;

    /**
     * Constructs a new plane sweep join operator. The input is two datasets,
     * R and S, and the output is every pair of records (r, s) where the join
     * predicate is true.
     * 
     * @param spec
     *            Job specification
     * @param rx1sx1
     *            Compares the left edge of two records from R and S.
     *            Used to run the plane-sweep join algorithm.
     * @param rx1sx2
     *            Compares the left edge of an R record to the right edge of an
     *            S record. This comparator is used to run the plane-sweep algorithm.
     * @param sx1rx2
     *            Compares the left edge of an S record to the right edge of an
     *            R record. This comparator is used to run the plane-sweep algorithm.
     * @param outputDescriptor
     * @param predEvaluator
     */
    public PlaneSweepJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, ITuplePairComparator rx1sx1,
            ITuplePairComparator rx1sx2, ITuplePairComparator sx1rx2, RecordDescriptor outputDescriptor,
            IPredicateEvaluator predEvaluator) {
        super(spec, 2, 1);
        this.rx1sx1 = rx1sx1;
        this.rx1sx2 = rx1sx2;
        this.sx1rx2 = sx1rx2;
        // TODO can I create the output record descriptor here by concatenating
        // the two input descriptors, or do I have to take it as input?
        this.recordDescriptors[0] = outputDescriptor;
        this.predEvaluator = predEvaluator;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId joinId = new ActivityId(getOperatorId(), JOIN_ACTIVITY_ID);
        PlaneSweepJoinActivityNode joinActivity = new PlaneSweepJoinActivityNode(joinId);

        builder.addActivity(this, joinActivity);
        builder.addSourceEdge(0, joinActivity, 0);
        builder.addSourceEdge(1, joinActivity, 1);
        builder.addTargetEdge(0, joinActivity, 0);
    }

    /**
     * The core spatial join code is embedded in this activity node.
     * 
     * @author Ahmed Eldawy
     */
    public class PlaneSweepJoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1433873095538085055L;

        /** The writer which is used to write output records */
        protected IFrameWriter outputWriter;
        /** Descriptor of output records */
        protected RecordDescriptor outputRecordDescriptor;

        /** A cached version of the two input datasets */
        protected CacheFrameWriter[] datasets;

        /** Number of inputs that have been completely read (0, 1 or 2) */
        protected int numInputsComplete;

        /** Hyracks context of the underlying job */
        protected IHyracksTaskContext ctx;

        /**
         * Record descriptor for the first input dataset. This descriptor
         * understands how a tuple is encoded in binary data and can parse it
         */
        private RecordDescriptor[] rds;

        public PlaneSweepJoinActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int numPartitions)
                        throws HyracksDataException {
            this.ctx = ctx;
            this.datasets = new CacheFrameWriter[2];
            this.rds = new RecordDescriptor[datasets.length];
            for (int i = 0; i < datasets.length; i++) {
                rds[i] = recordDescProvider.getInputRecordDescriptor(getActivityId(), i);
                datasets[i] = new CacheFrameWriter(ctx, rds[i]);
            }

            IOperatorNodePushable op = new AbstractOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    // TODO Auto-generated method stub

                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    // TODO Auto-generated method stub

                }

                @Override
                public int getInputArity() {
                    return 2;
                }

                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                        throws HyracksDataException {
                    outputWriter = writer;
                    outputRecordDescriptor = recordDesc;
                }

                @Override
                public IFrameWriter getInputFrameWriter(int index) {
                    return datasets[index];
                }
            };

            return op;
        }

        public void inputComplete() throws HyracksDataException {
            // A notification that one of the inputs has been completely read
            numInputsComplete++;
            if (numInputsComplete == 2) {
                PlaneSweepJoin.planesweepJoin(ctx, datasets, outputWriter, rx1sx1, rx1sx2, sx1rx2, predEvaluator);
            }
        }

        /**
         * A frame writer that caches all frames in memory and makes them available
         * for later use.
         * 
         * @author Ahmed Eldawy
         */
        public class CacheFrameWriter implements IFrameWriter {
            /** All cached frames */
            private List<ByteBuffer> cachedFrames;
            /** Hyracks context of the running job */
            private IHyracksTaskContext ctx;

            /** The current frame being accessed */
            private int currentFrame;
            /** The index of the record inside the current frame being accessed */
            protected int currentRecord;
            /** {@link FrameTupleAccessor} to iterate over records */
            protected FrameTupleAccessor fta;
            /** {@link RecordDescriptor} for cached data */
            private RecordDescriptor rd;

            /** The index of the marked frame */
            private int markFrame;
            /** The index of the marked record inside the marked frame */
            private int markRecord;

            /**
             * Creates a frame writer that caches all records in memory
             * 
             * @param ctx
             *            Hyracks context of the job being run
             * @param notifiable
             *            Used to notify the caller of end of stream
             * @param rd
             *            {@link RecordDescriptor} of cached data
             */
            public CacheFrameWriter(IHyracksTaskContext ctx, RecordDescriptor rd) {
                this.ctx = ctx;
                this.rd = rd;
            }

            @Override
            public void open() throws HyracksDataException {
                // Initialize the in-memory store that will be used to store frames
                cachedFrames = new ArrayList<ByteBuffer>();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // Store this buffer in memory for later use
                ByteBuffer copyBuffer = ctx.allocateFrame(buffer.capacity());
                FrameUtils.copyAndFlip(buffer, copyBuffer);
                cachedFrames.add(copyBuffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                outputWriter.fail(); // Cascade the failure to the output
                cachedFrames = null; // To prevent further insertions
            }

            @Override
            public void close() throws HyracksDataException {
                // Notify its creator that it has been closed
                inputComplete();
            }

            /**
             * Put a mark on the current record being accessed
             */
            public void mark() {
                this.markFrame = this.currentFrame;
                this.markRecord = this.currentRecord;
            }

            /**
             * Reset the iterator to the last marked position
             */
            public void reset() {
                if (this.currentFrame != this.markFrame) {
                    this.currentFrame = this.markFrame;
                    // Move to this frame
                    this.fta.reset(this.cachedFrames.get(currentFrame));
                }
                this.currentRecord = this.markRecord;
            }

            /** Initialize iteration over records */
            public void init() {
                this.currentFrame = this.markFrame = 0;
                this.currentRecord = this.markRecord = 0;
                this.fta = new FrameTupleAccessor(rd);
                this.fta.reset(this.cachedFrames.get(currentFrame));
                // Skip over empty frames, if any
                // Notice, initially currentRecord is zero
                while (currentRecord >= fta.getTupleCount() && currentFrame < cachedFrames.size()) {
                    currentFrame++; // Move to next frame
                    if (currentFrame < cachedFrames.size())
                        this.fta.reset(this.cachedFrames.get(currentFrame));
                }
            }

            /** Returns true if end-of-list has been reached */
            public boolean isEOL() {
                return this.currentFrame >= this.cachedFrames.size();
            }

            public void next() {
                this.currentRecord++;
                // Skip to next frame if reached end of current frame
                while (currentRecord >= fta.getTupleCount() && currentFrame < cachedFrames.size()) {
                    currentFrame++;
                    if (currentFrame < cachedFrames.size()) {
                        // Move to next data frame
                        this.fta.reset(this.cachedFrames.get(currentFrame));
                        currentRecord = 0;
                    }
                }
            }
        }

    }

}
