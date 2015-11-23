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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
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
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
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

    /** Compares two R records based on their left edge */
    private ITuplePairComparator rx1rx1;

    /** Compares two S records based on their left edge */
    private ITuplePairComparator sx1sx1;

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
     * @param rx1rx1
     *            Compares two records r1 and r2 that both belong to R based
     *            on their left edge. This comparator is used to sort the R dataset.
     * @param sx1sx1
     *            Compares two records s1 and s2 that both belong to S based
     *            on their left edge. This comparator is used to sort the S dataset.
     * @param rx1sx2
     *            Compares the left edge of an R record to the right edge of an
     *            S record. This comparator is used to run the plane-sweep algorithm.
     * @param sx1rx2
     *            Compares the left edge of an S record to the right edge of an
     *            R record. This comparator is used to run the plane-sweep algorithm.
     * @param outputDescriptor
     * @param predEvaluator
     */
    public PlaneSweepJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, ITuplePairComparator rx1rx1,
            ITuplePairComparator sx1sx1, ITuplePairComparator rx1sx2, ITuplePairComparator sx1rx2,
            RecordDescriptor outputDescriptor, IPredicateEvaluator predEvaluator) {
        super(spec, 2, 1);
        this.rx1rx1 = rx1rx1;
        this.sx1sx1 = sx1sx1;
        this.rx1sx2 = rx1sx2;
        this.sx1rx2 = sx1rx2;
        this.recordDescriptors[0] = outputDescriptor;
        this.predEvaluator = predEvaluator;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId joinId = new ActivityId(getOperatorId(), JOIN_ACTIVITY_ID);
        PlaneSweepJoinActivityNode joinActivity = new PlaneSweepJoinActivityNode(joinId, predEvaluator);

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
    public static class PlaneSweepJoinActivityNode extends AbstractActivityNode implements Notifiable {
        private static final long serialVersionUID = 1433873095538085055L;

        /** The writer which is used to write output records */
        protected IFrameWriter outputWriter;
        /** Descriptor of output records */
        protected RecordDescriptor outputRecordDescriptor;

        /** A cached version of the two datasets */
        protected CacheFrameWriter[] inputDatasets;

        /** Number of inputs that have been completely read (0, 1 or 2) */
        protected int numInputsComplete;

        /** Hyracks context of the underlying job */
        protected IHyracksTaskContext ctx;

        /**
         * The predicate evaluator that tests if two tuples in the two datasets
         * satisfy the spatial join predicate
         */
        private IPredicateEvaluator predEvaluator;

        /**
         * Record descriptor for the first input dataset. This descriptor
         * understands how a tuple is encoded in binary data and can parse it
         */
        private RecordDescriptor rd0;

        private RecordDescriptor rd1;

        public PlaneSweepJoinActivityNode(ActivityId id, IPredicateEvaluator predEvaluator) {
            super(id);
            this.predEvaluator = predEvaluator;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int numPartitions)
                        throws HyracksDataException {
            rd0 = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);
            rd1 = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 1);
            this.ctx = ctx;
            inputDatasets = new CacheFrameWriter[2];
            for (int i = 0; i < inputDatasets.length; i++)
                inputDatasets[i] = new CacheFrameWriter(ctx, this);

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
                    // TODO Should I define the output record descriptor here or use
                    // it as an input?
                    outputRecordDescriptor = recordDesc;
                }

                @Override
                public IFrameWriter getInputFrameWriter(int index) {
                    return inputDatasets[index];
                }
            };

            return op;
        }

        @Override
        public void notify(Object notified) throws HyracksDataException {
            // A notification that one of the inputs has been completely read
            numInputsComplete++;
            if (numInputsComplete == 2) {
                FrameTupleAccessor accessor0 = new FrameTupleAccessor(rd0);
                FrameTupleAccessor accessor1 = new FrameTupleAccessor(rd1);
                FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                outputWriter.open();
                // The two inputs have been completely read. Do the join
                // XXX run a nest loop join, for now
                for (ByteBuffer data0 : inputDatasets[0].cachedFrames) {
                    for (ByteBuffer data1 : inputDatasets[1].cachedFrames) {
                        // Compare records in frame1 with records in frame2
                        accessor0.reset(data0);
                        accessor1.reset(data1);

                        for (int i0 = 0; i0 < accessor0.getTupleCount(); i0++) {
                            for (int i1 = 0; i1 < accessor1.getTupleCount(); i1++) {
                                if (predEvaluator.evaluate(accessor0, i0, accessor1, i1)) {
                                    // Found a matching pair, write them to the output
                                    FrameUtils.appendConcatToWriter(outputWriter, appender, accessor0, i0, accessor1,
                                            i1);
                                    System.out.println("Found a matching pair");
                                }
                            }
                        }
                    }
                }
                appender.flush(outputWriter, true);
                outputWriter.close();
            }
        }

    }

}
