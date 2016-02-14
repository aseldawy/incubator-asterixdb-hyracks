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
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * A filter operator that applies a given predicate to filter out non-matching records.
 * 
 * @author Ahmed Eldawy
 */
public class FilterOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = -516636092552514479L;

    /** The predicate to apply to input records */
    private IFilter predicate;

    /** The Hyracks context of the underlying task */
    private IHyracksTaskContext ctx;

    public FilterOperatorDescriptor(IOperatorDescriptorRegistry spec, final RecordDescriptor outDesc,
            IFilter predicate) {
        super(spec, 1, 1);
        recordDescriptors[0] = outDesc;
        this.predicate = predicate;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                    throws HyracksDataException {
        this.ctx = ctx;

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            /** FrameTupleAccessor for input records */
            private FrameTupleAccessor ftaIn;
            /** The appender used to write output records */
            private FrameTupleAppender appender;

            @Override
            public void open() throws HyracksDataException {
                this.ftaIn = new FrameTupleAccessor(recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
                this.appender = new FrameTupleAppender(new VSizeFrame(FilterOperatorDescriptor.this.ctx));
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                ftaIn.reset(buffer);
                int count = ftaIn.getTupleCount();
                for (int iTuple = 0; iTuple < count; iTuple++) {
                    if (predicate.evaluate(ftaIn, iTuple))
                        FrameUtils.appendToWriter(writer, appender, ftaIn, iTuple);
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
