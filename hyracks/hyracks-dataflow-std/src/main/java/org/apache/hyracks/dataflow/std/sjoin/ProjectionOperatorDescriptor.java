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

public class ProjectionOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    /** Indices of fields in the input to retain in the output */
    private int[] fieldsToRetain;
    private IHyracksTaskContext ctx;

    public ProjectionOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            int[] fieldsToRetain) {
        super(spec, 1, 1);
        recordDescriptors[0] = rDesc;
        this.fieldsToRetain = fieldsToRetain;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        this.ctx = ctx;
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            /** FrameTupleAccessor for input records */
            private FrameTupleAccessor ftaIn;
            /** The appender used to write output records */
            private FrameTupleAppender appender;

            @Override
            public void open() throws HyracksDataException {
                this.ftaIn = new FrameTupleAccessor(recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
                this.appender = new FrameTupleAppender(new VSizeFrame(ProjectionOperatorDescriptor.this.ctx));
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                ftaIn.reset(buffer);
                int count = ftaIn.getTupleCount();
                for (int iTuple = 0; iTuple < count; iTuple++) {
                    FrameUtils.appendProjectionToWriter(writer, appender, ftaIn, iTuple, fieldsToRetain);
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
