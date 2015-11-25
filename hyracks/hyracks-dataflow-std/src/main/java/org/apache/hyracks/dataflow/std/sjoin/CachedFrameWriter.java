package org.apache.hyracks.dataflow.std.sjoin;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.sjoin.PlaneSweepJoinOperatorDescriptor.PlaneSweepJoinActivityNode;

/**
 * A frame writer that caches all frames in memory and makes them available
 * for later use.
 * 
 * @author Ahmed Eldawy
 */
public class CachedFrameWriter implements IFrameWriter {
    /** All cached frames stored in a circular queue */
    private CircularQueue<ByteBuffer> cachedFrames;
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
    /** A flag that is raised after the underlying dataset is finished */
    private boolean reachedEndOfStream;
    /** The activity node that owns this cached dataset */
    private PlaneSweepJoinActivityNode owner;

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
    public CachedFrameWriter(PlaneSweepJoinOperatorDescriptor.PlaneSweepJoinActivityNode owner, IHyracksTaskContext ctx,
            RecordDescriptor rd) {
        this.owner = owner;
        this.ctx = ctx;
        this.rd = rd;
        this.reachedEndOfStream = false;
        // Initialize the in-memory store that will be used to store frames
        cachedFrames = new CircularQueue<ByteBuffer>(owner.getMemCapacity());
    }

    @Override
    public void open() throws HyracksDataException {
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        System.out.println("Thread#" + Thread.currentThread().getId() + ": Received a frame at " + this);
        // Store this buffer in memory for later use
        ByteBuffer copyBuffer = ctx.allocateFrame(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, copyBuffer);
        if (cachedFrames.isFull()) {
            // TODO run the plane-sweep algorithm in case it can free some buffer entries

            // TODO If after running the plane-sweep, we still cannot find empty entries,
            // we should start spilling records to disk.
            if (cachedFrames.isFull())
                throw new HyracksDataException("Memory full");
        }
        cachedFrames.add(copyBuffer);
    }

    @Override
    public void fail() throws HyracksDataException {
        owner.outputWriter.fail(); // Cascade the failure to the output
        cachedFrames = null; // To prevent further insertions
    }

    @Override
    public void close() throws HyracksDataException {
        // Marks the end of stream
        reachedEndOfStream = true;
        try {
            System.out.println(Thread.currentThread().getName() + ": call the SJ algorithm");
            owner.getPlaneSweepJoin().planesweepJoin(this);
            System.out.println(Thread.currentThread().getName() + ": done");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Put a mark on the current record being accessed
     */
    public void mark() {
        // This mark indicates that we do not need to get back beyond this point
        // We can shrink our queue now to accommodate new data frames
        cachedFrames.removeFirstN(this.currentFrame);
        this.markFrame = this.currentFrame = 0;
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

    /**
     * Returns true if no more cached records are available. This is true
     * if the current iterator reached the last input record in cached
     * frames even if more data frames will be later received by the
     * output.
     */
    public boolean noMoreImmediatelyAvailableRecords() {
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

    /**
     * Tells whether this cache can still grow in memory without spilling
     * to disk or not. A cached dataset can further grown in memory if
     * the following two conditions hold.
     * <ol>
     * <li>There are available buffer entries in the in-memory cache, and</li>
     * <li>The input source didn't reach its end-of-stream yet</li>
     * </ol>
     * 
     * @return
     */
    public boolean canGrowInMemory() {
        return !cachedFrames.isFull() && !reachedEndOfStream;
    }
}
