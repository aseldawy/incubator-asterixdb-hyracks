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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;

/**
 * A helper class for the plain sweep join operator. An object needs to be
 * instantiated of this class and then it can be incrementally called to perform
 * the spatial join algorithm in rounds. It keeps track of its current state so
 * that it can continue from where it stopped. This allows the algorithm to run
 * incrementally on partial data without having to load the whole input datasets
 * in memory at the same time.
 * 
 * @author Ahmed Eldawy
 */
public class PlaneSweepJoin {

    private IHyracksTaskContext ctx;
    private CachedFrameWriter[] datasets;
    private IFrameWriter outputWriter;
    private ITuplePairComparator rx1sx1;
    private ITuplePairComparator rx1sx2;
    private ITuplePairComparator sx1rx2;
    private IPredicateEvaluator predEvaluator;
    /** The appender used to write output records */
    private FrameTupleAppender appender;

    /**
     * An enumerated type that stores the current state of the spatial join
     * algorithm so that it can continue from where it stopped.
     * 
     * @author Ahmed Eldawy
     */
    public enum SJ_State {
        /** The spatial join algorithm did not start yet */
        SJ_NOT_STARTED,
        /** The current record in dataset 0 is active and needs to be tested with more records in dataset 1 */
        SJ_DATASET0_ACTIVE,
        /** The current record in dataset 1 is active and needs to be tested with more records in dataset 0 */
        SJ_DATASET1_ACTIVE,
        /** No active records */
        SJ_NOT_ACTIVE,
        /** The spaital join algorithm has finished. No more work to do. */
        SJ_FINISHED,
    };

    /** Current state of the spatial join algorithm */
    private SJ_State sjState;

    /**
     * @param ctx
     *            The {@link IHyracksTaskContext} associated with the running task
     * @param datasets
     *            The two datasets that need to be joined
     * @param outputWriter
     *            The {@link IFrameWriter} used to write output frames
     * @param rx1sx1
     *            Compares the left edge of two records from the two datasets
     * @param rx1sx2
     *            Compares the left edge of an R record to the right edge of an S record
     * @param sx1rx2
     *            Compares the left edge of an S record to the right edge of an R record
     * @param predEvaluator
     *            Tests two records for overlap
     * @throws HyracksDataException
     */
    public PlaneSweepJoin(IHyracksTaskContext ctx, CachedFrameWriter[] datasets, IFrameWriter outputWriter,
            ITuplePairComparator rx1sx1, ITuplePairComparator rx1sx2, ITuplePairComparator sx1rx2,
            IPredicateEvaluator predEvaluator) throws HyracksDataException {
        this.ctx = ctx;
        this.datasets = datasets;
        this.outputWriter = outputWriter;
        this.rx1sx1 = rx1sx1;
        this.rx1sx2 = rx1sx2;
        this.sx1rx2 = sx1rx2;
        this.predEvaluator = predEvaluator;
        this.sjState = SJ_State.SJ_NOT_STARTED;
        this.appender = new FrameTupleAppender(new VSizeFrame(this.ctx));
        this.outputWriter.open();
    }

    /**
     * Perform the plane-sweep join algorithm between two cached and sorted datasets.
     * This method is thread-safe which means it can be called by two different threads
     * and it will run fine. However, only one thread will be working at a time.
     * Typically, it is called by the two input pushables which have to run in
     * two different threads. A calling thread might get blocked in this call
     * until the other input pushes enough data frames for this subroutine to
     * start. This ensures that a very fast input source paces down until the
     * other input stream can cope up with it.
     * 
     * @param fullDataset
     *            Tells this function which dataset went full and resulted in a call to
     *            this method. This tells this method that it should wait on the
     *            other dataset to become full as well before starting the join
     *            subroutine
     * @throws HyracksDataException
     * @throws InterruptedException
     */
    public synchronized void planesweepJoin(CachedFrameWriter fullDataset)
            throws HyracksDataException, InterruptedException {
        // No more work to do
        if (sjState == SJ_State.SJ_FINISHED)
            return;

        // The following block ensures that the spatial join subroutine does not start
        // unless the two in-memory buffers are full, or have reached the end
        // of their input. This has two benefits:
        // 1. If one of the two input sources is much faster than the other,
        //   it will block here so that the other input source can cope up with it.
        //   This avoids unnecessary spill to disk.
        // 2. It reduces the overall number of calls to the join subroutine as it
        //   doesn't start unless there is a big chunk of work to do.
        CachedFrameWriter otherDataset = fullDataset == datasets[0] ? datasets[1] : datasets[0];

        // Wait until the other dataset can no longer grow in memory before
        // starting the join subroutine
        if (otherDataset.canGrowInMemory()) {
            this.wait();
            // At this point, the join subroutine must have been called by the
            // other thread and we no longer need to run it. The calling method
            // will decide to either continue receiving input frames (if fullDataset.canGrowInMemory() is true),
            // or till will decide to spill some records to disk (if fullDataset.canGrowInMemory() is still false)
            return;
        }

        if (sjState == SJ_State.SJ_NOT_STARTED) {
            // First time starting the join subroutine
            datasets[0].init();
            datasets[1].init();
            this.sjState = SJ_State.SJ_NOT_ACTIVE;
        }
        // The two inputs have been completely read. Do the join
        while (!datasets[0].noMoreImmediatelyAvailableRecords() && !datasets[1].noMoreImmediatelyAvailableRecords()) {
            // Move the sweep line to the lower of the two current records
            if (rx1sx1.compare(datasets[0].fta, datasets[0].currentRecord, datasets[1].fta,
                    datasets[1].currentRecord) < 0) {
                sjState = SJ_State.SJ_DATASET0_ACTIVE;
                // Sweep line stops at an r record (dataset 0)
                // Current r record is called the active record
                // Scan records in dataset 1 until we pass the right
                // edge of the active record. i.e., while (s.x1 < r.x2)

                // Mark the current position at dataset 1 to return to it later
                datasets[1].mark();

                while (!datasets[1].noMoreImmediatelyAvailableRecords() && sx1rx2.compare(datasets[1].fta,
                        datasets[1].currentRecord, datasets[0].fta, datasets[0].currentRecord) < 0) {
                    // Check if r and s overlap
                    if (predEvaluator.evaluate(datasets[0].fta, datasets[0].currentRecord, datasets[1].fta,
                            datasets[1].currentRecord)) {
                        // Report this pair to answer
                        FrameUtils.appendConcatToWriter(outputWriter, appender, datasets[0].fta,
                                datasets[0].currentRecord, datasets[1].fta, datasets[1].currentRecord);
                    }
                    // Move to next record in s
                    datasets[1].next();
                }

                // Reset to the old position of dataset 1
                datasets[1].reset();
                // Move to the next record in dataset 0
                datasets[0].next();
                datasets[0].mark(); // Will never need to go back again
            } else {
                sjState = SJ_State.SJ_DATASET1_ACTIVE;
                // Sweep line stops at an s record (dataset 1)
                // Current s record is called the active record
                // Scan records in dataset 0 until we pass the right
                // edge of the active record. i.e., while (r.x1 < s.x2)

                // Mark the current position at dataset 0 to return to it later
                datasets[0].mark();

                while (!datasets[0].noMoreImmediatelyAvailableRecords() && rx1sx2.compare(datasets[0].fta,
                        datasets[0].currentRecord, datasets[1].fta, datasets[1].currentRecord) < 0) {
                    // Check if r and s overlap
                    if (predEvaluator.evaluate(datasets[0].fta, datasets[0].currentRecord, datasets[1].fta,
                            datasets[1].currentRecord)) {
                        // Report this pair to answer
                        FrameUtils.appendConcatToWriter(outputWriter, appender, datasets[0].fta,
                                datasets[0].currentRecord, datasets[1].fta, datasets[1].currentRecord);
                    }
                    // Move to next record in r
                    datasets[0].next();
                }

                // Reset to the old position of dataset 1
                datasets[0].reset();
                // Move to the next record in dataset 0
                datasets[1].next();
                datasets[1].mark(); // Will never need to go back again
            }
            sjState = SJ_State.SJ_NOT_ACTIVE;
        }

        // If we are completely done with one of the datasets, we don't need to continue
        if ((datasets[0].isComplete() && datasets[0].noMoreImmediatelyAvailableRecords())
                || (datasets[1].isComplete() && datasets[1].noMoreImmediatelyAvailableRecords()))
            sjState = SJ_State.SJ_FINISHED;

        if (sjState == SJ_State.SJ_FINISHED) {
            appender.flush(outputWriter, true);
            datasets[0].clear();
            datasets[1].clear();
            outputWriter.close();
        }

        // To wake up the other thread
        this.notify();
    }

    public SJ_State getState() {
        return sjState;
    }
}
