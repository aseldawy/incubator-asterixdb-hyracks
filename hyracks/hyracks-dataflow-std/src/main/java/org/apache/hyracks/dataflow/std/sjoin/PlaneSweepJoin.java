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
import org.apache.hyracks.dataflow.std.sjoin.PlaneSweepJoinOperatorDescriptor.PlaneSweepJoinActivityNode.CacheFrameWriter;

/**
 * A helper class for the plain sweep join operator
 * 
 * @author Ahmed Eldawy
 */
public class PlaneSweepJoin {
    /**
     * Perform the plane-sweep join algorithm between two cached and sorted datasets.
     * 
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
     * @throws HyracksDataException
     */
    public static void planesweepJoin(IHyracksTaskContext ctx, CacheFrameWriter[] datasets, IFrameWriter outputWriter,
            ITuplePairComparator rx1sx1, ITuplePairComparator rx1sx2, ITuplePairComparator sx1rx2,
            IPredicateEvaluator predEvaluator) throws HyracksDataException {
        datasets[0].init();
        datasets[1].init();
        FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
        outputWriter.open();
        // The two inputs have been completely read. Do the join
        while (!datasets[0].isEOL() && !datasets[1].isEOL()) {
            // Move the sweep line to the lower of the two current records
            if (rx1sx1.compare(datasets[0].fta, datasets[0].currentRecord, datasets[1].fta,
                    datasets[1].currentRecord) < 0) {
                // Sweep line stops at an r record (dataset 0)
                // Current r record is called the active record
                // Scan records in dataset 1 until we pass the right
                // edge of the active record. i.e., while (s.x1 < r.x2)

                // Mark the current position at dataset 1 to return to it later
                datasets[1].mark();

                while (!datasets[1].isEOL() && sx1rx2.compare(datasets[1].fta, datasets[1].currentRecord,
                        datasets[0].fta, datasets[0].currentRecord) < 0) {
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
            } else {
                // Sweep line stops at an s record (dataset 1)
                // Current s record is called the active record
                // Scan records in dataset 0 until we pass the right
                // edge of the active record. i.e., while (r.x1 < s.x2)

                // Mark the current position at dataset 0 to return to it later
                datasets[0].mark();

                while (!datasets[0].isEOL() && rx1sx2.compare(datasets[0].fta, datasets[0].currentRecord,
                        datasets[1].fta, datasets[1].currentRecord) < 0) {
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
            }
        }

        appender.flush(outputWriter, true);
        outputWriter.close();
    }
}
