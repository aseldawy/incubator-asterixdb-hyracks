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
package org.apache.hyracks.tests.integration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sjoin.PlaneSweepJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.tests.util.ResultSerializerFactoryProvider;
import org.junit.Assert;
import org.junit.Test;

public class SpatialJoinTest extends AbstractIntegrationTest {

    @Test
    public void shouldWorkOnSortedDoubleDataTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects1.sorted.csv"))) };
        IFileSplitProvider rect1SplitsProvider = new ConstantFileSplitProvider(rect1Splits);
        RecordDescriptor rect1Desc = new RecordDescriptor(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                        DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });
        FileScanOperatorDescriptor rect1Scanner = new FileScanOperatorDescriptor(spec, rect1SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE }, ','),
                rect1Desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect1Scanner, NC1_ID);

        // Define second input file
        FileSplit[] rect2Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects2.sorted.csv"))) };
        IFileSplitProvider rect2SplitsProvider = new ConstantFileSplitProvider(rect2Splits);
        RecordDescriptor rect2Desc = new RecordDescriptor(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                        DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });
        FileScanOperatorDescriptor rect2Scanner = new FileScanOperatorDescriptor(spec, rect2SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE }, ','),
                rect2Desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect2Scanner, NC1_ID);

        // Define the output file
        RecordDescriptor outputDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

        PlaneSweepJoinOperatorDescriptor join = new PlaneSweepJoinOperatorDescriptor(spec, new X1X1ComparatorD(),
                new X1X2ComparatorD(), new X1X2ComparatorD(), outputDesc, 100, new SpatialOverlapPredicateD());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        // TODO consider using for debugging PlainFileWriterOperatorDescriptor
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());

        //        IFileSplitProvider outputSplits = new ConstantFileSplitProvider(new FileSplit[] {new FileSplit(NC1_ID, "test_output")});
        //        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outputSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        // Connect the two inputs
        IConnectorDescriptor input1Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input1Conn, rect1Scanner, 0, join, 0);

        IConnectorDescriptor input2Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input2Conn, rect2Scanner, 0, join, 1);

        // Connect the output
        IConnectorDescriptor outputConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(outputConn, join, 0, printer, 0);

        spec.addRoot(printer);
        //runTestAndStoreResult(spec, new File("sj_test_output"));
        runTestAndCompareResults(spec, new String[] { "data/spatial/result12.csv" });
    }

    /**
     * This predicate evaluator tests if two tuples spatially overlap. The format
     * of the two input tuples is assumed to be (id, x1, y1, x2, y2)
     */
    public static class SpatialOverlapPredicateD implements IPredicateEvaluator, Serializable {

        private static final long serialVersionUID = 5418297063092065477L;

        @Override
        public boolean evaluate(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1, int tupId1) {
            // Read the coordinates of the first rectangle
            ByteBuffer buf = fta0.getBuffer();
            double r0_x1 = buf.getDouble(fta0.getAbsoluteFieldStartOffset(tupId0, 1));
            double r0_y1 = buf.getDouble(fta0.getAbsoluteFieldStartOffset(tupId0, 2));
            double r0_x2 = buf.getDouble(fta0.getAbsoluteFieldStartOffset(tupId0, 3));
            double r0_y2 = buf.getDouble(fta0.getAbsoluteFieldStartOffset(tupId0, 4));
            // Read the coordinates of the second rectangle
            buf = fta1.getBuffer();
            double r1_x1 = buf.getDouble(fta1.getAbsoluteFieldStartOffset(tupId1, 1));
            double r1_y1 = buf.getDouble(fta1.getAbsoluteFieldStartOffset(tupId1, 2));
            double r1_x2 = buf.getDouble(fta1.getAbsoluteFieldStartOffset(tupId1, 3));
            double r1_y2 = buf.getDouble(fta1.getAbsoluteFieldStartOffset(tupId1, 4));

            // Evaluate the overlap of the two rectangles
            return r0_x2 > r1_x1 && r1_x2 > r0_x1 && r0_y2 > r1_y1 && r1_y2 > r0_y1;
        }
    }

    /**
     * Compares two records based on the coordinates of their left edges.
     * 
     * @author Ahmed Eldawy
     */
    public static class X1X1ComparatorD implements ITuplePairComparator, Serializable {

        private static final long serialVersionUID = -5880035679836791236L;

        @Override
        public int compare(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1, int tupId1)
                throws HyracksDataException {
            // Read the coordinates of the first rectangle
            ByteBuffer buf = fta0.getBuffer();
            double r0_x1 = buf.getDouble(fta0.getAbsoluteFieldStartOffset(tupId0, 1));
            // Read the coordinates of the second rectangle
            buf = fta1.getBuffer();
            double r1_x1 = buf.getDouble(fta1.getAbsoluteFieldStartOffset(tupId1, 1));

            if (r0_x1 < r1_x1)
                return -1;
            if (r0_x1 > r1_x1)
                return 1;
            return 0;
        }
    }

    /**
     * Compares the left edge of the first record to the right edge of the second record
     * 
     * @author Ahmed Eldawy
     */
    public static class X1X2ComparatorD implements ITuplePairComparator, Serializable {

        private static final long serialVersionUID = -5880035679836791236L;

        @Override
        public int compare(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1, int tupId1)
                throws HyracksDataException {
            // Read the coordinates of the first rectangle
            ByteBuffer buf = fta0.getBuffer();
            double r0_x1 = buf.getDouble(fta0.getAbsoluteFieldStartOffset(tupId0, 1));
            // Read the coordinates of the second rectangle
            buf = fta1.getBuffer();
            double r1_x2 = buf.getDouble(fta1.getAbsoluteFieldStartOffset(tupId1, 3));

            if (r0_x1 < r1_x2)
                return -1;
            if (r0_x1 > r1_x2)
                return 1;
            return 0;
        }
    }

    @Test
    public void shouldWorkOnSortedIntegerDataTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects1.sorted.csv"))) };
        IFileSplitProvider rect1SplitsProvider = new ConstantFileSplitProvider(rect1Splits);
        RecordDescriptor rect1Desc = new RecordDescriptor(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        FileScanOperatorDescriptor rect1Scanner = new FileScanOperatorDescriptor(spec, rect1SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE }, ','),
                rect1Desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect1Scanner, NC1_ID);

        // Define second input file
        FileSplit[] rect2Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects2.sorted.csv"))) };
        IFileSplitProvider rect2SplitsProvider = new ConstantFileSplitProvider(rect2Splits);
        RecordDescriptor rect2Desc = new RecordDescriptor(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        FileScanOperatorDescriptor rect2Scanner = new FileScanOperatorDescriptor(spec, rect2SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE }, ','),
                rect2Desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect2Scanner, NC1_ID);

        // Define the output file
        RecordDescriptor outputDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        PlaneSweepJoinOperatorDescriptor join = new PlaneSweepJoinOperatorDescriptor(spec, new X1X1ComparatorI(),
                new X1X2ComparatorI(), new X1X2ComparatorI(), outputDesc, 100, new SpatialOverlapPredicateI());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        // TODO consider using for debugging PlainFileWriterOperatorDescriptor
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());

        //        IFileSplitProvider outputSplits = new ConstantFileSplitProvider(new FileSplit[] {new FileSplit(NC1_ID, "test_output")});
        //        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outputSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        // Connect the two inputs
        IConnectorDescriptor input1Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input1Conn, rect1Scanner, 0, join, 0);

        IConnectorDescriptor input2Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input2Conn, rect2Scanner, 0, join, 1);

        // Connect the output
        IConnectorDescriptor outputConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(outputConn, join, 0, printer, 0);

        spec.addRoot(printer);
        //runTestAndStoreResult(spec, new File("sj_test_output_i"));
        runTestAndCompareResults(spec, new String[] { "data/spatial/result12.csv" });
    }

    /**
     * This predicate evaluator tests if two tuples spatially overlap. The format
     * of the two input tuples is assumed to be (id, x1, y1, x2, y2)
     */
    public static class SpatialOverlapPredicateI implements IPredicateEvaluator, Serializable {

        private static final long serialVersionUID = 5418297063092065477L;

        @Override
        public boolean evaluate(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1, int tupId1) {
            // Read the coordinates of the first rectangle
            ByteBuffer buf = fta0.getBuffer();
            int r0_x1 = buf.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 1));
            int r0_y1 = buf.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 2));
            int r0_x2 = buf.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 3));
            int r0_y2 = buf.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 4));
            // Read the coordinates of the second rectangle
            buf = fta1.getBuffer();
            int r1_x1 = buf.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 1));
            int r1_y1 = buf.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 2));
            int r1_x2 = buf.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 3));
            int r1_y2 = buf.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 4));

            // Evaluate the overlap of the two rectangles
            return r0_x2 > r1_x1 && r1_x2 > r0_x1 && r0_y2 > r1_y1 && r1_y2 > r0_y1;
        }
    }

    /**
     * Compares two records based on the coordinates of their left edges.
     * 
     * @author Ahmed Eldawy
     */
    public static class X1X1ComparatorI implements ITuplePairComparator, Serializable {

        private static final long serialVersionUID = -5880035679836791236L;

        @Override
        public int compare(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1, int tupId1)
                throws HyracksDataException {
            // Read the coordinates of the first rectangle
            ByteBuffer buf = fta0.getBuffer();
            int r0_x1 = buf.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 1));
            // Read the coordinates of the second rectangle
            buf = fta1.getBuffer();
            int r1_x1 = buf.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 1));

            if (r0_x1 < r1_x1)
                return -1;
            if (r0_x1 > r1_x1)
                return 1;
            return 0;
        }
    }

    /**
     * Compares the left edge of the first record to the right edge of the second record
     * 
     * @author Ahmed Eldawy
     */
    public static class X1X2ComparatorI implements ITuplePairComparator, Serializable {

        private static final long serialVersionUID = -5880035679836791236L;

        @Override
        public int compare(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1, int tupId1)
                throws HyracksDataException {
            // Read the coordinates of the first rectangle
            ByteBuffer buf = fta0.getBuffer();
            int r0_x1 = buf.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 1));
            // Read the coordinates of the second rectangle
            buf = fta1.getBuffer();
            int r1_x2 = buf.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 3));

            if (r0_x1 < r1_x2)
                return -1;
            if (r0_x1 > r1_x2)
                return 1;
            return 0;
        }
    }

    /**
     * run test and compare results without taking the sort order into consideration
     */
    @Override
    protected boolean runTestAndCompareResults(JobSpecification spec, String[] expectedFileNames) throws Exception {
        File tempFile = File.createTempFile("test_out", "");
        runTestAndStoreResult(spec, tempFile);

        // Read the actual result file returned by the job into memory
        BufferedReader actualResultFile = new BufferedReader(new FileReader(tempFile));
        String actualLine;
        List<int[]> actualResults = new ArrayList<int[]>();
        while ((actualLine = actualResultFile.readLine()) != null) {
            String[] parts = actualLine.split(",");
            int[] ids = new int[2]; // IDs of the resulting pairs
            ids[0] = Integer.parseInt(parts[0].trim());
            ids[1] = Integer.parseInt(parts[5].trim());
            actualResults.add(ids);
        }
        Comparator<int[]> idComparator = new Comparator<int[]>() {
            @Override
            public int compare(int[] o1, int[] o2) {
                return o1[0] != o2[0] ? (o1[0] - o2[0]) : (o1[1] - o2[1]);
            }
        };
        actualResults.sort(idComparator);
        actualResultFile.close();
        tempFile.delete();
        int numActualResults = actualResults.size();

        // Compare with the expected results
        int numExpectedResults = 0;
        for (int i = 0; i < expectedFileNames.length; i++) {
            BufferedReader expectedFile = new BufferedReader(new FileReader(expectedFileNames[i]));

            String expectedLine;
            while ((expectedLine = expectedFile.readLine()) != null) {
                String[] parts = expectedLine.split(",");
                int[] ids = new int[2]; // IDs of the resulting pairs
                ids[0] = Integer.parseInt(parts[0].trim());
                ids[1] = Integer.parseInt(parts[5].trim());
                Assert.assertTrue("Expected result not found: " + expectedLine,
                        Collections.binarySearch(actualResults, ids, idComparator) >= 0);
                numExpectedResults++;
            }
            expectedFile.close();
        }

        Assert.assertEquals("More actual results than expected", numExpectedResults, numActualResults);

        return true;
    }

    @Test
    public void shouldWorkOnUnsortedDataTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects3.csv"))) };
        IFileSplitProvider rect1SplitsProvider = new ConstantFileSplitProvider(rect1Splits);
        RecordDescriptor rect1Desc = new RecordDescriptor(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        FileScanOperatorDescriptor rect1Scanner = new FileScanOperatorDescriptor(spec, rect1SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE }, ','),
                rect1Desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect1Scanner, NC1_ID);

        // Sort first input file
        ExternalSortOperatorDescriptor sorter1 = new ExternalSortOperatorDescriptor(spec, 10, new int[] { 1 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                rect1Desc);

        // Define second input file
        FileSplit[] rect2Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects4.csv"))) };
        IFileSplitProvider rect2SplitsProvider = new ConstantFileSplitProvider(rect2Splits);
        RecordDescriptor rect2Desc = new RecordDescriptor(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        FileScanOperatorDescriptor rect2Scanner = new FileScanOperatorDescriptor(spec, rect2SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE }, ','),
                rect2Desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect2Scanner, NC1_ID);
        // Sort second input file
        ExternalSortOperatorDescriptor sorter2 = new ExternalSortOperatorDescriptor(spec, 10, new int[] { 1 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                rect2Desc);

        // Define the output file
        RecordDescriptor outputDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        // Create the join operator descriptor
        PlaneSweepJoinOperatorDescriptor join = new PlaneSweepJoinOperatorDescriptor(spec, new X1X1ComparatorI(),
                new X1X2ComparatorI(), new X1X2ComparatorI(), outputDesc, 100, new SpatialOverlapPredicateI());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        // Create the sink (output) operator
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        // Connect the two inputs
        IConnectorDescriptor input1Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input1Conn, rect1Scanner, 0, sorter1, 0);

        IConnectorDescriptor input2Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input2Conn, rect2Scanner, 0, sorter2, 0);

        IConnectorDescriptor input3Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input3Conn, sorter1, 0, join, 0);

        IConnectorDescriptor input4Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input4Conn, sorter2, 0, join, 1);

        // Connect the output
        IConnectorDescriptor outputConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(outputConn, join, 0, printer, 0);

        spec.addRoot(printer);
        //runTestAndStoreResult(spec, new File("sj_test_output_i"));
        runTestAndCompareResults(spec, new String[] { "data/spatial/result34.csv" });
    }

    @Test
    public void shouldWorkOnUnsortedBigDataTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects4.csv"))) };
        IFileSplitProvider rect1SplitsProvider = new ConstantFileSplitProvider(rect1Splits);
        RecordDescriptor rect1Desc = new RecordDescriptor(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        FileScanOperatorDescriptor rect1Scanner = new FileScanOperatorDescriptor(spec, rect1SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE }, ','),
                rect1Desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect1Scanner, NC1_ID);

        // Sort first input file
        ExternalSortOperatorDescriptor sorter1 = new ExternalSortOperatorDescriptor(spec, 10, new int[] { 1 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                rect1Desc);

        // Define second input file
        FileSplit[] rect2Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects5.csv"))) };
        IFileSplitProvider rect2SplitsProvider = new ConstantFileSplitProvider(rect2Splits);
        RecordDescriptor rect2Desc = new RecordDescriptor(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        FileScanOperatorDescriptor rect2Scanner = new FileScanOperatorDescriptor(spec, rect2SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE }, ','),
                rect2Desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect2Scanner, NC1_ID);
        // Sort second input file
        ExternalSortOperatorDescriptor sorter2 = new ExternalSortOperatorDescriptor(spec, 10, new int[] { 1 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                rect2Desc);

        // Define the output file
        RecordDescriptor outputDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        // Create the join operator descriptor
        PlaneSweepJoinOperatorDescriptor join = new PlaneSweepJoinOperatorDescriptor(spec, new X1X1ComparatorI(),
                new X1X2ComparatorI(), new X1X2ComparatorI(), outputDesc, 10, new SpatialOverlapPredicateI());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        // Create the sink (output) operator
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        // Connect the two inputs
        IConnectorDescriptor input1Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input1Conn, rect1Scanner, 0, sorter1, 0);

        IConnectorDescriptor input2Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input2Conn, rect2Scanner, 0, sorter2, 0);

        IConnectorDescriptor input3Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input3Conn, sorter1, 0, join, 0);

        IConnectorDescriptor input4Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input4Conn, sorter2, 0, join, 1);

        // Connect the output
        IConnectorDescriptor outputConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(outputConn, join, 0, printer, 0);

        spec.addRoot(printer);
        //runTestAndStoreResult(spec, new File("sj_test_output_i"));
        runTestAndCompareResults(spec, new String[] { "data/spatial/result45.csv" });
    }
}