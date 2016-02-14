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
import org.apache.hyracks.dataflow.std.sjoin.FilterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sjoin.IFilter;
import org.apache.hyracks.dataflow.std.sjoin.ISpatialPartitioner;
import org.apache.hyracks.dataflow.std.sjoin.PlaneSweepJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sjoin.ProjectionOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sjoin.SpatialPartitionOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.tests.util.ResultSerializerFactoryProvider;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test various parts of spatial join.
 * 
 * @author Ahmed Eldawy
 */
public class SpatialJoinTest extends AbstractIntegrationTest {

    @Test
    public void shouldWorkOnSortedDoubleDataTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects1.sorted.txt"))) };
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
                new FileSplit(NC2_ID, new FileReference(new File("data/spatial/rects2.sorted.txt"))) };
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
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect2Scanner, NC2_ID);

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
        runTestAndCompareResults(spec, new String[] { "data/spatial/result12.txt" });
    }

    @Test
    public void shouldWorkOnSortedIntegerDataTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects1.sorted.txt"))) };
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
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects2.sorted.txt"))) };
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
        runTestAndCompareResults(spec, new String[] { "data/spatial/result12.txt" });
    }

    @Test
    public void shouldWorkOnUnsortedDataTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects3.txt"))) };
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
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects4.txt"))) };
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
        runTestAndCompareResults(spec, new String[] { "data/spatial/result34.txt" });
    }

    @Test
    public void shouldWorkOnUnsortedBigDataTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects4.txt"))) };
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
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects5.txt"))) };
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
        runTestAndCompareResults(spec, new String[] { "data/spatial/result45.txt" });
    }

    /**
     * Partitions an input using a uniform grid.
     * 
     * @throws Exception
     */
    @Test
    public void shouldWorkWithGridPartitionerTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects1.sorted.txt"))) };
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

        // Define the output file, cellID, recordID, x1, y1, x2, y2
        RecordDescriptor outputDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        UniformGridPartitionerI gridPartitioner = new UniformGridPartitionerI(0, 0, 80, 80, 2, 2);

        // Project a cell ID column to each record
        SpatialPartitionOperatorDescriptor partitionOp = new SpatialPartitionOperatorDescriptor(spec, outputDesc,
                gridPartitioner);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        // Create the sink (output) operator
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        // Connect input and output
        IConnectorDescriptor input1Conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(input1Conn, rect1Scanner, 0, partitionOp, 0);
        IConnectorDescriptor outputConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(outputConn, partitionOp, 0, printer, 0);

        spec.addRoot(printer);
        //runTestAndStoreResult(spec, new File("sj_test_output_i"));
        runTestAndCompareResults(spec, new String[] { "data/spatial/rects1.partitioned.txt" });
    }

    /**
     * Partitions an input using a uniform grid and joins the records in each grid cell.
     * 
     * @throws Exception
     */
    @Test
    public void shouldRunPBSMTest() throws Exception {
        JobSpecification spec = new JobSpecification();
        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects1.sorted.txt"))) };
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
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects2.sorted.txt"))) };
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

        // Define the format of partitioned data: cellID, recordID, x1, y1, x2, y2
        RecordDescriptor partitioned1Desc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        RecordDescriptor partitioned2Desc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        // Define the output file: rCID, rID, rx1, ry1, rx2, ry2, sCID, sID, sx1, sy1, sx2, sy2
        RecordDescriptor joinedDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        // Final output (remove cell IDs)t: rID, rx1, ry1, rx2, ry2, sID, sx1, sy1, sx2, sy2
        RecordDescriptor outputDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        UniformGridPartitionerI gridPartitioner1 = new UniformGridPartitionerI(0, 0, 80, 80, 2, 2);
        UniformGridPartitionerI gridPartitioner2 = new UniformGridPartitionerI(0, 0, 80, 80, 2, 2);

        // Project a cell ID column to each record
        SpatialPartitionOperatorDescriptor partitionOp1 = new SpatialPartitionOperatorDescriptor(spec, partitioned1Desc,
                gridPartitioner1);
        SpatialPartitionOperatorDescriptor partitionOp2 = new SpatialPartitionOperatorDescriptor(spec, partitioned2Desc,
                gridPartitioner2);

        // Sort the two inputs based on (cellID, X1)
        ExternalSortOperatorDescriptor sorter1 = new ExternalSortOperatorDescriptor(spec, 10, new int[] { 0, 2 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                partitioned1Desc);
        ExternalSortOperatorDescriptor sorter2 = new ExternalSortOperatorDescriptor(spec, 10, new int[] { 0, 2 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                partitioned2Desc);

        // Plane-sweep join operator
        PlaneSweepJoinOperatorDescriptor join = new PlaneSweepJoinOperatorDescriptor(spec, new CellIDX1X1ComparatorI(),
                new CellIDX1X2ComparatorI(), new CellIDX1X2ComparatorI(), joinedDesc, 10,
                new SpatialOverlapCellPredicateI());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        ProjectionOperatorDescriptor projectOp = new ProjectionOperatorDescriptor(spec, outputDesc,
                new int[] { 1, 2, 3, 4, 5, 7, 8, 9, 10, 11 });

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        // Create the sink (output) operator
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        // Connect input and output
        spec.connect(new OneToOneConnectorDescriptor(spec), rect1Scanner, 0, partitionOp1, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), rect2Scanner, 0, partitionOp2, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), partitionOp1, 0, sorter1, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), partitionOp2, 0, sorter2, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), sorter1, 0, join, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorter2, 0, join, 1);

        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, projectOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), projectOp, 0, printer, 0);

        spec.addRoot(printer);
        //runTestAndStoreResult(spec, new File("sj_test_output_i"));
        runTestAndCompareResults(spec, new String[] { "data/spatial/result12.txt" });
    }

    /**
     * Partitions an input using a uniform grid and joins the records in each grid cell.
     * It also tests if the duplicate avoidance step works correctly.
     * 
     * @throws Exception
     */
    @Test
    public void shouldRunPBSMWithDuplicateAvoidanceTest() throws Exception {

        JobSpecification spec = new JobSpecification();
        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects1.sorted.txt"))) };
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
                new FileSplit(NC1_ID, new FileReference(new File("data/spatial/rects2.sorted.txt"))) };
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

        // Define the format of partitioned data: cellID, recordID, x1, y1, x2, y2
        RecordDescriptor partitioned1Desc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        RecordDescriptor partitioned2Desc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        // Define the output file: rCID, rID, rx1, ry1, rx2, ry2, sCID, sID, sx1, sy1, sx2, sy2
        RecordDescriptor joinedDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        // Final output (remove cell IDs)t: rID, rx1, ry1, rx2, ry2, sID, sx1, sy1, sx2, sy2
        RecordDescriptor outputDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        UniformGridPartitionerI gridPartitioner1 = new UniformGridPartitionerI(0, 0, 80, 80, 4, 4);
        UniformGridPartitionerI gridPartitioner2 = new UniformGridPartitionerI(0, 0, 80, 80, 4, 4);

        // Project a cell ID column to each record
        SpatialPartitionOperatorDescriptor partitionOp1 = new SpatialPartitionOperatorDescriptor(spec, partitioned1Desc,
                gridPartitioner1);
        SpatialPartitionOperatorDescriptor partitionOp2 = new SpatialPartitionOperatorDescriptor(spec, partitioned2Desc,
                gridPartitioner2);

        // Sort the two inputs based on (cellID, X1)
        ExternalSortOperatorDescriptor sorter1 = new ExternalSortOperatorDescriptor(spec, 10, new int[] { 0, 2 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                partitioned1Desc);
        ExternalSortOperatorDescriptor sorter2 = new ExternalSortOperatorDescriptor(spec, 10, new int[] { 0, 2 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                partitioned2Desc);

        // Plane-sweep join operator
        PlaneSweepJoinOperatorDescriptor join = new PlaneSweepJoinOperatorDescriptor(spec, new CellIDX1X1ComparatorI(),
                new CellIDX1X2ComparatorI(), new CellIDX1X2ComparatorI(), joinedDesc, 10,
                new SpatialOverlapCellPredicateI());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        FilterOperatorDescriptor dupAvoidanceOp = new FilterOperatorDescriptor(spec, joinedDesc,
                new ReferencePointI(gridPartitioner1));

        ProjectionOperatorDescriptor projectOp = new ProjectionOperatorDescriptor(spec, outputDesc,
                new int[] { 1, 2, 3, 4, 5, 7, 8, 9, 10, 11 });

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        // Create the sink (output) operator
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        // Connect input and output
        spec.connect(new OneToOneConnectorDescriptor(spec), rect1Scanner, 0, partitionOp1, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), rect2Scanner, 0, partitionOp2, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), partitionOp1, 0, sorter1, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), partitionOp2, 0, sorter2, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), sorter1, 0, join, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorter2, 0, join, 1);

        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, dupAvoidanceOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), dupAvoidanceOp, 0, projectOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), projectOp, 0, printer, 0);

        spec.addRoot(printer);
        runTestAndStoreResult(spec, new File("sj_test_output_i"));
        runTestAndCompareResults(spec, new String[] { "data/spatial/result12.txt" });
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
            return r0_x1 - r1_x1;
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
            return r0_x1 - r1_x2;
        }
    }

    /**
     * A grid partitioner for records represented as five integers in the format:
     * id, x1, y1, x2, y2
     * 
     * @author Ahmed Eldawy
     */
    public static class UniformGridPartitionerI implements ISpatialPartitioner, Serializable {

        private static final long serialVersionUID = 1L;
        /** Coordinates of the MBR of the input space domain */
        private int xmin;
        private int ymin;
        private int xmax;
        private int ymax;
        /** Number of columns */
        private int cols;
        /** Number of rows in the grid */
        private int rows;

        public UniformGridPartitionerI(int xmin, int ymin, int xmax, int ymax, int cols, int rows) {
            this.xmin = xmin;
            this.ymin = ymin;
            this.xmax = xmax;
            this.ymax = ymax;
            this.cols = cols;
            this.rows = rows;
        }

        @Override
        public int getMatchingCells(IFrameTupleAccessor fta, int tupId, int[] matchingCells) {
            // Read the coordinates of the rectangle
            ByteBuffer buf = fta.getBuffer();

            System.out.printf("Grid Partitioner tuple 0 (%d): ", fta.getFieldCount());
            for (int i = 0; i < fta.getFieldCount(); i++) {
                System.out.printf("[%d],", fta.getFieldLength(tupId, i));
            }
            for (int i = 0; i < fta.getFieldCount(); i++) {
                System.out.printf("%d,", buf.getInt(fta.getAbsoluteFieldStartOffset(tupId, i)));
            }
            System.out.println();

            int x1 = buf.getInt(fta.getAbsoluteFieldStartOffset(tupId, 1));
            int y1 = buf.getInt(fta.getAbsoluteFieldStartOffset(tupId, 2));
            int x2 = buf.getInt(fta.getAbsoluteFieldStartOffset(tupId, 3));
            int y2 = buf.getInt(fta.getAbsoluteFieldStartOffset(tupId, 4));

            int col1 = (x1 - xmin) * cols / (xmax - xmin);
            int col2 = ((x2 - xmin) * cols - 1) / (xmax - xmin) + 1;
            int row1 = (y1 - ymin) * rows / (ymax - ymin);
            int row2 = ((y2 - ymin) * rows - 1) / (ymax - ymin) + 1;

            int numMatches = (row2 - row1) * (col2 - col1);
            int iCell = 0;

            if (numMatches < matchingCells.length) {
                for (int col = col1; col < col2; col++) {
                    for (int row = row1; row < row2; row++) {
                        matchingCells[iCell++] = row * cols + col;
                    }
                }
            }
            return numMatches;
        }

        @Override
        public int getMatchingCell(int x, int y) {
            int col = (x - xmin) * cols / (xmax - xmin);
            int row = (y - ymin) * rows / (ymax - ymin);
            return row * cols + col;
        }

        @Override
        public int getMatchingCell(double x, double y) {
            int col = (int) Math.floor((x - xmin) * cols / (xmax - xmin));
            int row = (int) Math.floor((y - ymin) * rows / (ymax - ymin));
            return row * cols + col;
        }

    }

    public static class CellIDX1X1ComparatorI implements ITuplePairComparator, Serializable {

        private static final long serialVersionUID = -5880035679836791236L;

        @Override
        public int compare(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1, int tupId1)
                throws HyracksDataException {
            // Compare the cell IDs
            ByteBuffer buf0 = fta0.getBuffer();
            ByteBuffer buf1 = fta1.getBuffer();

            System.out.print("Comparing X1 X1: (");
            for (int i = 0; i < fta0.getFieldCount(); i++) {
                System.out.printf("%d,", buf0.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, i)));
            }
            System.out.print(") to (");
            for (int i = 0; i < fta1.getFieldCount(); i++) {
                System.out.printf("%d,", buf1.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, i)));
            }
            System.out.println(")");

            int r0_cellId = buf0.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 0));
            int r1_cellId = buf1.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 0));
            if (r0_cellId != r1_cellId)
                return r0_cellId - r1_cellId;

            // Compare the x1 coordinates
            int r0_x1 = buf0.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 1));
            int r1_x1 = buf1.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 1));
            return r0_x1 - r1_x1;
        }
    }

    public static class CellIDX1X2ComparatorI implements ITuplePairComparator, Serializable {
        private static final long serialVersionUID = -5880035679836791236L;

        @Override
        public int compare(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1, int tupId1)
                throws HyracksDataException {
            // Compare the cell IDs
            ByteBuffer buf0 = fta0.getBuffer();
            ByteBuffer buf1 = fta1.getBuffer();
            int r0_cellId = buf0.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 0));
            int r1_cellId = buf1.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 0));
            if (r0_cellId != r1_cellId)
                return r0_cellId - r1_cellId;

            // Compare the x1 coordinates
            int r0_x1 = buf0.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 2));
            int r1_x2 = buf1.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 4));
            return r0_x1 - r1_x2;
        }
    }

    public static class SpatialOverlapCellPredicateI implements IPredicateEvaluator, Serializable {

        private static final long serialVersionUID = 5418297063092065477L;

        @Override
        public boolean evaluate(IFrameTupleAccessor fta0, int tupId0, IFrameTupleAccessor fta1, int tupId1) {
            // Compare cell IDs
            ByteBuffer buf0 = fta0.getBuffer();
            ByteBuffer buf1 = fta1.getBuffer();

            if (buf0.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 0)) != buf1
                    .getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 0)))
                return false;

            // Read the coordinates of the first rectangle
            int r0_x1 = buf0.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 2));
            int r0_y1 = buf0.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 3));
            int r0_x2 = buf0.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 4));
            int r0_y2 = buf0.getInt(fta0.getAbsoluteFieldStartOffset(tupId0, 5));
            // Read the coordinates of the second rectangle
            int r1_x1 = buf1.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 2));
            int r1_y1 = buf1.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 3));
            int r1_x2 = buf1.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 4));
            int r1_y2 = buf1.getInt(fta1.getAbsoluteFieldStartOffset(tupId1, 5));

            // Evaluate the overlap of the two rectangles
            return r0_x2 > r1_x1 && r1_x2 > r0_x1 && r0_y2 > r1_y1 && r1_y2 > r0_y1;
        }
    }

    /**
     * Reference point duplicate avoidance techniques applied to rectangles
     * with integer coordinates.
     * Format of input tuples: cID, rID, rx1, ry1, rx2, ry2, cID, sID, sx1, sy1, sx2, sy2
     * Notice that cID is repeated twice as a result of co-partitioning the two input
     * datasets. The two cIDs should have equal values as the PBSM applies the join
     * function for each cell separately.
     * 
     * @author Ahmed Eldawy
     */
    public static class ReferencePointI implements IFilter, Serializable {

        private static final long serialVersionUID = -5545594451729241617L;

        /** The underlying spatial partitioner */
        private ISpatialPartitioner partitioner;

        public ReferencePointI(ISpatialPartitioner partitioner) {
            this.partitioner = partitioner;
        }

        @Override
        public boolean evaluate(IFrameTupleAccessor fta, int tupId) {
            ByteBuffer buf = fta.getBuffer();
            int cellID = buf.getInt(fta.getAbsoluteFieldStartOffset(tupId, 0));
            int rx1 = buf.getInt(fta.getAbsoluteFieldStartOffset(tupId, 2));
            int ry1 = buf.getInt(fta.getAbsoluteFieldStartOffset(tupId, 3));
            int sx1 = buf.getInt(fta.getAbsoluteFieldStartOffset(tupId, 8));
            int sy1 = buf.getInt(fta.getAbsoluteFieldStartOffset(tupId, 9));
            int refX = Math.max(rx1, sx1);
            int refY = Math.max(ry1, sy1);
            int refCellID = partitioner.getMatchingCell(refX, refY);
            return refCellID == cellID;
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
}