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

import java.io.File;

import org.junit.Test;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.join.NestedLoopJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.tests.util.NoopNullWriterFactory;
import org.apache.hyracks.tests.util.ResultSerializerFactoryProvider;

public class SpatialJoinTest extends AbstractIntegrationTest {
    private static class JoinComparatorFactory implements ITuplePairComparatorFactory {
        private static final long serialVersionUID = 1L;

        private final IBinaryComparatorFactory bFactory;
        private final int pos0;
        private final int pos1;

        public JoinComparatorFactory(IBinaryComparatorFactory bFactory, int pos0, int pos1) {
            this.bFactory = bFactory;
            this.pos0 = pos0;
            this.pos1 = pos1;
        }

        @Override
        public ITuplePairComparator createTuplePairComparator(IHyracksTaskContext ctx) {
            return new JoinComparator(bFactory.createBinaryComparator(), pos0, pos1);
        }
    }

    private static class JoinComparator implements ITuplePairComparator {

        private final IBinaryComparator bComparator;
        private final int field0;
        private final int field1;

        public JoinComparator(IBinaryComparator bComparator, int field0, int field1) {
            this.bComparator = bComparator;
            this.field0 = field0;
            this.field1 = field1;
        }

        @Override
        public int compare(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
                throws HyracksDataException {
            int tStart0 = accessor0.getTupleStartOffset(tIndex0);
            int fStartOffset0 = accessor0.getFieldSlotsLength() + tStart0;

            int tStart1 = accessor1.getTupleStartOffset(tIndex1);
            int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

            int fStart0 = accessor0.getFieldStartOffset(tIndex0, field0);
            int fEnd0 = accessor0.getFieldEndOffset(tIndex0, field0);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = accessor1.getFieldStartOffset(tIndex1, field1);
            int fEnd1 = accessor1.getFieldEndOffset(tIndex1, field1);
            int fLen1 = fEnd1 - fStart1;

            int c = bComparator.compare(accessor0.getBuffer().array(), fStart0 + fStartOffset0, fLen0, accessor1
                    .getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
            return 0;
        }
    }

    /*
     * TPCH Customer table: CREATE TABLE CUSTOMER ( C_CUSTKEY INTEGER NOT NULL,
     * C_NAME VARCHAR(25) NOT NULL, C_ADDRESS VARCHAR(40) NOT NULL, C_NATIONKEY
     * INTEGER NOT NULL, C_PHONE CHAR(15) NOT NULL, C_ACCTBAL DECIMAL(15,2) NOT
     * NULL, C_MKTSEGMENT CHAR(10) NOT NULL, C_COMMENT VARCHAR(117) NOT NULL );
     * TPCH Orders table: CREATE TABLE ORDERS ( O_ORDERKEY INTEGER NOT NULL,
     * O_CUSTKEY INTEGER NOT NULL, O_ORDERSTATUS CHAR(1) NOT NULL, O_TOTALPRICE
     * DECIMAL(15,2) NOT NULL, O_ORDERDATE DATE NOT NULL, O_ORDERPRIORITY
     * CHAR(15) NOT NULL, O_CLERK CHAR(15) NOT NULL, O_SHIPPRIORITY INTEGER NOT
     * NULL, O_COMMENT VARCHAR(79) NOT NULL );
     */
    @Test
    public void customerOrderCIDJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/spatial/rects1.csv"))) };
        IFileSplitProvider rect1SplitsProvider = new ConstantFileSplitProvider(rect1Splits);
        RecordDescriptor rect1Desc = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer()});

        // Define second input file
        FileSplit[] rect2Splits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/spatial/rects2.csv"))) };
        IFileSplitProvider rect2SplitsProvider = new ConstantFileSplitProvider(rect2Splits);
        RecordDescriptor rect2Desc = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer() });

        RecordDescriptor sJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, rect2SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE }, ','), rect2Desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor rect2Scanner = new FileScanOperatorDescriptor(spec, rect1SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                		DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE,
                		DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE }, ','), rect1Desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect2Scanner, NC1_ID);

        NestedLoopJoinOperatorDescriptor join = new NestedLoopJoinOperatorDescriptor(spec, new JoinComparatorFactory(
                PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY), 0, 0), sJoinDesc, 4, false,
                null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        // TODO consider using for debugging PlainFileWriterOperatorDescriptor
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        // Connect the two inputs
        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(custJoinConn, rect2Scanner, 0, join, 1);

        // Connect the output
        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTestAndStoreResult(spec, new File("sj_test_output"));
    }

}