package org.apache.hyracks.tests.integration;

import java.io.DataInputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.EnumSet;

import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IResultSerializer;
import org.apache.hyracks.api.dataflow.value.IResultSerializerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sjoin.FilterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sjoin.PlaneSweepJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sjoin.ProjectionOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sjoin.SpatialPartitionOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.tests.integration.SpatialJoinTest.CellIDX1X1ComparatorD;
import org.apache.hyracks.tests.integration.SpatialJoinTest.CellIDX1X2ComparatorD;
import org.apache.hyracks.tests.integration.SpatialJoinTest.ReferencePointD;
import org.apache.hyracks.tests.integration.SpatialJoinTest.SpatialOverlapCellPredicateD;
import org.apache.hyracks.tests.integration.SpatialJoinTest.UniformGridPartitionerD;

public class SpatialJoinRun {

    public static final String NC1_ID = "nc1";
    public static final String NC2_ID = "nc2";

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static NodeControllerService nc2;
    private static IHyracksClientConnection hcc;

    static {
        try {
            init();
        } catch (Exception e) {
            throw new RuntimeException("Error initializing class", e);
        }
    }

    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 39001;
        ccConfig.profileDumpPeriod = 10000;
        File outDir = new File("target" + File.separator + "ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(AbstractIntegrationTest.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.ccRoot = ccRoot.getAbsolutePath();
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = "localhost";
        ncConfig1.ccPort = 39001;
        ncConfig1.clusterNetIPAddress = "127.0.0.1";
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.resultIPAddress = "127.0.0.1";
        ncConfig1.nodeId = NC1_ID;
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig();
        ncConfig2.ccHost = "localhost";
        ncConfig2.ccPort = 39001;
        ncConfig2.clusterNetIPAddress = "127.0.0.1";
        ncConfig2.dataIPAddress = "127.0.0.1";
        ncConfig2.resultIPAddress = "127.0.0.1";
        ncConfig2.nodeId = NC2_ID;
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2)
            throw new RuntimeException("Require two input files");
        String inFile1 = args[0];
        String inFile2 = args[1];

        JobSpecification spec = new JobSpecification();
        // Define first input file
        FileSplit[] rect1Splits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(inFile1))) };
        IFileSplitProvider rect1SplitsProvider = new ConstantFileSplitProvider(rect1Splits);
        RecordDescriptor inDesc = new RecordDescriptor(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                        DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });
        FileScanOperatorDescriptor rect1Scanner = new FileScanOperatorDescriptor(spec, rect1SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE }, ','),
                inDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect1Scanner, NC1_ID);

        // Define second input file
        FileSplit[] rect2Splits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(inFile2))) };
        IFileSplitProvider rect2SplitsProvider = new ConstantFileSplitProvider(rect2Splits);
        FileScanOperatorDescriptor rect2Scanner = new FileScanOperatorDescriptor(spec, rect2SplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE }, ','),
                inDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rect2Scanner, NC1_ID);

        // Define the format of partitioned data: cellID, recordID, x1, y1, x2, y2
        RecordDescriptor partitionedDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

        // Define the output file: rCID, rID, rx1, ry1, rx2, ry2, sCID, sID, sx1, sy1, sx2, sy2
        RecordDescriptor joinedDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });
        // Final output (remove cell IDs)t: rID, rx1, ry1, rx2, ry2, sID, sx1, sy1, sx2, sy2
        RecordDescriptor outputDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

        UniformGridPartitionerD gridPartitioner = new UniformGridPartitionerD(0, 0, 10000, 10000, 100, 100);

        // Project a cell ID column to each record
        SpatialPartitionOperatorDescriptor partitionOp1 = new SpatialPartitionOperatorDescriptor(spec, partitionedDesc,
                gridPartitioner);
        SpatialPartitionOperatorDescriptor partitionOp2 = new SpatialPartitionOperatorDescriptor(spec, partitionedDesc,
                gridPartitioner);

        // Sort the two inputs based on (cellID, X1)
        ExternalSortOperatorDescriptor sorter1 = new ExternalSortOperatorDescriptor(spec, 10, new int[] { 0, 2 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(DoublePointable.FACTORY) },
                partitionedDesc);
        ExternalSortOperatorDescriptor sorter2 = new ExternalSortOperatorDescriptor(spec, 10, new int[] { 0, 2 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(DoublePointable.FACTORY) },
                partitionedDesc);

        // Plane-sweep join operator
        PlaneSweepJoinOperatorDescriptor join = new PlaneSweepJoinOperatorDescriptor(spec, new CellIDX1X1ComparatorD(),
                new CellIDX1X2ComparatorD(), new CellIDX1X2ComparatorD(), joinedDesc, 10,
                new SpatialOverlapCellPredicateD());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        FilterOperatorDescriptor dupAvoidanceOp = new FilterOperatorDescriptor(spec, joinedDesc,
                new ReferencePointD(gridPartitioner));

        ProjectionOperatorDescriptor projectOp = new ProjectionOperatorDescriptor(spec, outputDesc,
                new int[] { 1, 2, 3, 4, 5, 7, 8, 9, 10, 11 });

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IResultSerializerFactory resultSerializerFactory = new IResultSerializerFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IResultSerializer createResultSerializer(final RecordDescriptor recordDesc,
                    final PrintStream printStream) {
                return new IResultSerializer() {
                    private static final long serialVersionUID = 1L;

                    ByteBufferInputStream bbis = new ByteBufferInputStream();
                    DataInputStream di = new DataInputStream(bbis);

                    @Override
                    public void init() throws HyracksDataException {

                    }

                    @Override
                    public boolean appendTuple(IFrameTupleAccessor tAccess, int tIdx) throws HyracksDataException {
                        int start = tAccess.getTupleStartOffset(tIdx) + tAccess.getFieldSlotsLength();

                        bbis.setByteBuffer(tAccess.getBuffer(), start);

                        Object[] record = new Object[recordDesc.getFieldCount()];
                        for (int i = 0; i < record.length; ++i) {
                            Object instance = recordDesc.getFields()[i].deserialize(di);
                            if (i == 0) {
                                printStream.print(String.valueOf(instance));
                            } else {
                                printStream.print(", " + String.valueOf(instance));
                            }
                        }
                        printStream.println();
                        return true;
                    }
                };
            }
        };

        // Create the sink (output) operator
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                resultSerializerFactory);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        // Connect input and output
        spec.connect(new OneToOneConnectorDescriptor(spec), rect1Scanner, 0, partitionOp1, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), rect2Scanner, 0, partitionOp2, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), partitionOp1, 0, sorter1, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), partitionOp2, 0, sorter2, 0);

        ////////////////////////////
        IConnectorDescriptor repartitioner1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) }));
        IConnectorDescriptor repartitioner2 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) }));
        ////////////////////////////////

        spec.connect(repartitioner1, sorter1, 0, join, 0);
        spec.connect(repartitioner2, sorter2, 0, join, 1);

        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, dupAvoidanceOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), dupAvoidanceOp, 0, projectOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), projectOp, 0, printer, 0);

        spec.addRoot(printer);
        long t1 = System.currentTimeMillis();
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(jobId);
        long t2 = System.currentTimeMillis();
        System.out.printf("Finished spatial join in %f seconds\n", (t2 - t1) / 1000.0);

        nc2.stop();
        nc1.stop();
        cc.stop();
    }

}
