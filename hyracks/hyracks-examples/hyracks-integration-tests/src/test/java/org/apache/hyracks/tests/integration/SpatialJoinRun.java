package org.apache.hyracks.tests.integration;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.EnumSet;

import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IResultSerializer;
import org.apache.hyracks.api.dataflow.value.IResultSerializerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.IHyracksDatasetReader;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.LongParserFactory;
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

    public static String NC_IDS[];

    private static ClusterControllerService cc;
    private static NodeControllerService[] ncs;
    private static IHyracksClientConnection hcc;

    private static IResultSerializerFactory resultSerializerFactory;

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

        int numCores = Runtime.getRuntime().availableProcessors();
        ncs = new NodeControllerService[numCores];
        NC_IDS = new String[numCores];

        for (int i = 0; i < numCores; i++) {
            NCConfig ncConfig1 = new NCConfig();
            ncConfig1.ccHost = "localhost";
            ncConfig1.ccPort = 39001;
            ncConfig1.clusterNetIPAddress = "127.0.0.1";
            ncConfig1.dataIPAddress = "127.0.0.1";
            ncConfig1.resultIPAddress = "127.0.0.1";
            ncConfig1.nodeId = NC_IDS[i] = String.format("nc%02d", i);
            ncs[i] = new NodeControllerService(ncConfig1);
            ncs[i].start();
        }

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);

        resultSerializerFactory = new IResultSerializerFactory() {
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
    }

    public static void main(String[] args) throws Exception {
        try {
            if (args.length < 2)
                throw new RuntimeException("Require two input files");
            JobSpecification spec = new JobSpecification();

            String[] inFiles = new String[] { args[0], args[1] };
            // Define the format of partitioned data: cellID, recordID, x1, y1, x2, y2
            UniformGridPartitionerD gridPartitioner = new UniformGridPartitionerD(-180, -90, 180, 90, 100, 100);
            RecordDescriptor partitionedDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                    IntegerSerializerDeserializer.INSTANCE, Integer64SerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

            int numBuffers = 250;

            IOperatorDescriptor[] rectScanners = new IOperatorDescriptor[inFiles.length];
            IOperatorDescriptor[] partitionOps = new IOperatorDescriptor[inFiles.length];
            IOperatorDescriptor[] sorters = new IOperatorDescriptor[inFiles.length];
            for (int iFile = 0; iFile < inFiles.length; iFile++) {
                // Define input file format
                FileSplit[] inputSplits;
                if (new File(inFiles[iFile]).isFile()) {
                    inputSplits = new FileSplit[] {
                            new FileSplit(NC_IDS[0], new FileReference(new File(inFiles[iFile]))) };
                } else {
                    File[] listFiles = new File(inFiles[iFile]).listFiles();
                    inputSplits = new FileSplit[listFiles.length];
                    for (int iSplit = 0; iSplit < listFiles.length; iSplit++) {
                        inputSplits[iSplit] = new FileSplit(NC_IDS[iFile % NC_IDS.length],
                                new FileReference(listFiles[iSplit]));
                    }
                }
                IFileSplitProvider rect1SplitsProvider = new ConstantFileSplitProvider(inputSplits);
                RecordDescriptor inDesc = new RecordDescriptor(
                        new ISerializerDeserializer[] { Integer64SerializerDeserializer.INSTANCE,
                                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });
                rectScanners[iFile] = new FileScanOperatorDescriptor(spec, rect1SplitsProvider,
                        new DelimitedDataTupleParserFactory(new IValueParserFactory[] { LongParserFactory.INSTANCE,
                                DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE,
                                DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE }, ','),
                        inDesc);
                String[] constraints = new String[Math.min(NC_IDS.length, inputSplits.length)];
                System.arraycopy(NC_IDS, 0, constraints, 0, constraints.length);
                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, rectScanners[iFile], constraints);

                // Partition file using grid partitioner
                partitionOps[iFile] = new SpatialPartitionOperatorDescriptor(spec, partitionedDesc, gridPartitioner);
                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, partitionOps[iFile], NC_IDS);

                // Connect input to partitioner
                spec.connect(new OneToOneConnectorDescriptor(spec), rectScanners[iFile], 0, partitionOps[iFile], 0);

                // Sort the file lexicographically by (cell ID, x1)
                sorters[iFile] = new ExternalSortOperatorDescriptor(spec, numBuffers, new int[] { 0, 2 },
                        new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY),
                                PointableBinaryComparatorFactory.of(DoublePointable.FACTORY) },
                        partitionedDesc);
                PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorters[iFile], NC_IDS);

                // Connect partitioned data to the sorter
                IConnectorDescriptor mnConnector = new MToNPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                                PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) }));

                spec.connect(mnConnector, partitionOps[iFile], 0, sorters[iFile], 0);
            }
            /*
            ///////////////////////////////////////////////////////////////////////////////
            // Write final output to disk
            ResultSetId rsId = new ResultSetId(1);
            spec.addResultSetId(rsId);
            
            // Create the sink (output) operator
            IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                    resultSerializerFactory);
            
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC_IDS);
            
            spec.connect(new OneToOneConnectorDescriptor(spec), sorters[0], 0, printer, 0);
            
            ///////////////////////////////////////////////////////////////////////////////
            */

            // Define the joined format: rCID, rID, rx1, ry1, rx2, ry2, sCID, sID, sx1, sy1, sx2, sy2
            RecordDescriptor joinedDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                    IntegerSerializerDeserializer.INSTANCE, Integer64SerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, Integer64SerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

            // Plane-sweep join operator
            PlaneSweepJoinOperatorDescriptor join = new PlaneSweepJoinOperatorDescriptor(spec,
                    new CellIDX1X1ComparatorD(), new CellIDX1X2ComparatorD(), new CellIDX1X2ComparatorD(), joinedDesc,
                    numBuffers, new SpatialOverlapCellPredicateD());
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC_IDS);

            // Connect sorted data to the plane-sweep operator
            for (int i = 0; i < inFiles.length; i++) {
                IConnectorDescriptor mnConnector = new MToNPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                                PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) }));

                spec.connect(mnConnector, sorters[i], 0, join, i);
            }

            // Duplicate avoidance
            FilterOperatorDescriptor dupAvoidanceOp = new FilterOperatorDescriptor(spec, joinedDesc,
                    new ReferencePointD(gridPartitioner));

            // Connect join output to duplicate avoidance
            spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, dupAvoidanceOp, 0);

            // Final output (remove cell IDs)t: rID, rx1, ry1, rx2, ry2, sID, sx1, sy1, sx2, sy2
            RecordDescriptor outputDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                    Integer64SerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, Integer64SerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

            ProjectionOperatorDescriptor projectOp = new ProjectionOperatorDescriptor(spec, outputDesc,
                    new int[] { 1, 2, 3, 4, 5, 7, 8, 9, 10, 11 });

            // Connect duplicate avoidance to the projection operator
            spec.connect(new OneToOneConnectorDescriptor(spec), dupAvoidanceOp, 0, projectOp, 0);

            // Write final output to disk
            ResultSetId rsId = new ResultSetId(1);
            spec.addResultSetId(rsId);

            // Create the sink (output) operator
            IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                    resultSerializerFactory);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC_IDS);

            spec.connect(new OneToOneConnectorDescriptor(spec), projectOp, 0, printer, 0);

            spec.addRoot(printer);
            long t1 = System.currentTimeMillis();
            JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
            hcc.waitForCompletion(jobId);
            long t2 = System.currentTimeMillis();
            System.out.printf("Finished spatial join in %f seconds\n", (t2 - t1) / 1000.0);

            t1 = System.currentTimeMillis();
            OutputStream output = new BufferedOutputStream(new FileOutputStream("sj_output.txt"));
            long numResults = 0;
            for (int i = 0; i < spec.getResultSetIds().size(); i++) {
                numResults += storeResults(spec, jobId, spec.getResultSetIds().get(i), output);
            }
            t2 = System.currentTimeMillis();
            System.out.printf("Stored %d results in %f seconds\n", numResults, (t2 - t1) / 1000.0);
            output.close();

        } finally {
            for (NodeControllerService nc : ncs) {
                nc.stop();
            }
            cc.stop();

        }
    }

    protected static long storeResults(JobSpecification spec, JobId jobId, ResultSetId resultSetId, OutputStream output)
            throws Exception {
        int nReaders = 1;

        IHyracksDataset hyracksDataset = new HyracksDataset(hcc, spec.getFrameSize(), nReaders);
        IHyracksDatasetReader reader = hyracksDataset.createReader(jobId, resultSetId);
        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor();

        FrameManager resultDisplayFrameMgr = new FrameManager(spec.getFrameSize());
        VSizeFrame frame = new VSizeFrame(resultDisplayFrameMgr);
        int readSize = reader.read(frame);
        long numResults = 0;

        while (readSize > 0) {
            frameTupleAccessor.reset(frame.getBuffer());
            for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                int start = frameTupleAccessor.getTupleStartOffset(tIndex);
                int length = frameTupleAccessor.getTupleEndOffset(tIndex) - start;
                output.write(frameTupleAccessor.getBuffer().array(), start, length);
                numResults++;
            }

            readSize = reader.read(frame);
        }
        return numResults;
    }

}
