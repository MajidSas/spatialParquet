package edu.ucr.cs.bdlab.geoParquet;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureReader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.locationtech.jts.geom.*;
import org.apache.parquet.filter2.predicate.FilterApi;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.locationtech.jts.geom.Point;
import org.apache.hadoop.fs.FileSystem;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/local_data/dblab/pkgs/spark-3.1.2-bin-without-hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/local_data/dblab/pkgs/spark-3.1.2-bin-without-hadoop/conf/hdfs-site.xml"));
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        FileSystem fs = FileSystem.get(URI.create("hdfs://ec-hn.cs.ucr.edu:8040/"), conf);
//        FileSystem fs = FileSystem.get(conf);
        // START EXPERIMENT OPTIONS
        String inputPath = args[0];
        String[] inputFiles;
        String datasetName;
        String operation = args[1];

        if(inputPath.endsWith(".geojson")) {
            inputFiles = new String[1];
            inputFiles[0] = inputPath;
            datasetName = inputPath.substring(inputPath.lastIndexOf("/")+1, inputPath.lastIndexOf("."));
        } else {
            datasetName = inputPath.substring(inputPath.lastIndexOf("/")+1);
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(inputPath), true);
            ArrayList<String> tmpFiles = new ArrayList<>();
            while(files.hasNext()) {
                String filePath = files.next().getPath().toString();
                if(filePath.endsWith(".geojson")) {
                    tmpFiles.add(filePath);
                }
            }
            inputFiles = tmpFiles.toArray(new String[0]);
        }
        String resultsPath = "./parquet_output/results_" + operation + "_" + datasetName+ ".csv";
        String sortMethod = args[3];
        boolean hasSort = false;
        String geometryType = args[4];
        boolean allTypes = false;
        if(geometryType.equals("Any")) {
            allTypes = true;
        }
        int bufferSize = Integer.parseInt(args[5]);
        CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
        switch (args[2]) {
            case "snappy":
                codec = CompressionCodecName.SNAPPY;
                break;
            case "gzip":
                codec = CompressionCodecName.GZIP;
                break;
            case "lz4":
                codec = CompressionCodecName.LZ4;
                break;
            case "lzo":
                codec = CompressionCodecName.LZO;
                break;
            case "zstd":
                codec = CompressionCodecName.ZSTD;
                break;
            case "brotli":
                codec = CompressionCodecName.BROTLI;
                break;
        }
        boolean hasFilter = false;
        double xMin = 0.0;
        double xMax = 0.0;
        double yMin = 0.0;
        double yMax = 0.0;
        if(args.length >= 10) {
            hasFilter = true;
            xMin = Double.parseDouble(args[6]);
            xMax = Double.parseDouble(args[7]);
            yMin = Double.parseDouble(args[8]);
            yMax = Double.parseDouble(args[9]);
        }

        if(sortMethod.equals("z-curve") || sortMethod.equals("hillbert-curve")) {
            hasSort = true;
        }

        String outputPath = "./parquet_output/" + datasetName + "_wkb_" + args[2] + "_" + sortMethod + ".parquet";
        if(operation.equals("write")) {
            Files.deleteIfExists(new File(outputPath).toPath());
        }

        // END EXPERIMENT OPTIONS
        MessageType schema = MessageTypeParser.parseMessageType(
                "message Geometry {\n" +
                        "required BINARY geometry;\n" +
                        "required DOUBLE minX;\n" +
                        "required DOUBLE maxX;\n" +
                        "required DOUBLE minY;\n" +
                        "required DOUBLE maxY;\n" +
                        "}");

        // write data in parquet format

        long sortingTime = 0L;
        long writingTime = 0L;
        long totalInput = 0;
        long readingTime = 0L;
        long outputSize = 0L;

//        double mbrMinX = 10000000.0;
//        double mbrMaxX = -10000000.0;
//        double mbrMinY = 10000000.0;
//        double mbrMaxY = -10000000.0;
        if(operation.equals("write")) {
            GeometryParquetWriter writer = getParquetWriter(schema, outputPath, codec);
            ArrayList<Geometry> geometryBuffer = new ArrayList<>(bufferSize);
            final ArrayList<Integer>  sortValuesBuffer = new ArrayList<>(bufferSize);
            for(String inputFile : inputFiles) {
                System.out.println("Processing: " + inputFile);
                GeoJSONFeatureReader geojson = new GeoJSONFeatureReader();

                geojson.initialize(new Path(inputFile), new BeastOptions());
                while(geojson.nextKeyValue()) {
                    Geometry geo = geojson.getCurrentValue().getGeometry();
                    if (geo != null && (allTypes || geo.getGeometryType().equals(geometryType)) && !geo.getGeometryType().equals("GeometryCollection")) {
                        totalInput++;
                        geometryBuffer.add(geo);
                        if(hasSort) {
                            long startTime = System.nanoTime();
                            if(sortMethod.charAt(0) == 'h') {
                                sortValuesBuffer.add(getHilbertValue(geo));
                            } else {
                                sortValuesBuffer.add(getZValue(geo));
                            }
                            sortingTime += System.nanoTime() - startTime;
                        }
//                    if(hasSort) {
//                        Envelope geoEnv = geo.getEnvelopeInternal();
//                        mbrMinX = Math.min(geoEnv.getMinX(), mbrMinX);
//                        mbrMaxX = Math.max(geoEnv.getMaxX(), mbrMaxX);
//                        mbrMinY = Math.min(geoEnv.getMinY(), mbrMinY);
//                        mbrMaxY = Math.max(geoEnv.getMaxY(), mbrMaxY);
//                    }
                        if (geometryBuffer.size() == bufferSize) {
                            if (hasSort) {
//                            final Envelope mbr = new Envelope(mbrMinX, mbrMaxX, mbrMinY, mbrMaxY);
//                            sort(geometryBuffer, mbr, sortMethod);
                                long startTime = System.nanoTime();
                                int[] sortedIndices = IntStream.range(0, bufferSize)
                                        .boxed().sorted(Comparator.comparingInt(sortValuesBuffer::get))
                                        .mapToInt(ele -> ele).toArray();
                                sortingTime += System.nanoTime() - startTime;
                                startTime = System.nanoTime();
                                for(int i = 0; i < sortedIndices.length; i++) {
                                    writer.write(geometryBuffer.get(sortedIndices[i]));
                                }
                                writingTime += System.nanoTime() - startTime;
                            } else {
                                long startTime = System.nanoTime();
                                for (Geometry geometry : geometryBuffer) {
                                    writer.write(geometry);
                                }
                                writingTime += System.nanoTime() - startTime;
                            }

                            geometryBuffer.clear();
                            sortValuesBuffer.clear();
//                        mbrMinX = 10000000.0;
//                        mbrMaxX = -10000000.0;
//                        mbrMinY = 10000000.0;
//                        mbrMaxY = -10000000.0;
                        }
                    }
                }
                geojson.close();
            }
            if(geometryBuffer.size() > 0) {
//            if(hasSort) {
//                final Envelope mbr = new Envelope(mbrMinX, mbrMaxX, mbrMinY, mbrMaxY);
//                sort(geometryBuffer, mbr, sortMethod);
//            }
                if(hasSort) {
                    long startTime = System.nanoTime();
                    int[] sortedIndices = IntStream.range(0, geometryBuffer.size())
                            .boxed().sorted(Comparator.comparingInt(sortValuesBuffer::get))
                            .mapToInt(ele -> ele).toArray();
                    sortingTime += System.nanoTime() - startTime;
                    startTime = System.nanoTime();
                    for(int i = 0; i < sortedIndices.length; i++) {
                        writer.write(geometryBuffer.get(sortedIndices[i]));
                    }
                    writingTime += System.nanoTime() - startTime;
                } else {
                    long startTime = System.nanoTime();
                    for (Geometry geometry : geometryBuffer) {
                        writer.write(geometry);
                    }
                    writingTime += System.nanoTime() - startTime;
                }

                geometryBuffer.clear();
                sortValuesBuffer.clear();
            }
            long startTime = System.nanoTime();
            writer.close();
            writingTime += System.nanoTime() - startTime;
            System.out.println("Written " + totalInput + " geometry objects.");

            outputSize = Files.size(Paths.get(outputPath));
        }

        long totalRead = 0;
        if(operation.equals("read")) {
            // read the created parquet file
            ParquetReader<Geometry> reader = getParquetReader(outputPath, hasFilter, xMin, xMax, yMin, yMax);
            readingTime = System.nanoTime();
            Geometry geo = reader.read();
            while(geo != null) {
                totalRead++;
                geo = reader.read();
            }
            readingTime = System.nanoTime() - readingTime;
            System.out.println("Read " + totalRead + " geometry objects.");
            reader.close();
        }

        String results = "";
        if(operation.equals("write")) {
            results = String.format("%s,wkb,%s,%s,%d,%d,%d,%d\n",
                    datasetName, args[2], args[3], totalInput, sortingTime, writingTime, outputSize);
        } else if (operation.equals("read")) {
            results = String.format("%s,wkb,%s,%s,%d,%b,%d\n",
                    datasetName, args[2], args[3], totalRead , hasFilter, readingTime);
        }
        BufferedWriter output = new BufferedWriter(new FileWriter(resultsPath, true));
        output.write(results);
        output.close();


//        GeoJsonReader geojson2 = new GeoJsonReader();
        //String content = FileUtils.readFileToString(new File(inputPath), "utf-8");

//        Geometry collection = geojson2.read(content);






        //  geojson.initialize(new Path(inputPath), new BeastOptions());

//        long readingTime = System.nanoTime();
//        Geometry g1 = reader.read();
//        readingTime =  System.nanoTime()-readingTime;
//        geojson.nextKeyValue();
//        Geometry g2 = geojson.getCurrentValue().getGeometry();
//        int i = 0;
//        while(g1 != null) {
//            try {
//                if (!g1.equals(g2) && !g2.getGeometryType().equals("GeometryCollection") && !customEqual(g1, g2)) {
//                    System.out.println(i + " NOT EQUAL: ");
//                    System.out.println(g1);
//                    System.out.println(g2);
//                    break;
//                }
//            } catch (TopologyException e) {
//                // in this case the topology itself has an issue
//                // just make sure the re-constructed object has
//                // equivalent coordinate values
//                if(!customEqual(g1, g2)) {
//                    System.out.println(i + " EXCEPTION, and coordinates not equal");
//                    System.out.println(g2);
//                    System.out.println(g1);
//                    System.out.println(e);
//                    break;
//                }
//
//            }
//                long start = System.nanoTime();
//                g1 = reader.read();
//                readingTime += System.nanoTime()-start;
//                geojson.nextKeyValue();
//                g2 = geojson.getCurrentValue().getGeometry();
//                i++;
//        }
    }

//    private static void sort(ArrayList<Geometry> list, final Envelope mbr, String sortMethod) {
//        double widthFactor = Short.MAX_VALUE / mbr.getWidth();
//        double heightFactor = Short.MAX_VALUE / mbr.getHeight();
//        if(sortMethod.equals("hillbert-curve")) {
//            list.sort(Comparator.comparingInt(g -> getHilbertValue(g, mbr.getMinX(), mbr.getMinY(), widthFactor, heightFactor)));
//        } else if(sortMethod.equals("z-curve")) {
//            list.sort(Comparator.comparingInt(g -> getZValue(g, mbr.getMinX(), mbr.getMinY(), widthFactor, heightFactor)));
//        }
//    }
    private static int getZValue(Geometry g) {
        if(g.isEmpty()) { return 0; };
        Point center = g.getCentroid();
        // copied from SpatialHadoop
        int x = (int) ((center.getX() + 180) * 91);
        int y = (int) ((center.getY() + 90) * 182);

        int morton = 0;
        for (int bitPosition = 0; bitPosition < 32; bitPosition++) {
            int mask = 1 << bitPosition;
            morton |= (x & mask) << (bitPosition + 1);
            morton |= (y & mask) << bitPosition;
        }
        return morton;
    }
    private static int getHilbertValue(Geometry g) {
        if(g.isEmpty()) { return 0; };
        Point center = g.getCentroid();
        // copied from SpatialHadoop
        int x = (int) ((center.getX() + 180) * 91);
        int y = (int) ((center.getY() + 90) * 182);
        int n = Short.MAX_VALUE+1;
        int h = 0;
        for (int s = n/2; s > 0; s/=2) {
            int rx = (x & s) > 0 ? 1 : 0;
            int ry = (y & s) > 0 ? 1 : 0;
            h += s * s * ((3 * rx) ^ ry);
            // Rotate
            if (ry == 0) {
                if (rx == 1) {
                    x = n-1 - x;
                    y = n-1 - y;
                }
                //Swap x and y
                int t = x; x = y; y = t;
            }
        }
        return h;
    }

//    private static boolean customEqual(Geometry g1, Geometry g2) {
//        // the existing equal operator doesn't work well for some Polygon objects
//        // we compare based on the coordinates only
//        Coordinate[] c1 = g1.getCoordinates();
//        Coordinate[] c2 = g2.getCoordinates();
//        boolean areEquivalent = g1.getGeometryType().equals(g2.getGeometryType()) && c1.length == c2.length;
//        if(areEquivalent) {
//            for(int i = 0; i < c1.length; i++) {
//                if(c1[i].getX() != c2[i].getX() || c1[i].getY() != c2[i].getY()) {
//                    return false;
//                }
//            }
//        }
//        return true;
//    }

    private static GeometryParquetWriter getParquetWriter(MessageType schema, String outputPath, CompressionCodecName codec) throws IOException {
        File outputParquetFile = new File(outputPath);
        Path path = new Path(outputParquetFile.toURI().toString());
        return new GeometryParquetWriter(
                path, schema, false, codec
        );
    }

    private static ParquetReader<Geometry> getParquetReader(String inputPath, boolean hasFilter,
                                                            double xMin, double xMax, double yMin, double yMax) throws IOException {
        Path path = new Path(inputPath);
        Operators.DoubleColumn xMinColumn = FilterApi.doubleColumn("minX");
        Operators.DoubleColumn xMaxColumn = FilterApi.doubleColumn("maxX");
        Operators.DoubleColumn yMinColumn = FilterApi.doubleColumn("minY");
        Operators.DoubleColumn yMaxColumn = FilterApi.doubleColumn("maxY");
        return ParquetReader.builder(new GeometryReadSupport(), path)
                .withFilter(FilterCompat.get(FilterApi.and(
                        FilterApi.and(
                           FilterApi.gtEq(xMinColumn, xMin),
                           FilterApi.ltEq(xMaxColumn, xMax)
                        ),
                        FilterApi.and(
                            FilterApi.gtEq(yMinColumn, yMin),
                            FilterApi.ltEq(yMaxColumn, yMax)
                        )
                )))
                // - don't use record filters for x,y
                // - only a custom filter for an entire geometry object
                //   can be used instead.
                .useRecordFilter(false)
                .useDictionaryFilter(false)
                .useBloomFilter(false)
                .useColumnIndexFilter(false)
                .useSignedStringMinMax(false)
                // use this to filter column blocks based on range
                .useStatsFilter(hasFilter)
                .build();
    }
}
