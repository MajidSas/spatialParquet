package edu.ucr.cs.bdlab.io;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureReader;
import edu.ucr.cs.bdlab.beast.io.GeoJSONFeatureWriter;
import edu.ucr.cs.bdlab.beast.io.shapefile.ShapefileFeatureReader;
import edu.ucr.cs.bdlab.beast.io.shapefile.ShapefileFeatureWriter;
import edu.ucr.cs.bdlab.spatialParquet.GeometryParquetWriter;
import edu.ucr.cs.bdlab.spatialParquet.GeometryReadSupport;
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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;
import org.locationtech.jts.geom.*;
import org.apache.parquet.filter2.predicate.FilterApi;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;
import org.apache.spark.beast.sql.GeometryDataType;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.locationtech.jts.geom.Point;
import org.apache.hadoop.fs.FileSystem;

import javax.xml.crypto.Data;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.addResource(new Path("/local_data/dblab/pkgs/spark-3.1.2-bin-without-hadoop/conf/core-site.xml"));
//        conf.addResource(new Path("/local_data/dblab/pkgs/spark-3.1.2-bin-without-hadoop/conf/hdfs-site.xml"));
//        conf.set("fs.hdfs.impl",
//                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
//        );
//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        conf.set("fs.defaultFS", "hdfs://ec-hn.cs.ucr.edu:8040/");
//        FileSystem fs = FileSystem.get(URI.create("hdfs://ec-hn.cs.ucr.edu:8040/"), conf);
        FileSystem fs = FileSystem.get(conf);
        // START EXPERIMENT OPTIONS
        String sortMethod = args[3];
        boolean hasSort = false;
        String geometryType = args[4];
        boolean allTypes = false;
        if(geometryType.equals("Any")) {
            allTypes = true;
        }
        String datasetName;
        if(args[0].endsWith(".geojson")) {
            datasetName = args[0].substring(args[0].lastIndexOf("/")+1, args[0].lastIndexOf("."));
        } else {
            datasetName = args[0].substring(args[0].lastIndexOf("/")+1);
        }
        String inputPath = "./parquet_output/" + datasetName + "_delta_" + args[2] + "_" + sortMethod + ".parquet";
        String operation = args[1];
        String outputFile = "";
        if(operation.equals("geojson")) {
            outputFile =  inputPath.substring(0,inputPath.lastIndexOf(".")) + ".geojson";
        } else if(operation.equals("shapefile")) {
            outputFile = inputPath.substring(0,inputPath.lastIndexOf(".")) + "_shapefile/";
        }
        int bufferSize = Integer.parseInt(args[5]);

        // END EXPERIMENT OPTIONS
        MessageType schema = MessageTypeParser.parseMessageType(
                "message Geometry {\n" +
                        "required INT32 geometryType;\n" +
                        "repeated group part {\n" +
                        "repeated group coordinate {\n" +
                        "required DOUBLE x;\n" +
                        "required DOUBLE y;\n" +
                        "}\n" +
                        "}\n" +
                        "}");




        long totalInput = 0;
        long datasetTotalCoordinates = 0L;
        long readingTime = 0L;
        long writingTime = 0L;
        long outputSize = 0L;
        int counter = 0;
        long start = 0L;


        ParquetReader<Geometry> reader = getParquetReader(inputPath, false, 0,0,0,0);
        ArrayList<Geometry> geometryBuffer = new ArrayList<>(bufferSize);

        if(operation.equals("geojson")) {
            GeoJSONFeatureWriter geojsonWriter = new GeoJSONFeatureWriter();
            start = System.nanoTime();
            geojsonWriter.initialize(new BufferedOutputStream(new FileOutputStream(outputFile)), conf);
            writingTime += System.nanoTime()-start;

            Geometry geo = reader.read();
            while(geo != null) {
                totalInput++;
                datasetTotalCoordinates += geo.getNumPoints();
                geometryBuffer.add(geo);
                if(geometryBuffer.size() == bufferSize) {
                    start = System.nanoTime();
                    for(Geometry g : geometryBuffer) {
                        geojsonWriter.write(Feature.create(new Feature(), g));
                    }
                    writingTime += System.nanoTime()-start;
                    geometryBuffer.clear();
                }
                geo = reader.read();
            }
            if(geometryBuffer.size() > 0) {
                start = System.nanoTime();
                for(Geometry g : geometryBuffer) {
                    geojsonWriter.write(Feature.create(new Feature(), g));
                }
                writingTime += System.nanoTime()-start;
                geometryBuffer.clear();
            }
            start = System.nanoTime();
            geojsonWriter.close();
            writingTime += System.nanoTime()-start;

        } else if(operation.equals("shapefile")) {
            Geometry geo = reader.read();
            while(geo != null) {
                totalInput++;
                datasetTotalCoordinates += geo.getNumPoints();
                geometryBuffer.add(geo);
                if(geometryBuffer.size() == bufferSize) {
                    ShapefileFeatureWriter shapefileWriter = new ShapefileFeatureWriter();
                    start = System.nanoTime();
                    shapefileWriter.initialize(new Path(outputFile + counter + ".shp"), conf);
                    for(Geometry g : geometryBuffer) {
                        shapefileWriter.write(Feature.create(new Feature(), g));
                    }
                    shapefileWriter.close();
                    writingTime += System.nanoTime()-start;
                    counter++;
                    geometryBuffer.clear();
                }
                geo = reader.read();
            }
            if(geometryBuffer.size() > 0) {
                ShapefileFeatureWriter shapefileWriter = new ShapefileFeatureWriter();
                start = System.nanoTime();
                shapefileWriter.initialize(new Path(outputFile + counter + ".shp"), conf);
                for(Geometry g : geometryBuffer) {
                    shapefileWriter.write(Feature.create(new Feature(), g));
                }
                shapefileWriter.close();
                writingTime += System.nanoTime()-start;
                counter++;
                geometryBuffer.clear();
            }
        }



        if(operation.equals("geojson")) {
            GeoJSONFeatureReader geojsonReader = new GeoJSONFeatureReader();
            geojsonReader.initialize(new Path(outputFile), new BeastOptions());
            start = System.nanoTime();
            while(geojsonReader.nextKeyValue()) {
                Geometry g = geojsonReader.getCurrentValue().getGeometry();
            }
            geojsonReader.close();
            readingTime += System.nanoTime()-start;
        } else if(operation.equals("shapefile")) {
            for(int i = 0; i < counter; i++) {
                ShapefileFeatureReader shapeReader = new ShapefileFeatureReader();
                shapeReader.initialize(new Path(outputFile + i + ".shp"), new BeastOptions());
                start = System.nanoTime();
                while(shapeReader.nextKeyValue()) {
                    Geometry g = shapeReader.getCurrentValue().getGeometry();
                }
                shapeReader.close();
                readingTime += System.nanoTime()-start;
            }

        }


        System.out.println(datasetName);
        System.out.println("Dataset total geometries: " + totalInput);
        System.out.println("Dataset total points: " + datasetTotalCoordinates);
        System.out.println(operation);
        System.out.println("Writing time: " + writingTime);
        System.out.println("Reading time: " + readingTime);
        System.out.println("Output size: " + outputSize);
    }




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
        Operators.DoubleColumn xColumn = FilterApi.doubleColumn("parts.coordinates.x");
        Operators.DoubleColumn yColumn = FilterApi.doubleColumn("parts.coordinates.y");
        return ParquetReader.builder(new GeometryReadSupport(), path)
                .withFilter(FilterCompat.get(FilterApi.and(
                        FilterApi.and(
                           FilterApi.gtEq(xColumn, xMin),
                           FilterApi.ltEq(xColumn, xMax)
                        ),
                        FilterApi.and(
                            FilterApi.gtEq(yColumn, yMin),
                            FilterApi.ltEq(yColumn, yMax)
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
