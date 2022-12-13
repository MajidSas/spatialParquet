package edu.ucr.cs.bdlab.geoParquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBWriter;

import java.util.HashMap;

public class GeometryWriteSupport extends WriteSupport<Geometry> {
    MessageType schema;
    RecordConsumer recordConsumer;
    WKBWriter wkbWriter;
    GeometryWriteSupport(MessageType schema) {
        this.schema = schema;
        wkbWriter = new WKBWriter();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

//    @Override
//    public WriteSupport.FinalizedWriteContext finalizeWrite() {
//        Map<String, String> metadata  = new HashMap<String, String>() {{
//            put("first", "0");
//            put("minX", "5");
//            put("minY", "7");
//        }};
//        return new WriteSupport.FinalizedWriteContext(metadata);
//    }

    private void writePart(Coordinate[] coordinates) {
        recordConsumer.startField("coordinate", 0);
        for (Coordinate coordinate : coordinates) {
            recordConsumer.startGroup();
            recordConsumer.startField("x", 0);
            recordConsumer.addDouble(coordinate.getX());
            recordConsumer.endField("x", 0);
            recordConsumer.startField("y", 1);
            recordConsumer.addDouble(coordinate.getY());
            recordConsumer.endField("y", 1);
            recordConsumer.endGroup();
        }
        recordConsumer.endField("coordinate", 0);
    }

    @Override
    public void write(Geometry geometry) {
        recordConsumer.startMessage();

        // Geometry Type
        recordConsumer.startField("geometry", 0);
        byte[] wkb = wkbWriter.write(geometry);
        recordConsumer.addBinary(Binary.fromConstantByteArray(wkb));
        recordConsumer.endField("geometry", 0);

        // Boundary
        Envelope mbr = geometry.getEnvelopeInternal();
        recordConsumer.startField("minX", 1);
        recordConsumer.addDouble(mbr.getMinX());
        recordConsumer.endField("minX", 1);

        recordConsumer.startField("maxX", 2);
        recordConsumer.addDouble(mbr.getMinX());
        recordConsumer.endField("maxX", 2);

        recordConsumer.startField("minY", 3);
        recordConsumer.addDouble(mbr.getMinY());
        recordConsumer.endField("minY", 3);

        recordConsumer.startField("maxY", 4);
        recordConsumer.addDouble(mbr.getMinY());
        recordConsumer.endField("maxY", 4);

        // end geometry object
        recordConsumer.endMessage();
    }
}
