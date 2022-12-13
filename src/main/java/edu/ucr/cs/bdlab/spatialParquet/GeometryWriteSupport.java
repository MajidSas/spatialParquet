package edu.ucr.cs.bdlab.spatialParquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.locationtech.jts.geom.*;

import java.util.HashMap;

public class GeometryWriteSupport extends WriteSupport<Geometry> {
    MessageType schema;
    RecordConsumer recordConsumer;

    GeometryWriteSupport(MessageType schema) {
        this.schema = schema;
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
        recordConsumer.startField("geometryType", 0);
        switch(geometry.getGeometryType()) {
            case "Point": recordConsumer.addInteger(1); break;
            case "LinearRing": recordConsumer.addInteger(2); break;
            case "LineString": recordConsumer.addInteger(3); break;
            case "MultiPoint": recordConsumer.addInteger(4); break;
            case "Polygon": recordConsumer.addInteger(5); break;
            case "MultiLineString": recordConsumer.addInteger(6); break;
             case "MultiPolygon": recordConsumer.addInteger(7); break; // not supported geometry-collection
            default: recordConsumer.addInteger(0); // not supported geometries will be empty
        }
        recordConsumer.endField("geometryType", 0);

        // Geometry Parts
        recordConsumer.startField("part", 1);
        recordConsumer.startGroup();
        if(geometry.getGeometryType().equals("Polygon")) {
            Polygon p = (Polygon) geometry;
            writePart(p.getExteriorRing().getCoordinates());
            int nInterior = p.getNumInteriorRing();
            for(int i = 0; i < nInterior; i++) {
                writePart(p.getInteriorRingN(i).getCoordinates());
            }
        } else if(geometry.getGeometryType().equals("MultiLineString")) {
            MultiLineString ml = (MultiLineString) geometry;
            int nLines = ml.getNumGeometries();
            for(int i = 0; i < nLines; i++) {
                writePart(ml.getGeometryN(i).getCoordinates());
            }
        } else if(geometry.getGeometryType().equals("MultiPolygon")) {
            MultiPolygon mp = (MultiPolygon) geometry;
            int nPolygons = mp.getNumGeometries();
            for(int i = 0; i < nPolygons; i++) {
                writePart(mp.getGeometryN(i).getCoordinates());
                // this array must be divided when reading
            }
        } else {
            writePart(geometry.getCoordinates());
        }

        // end parts
        recordConsumer.endGroup();
        recordConsumer.endField("part", 1);

        // end geometry object
        recordConsumer.endMessage();
    }
}
