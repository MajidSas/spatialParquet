package edu.ucr.cs.bdlab.geoParquet;

import org.apache.parquet.io.api.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

public class GeometryMaterializer extends RecordMaterializer<Geometry> {

    GeometryUpdater geometryUpdater = new GeometryUpdater();

    GroupConverter geometryConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i == 0) {
                return wkbConverter;
            } else {
                return otherValues;
            }
        }

        @Override
        public void start() {

        }

        @Override
        public void end() {

        }
    };
    PrimitiveConverter wkbConverter = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return true;
        }
        @Override
        public void addBinary(Binary value) {
            try {
                geometryUpdater.setWKB(value.getBytes());
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    };
    PrimitiveConverter otherValues = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return super.isPrimitive();
        }
        @Override
        public void addDouble(double value) {
           // do nothing
        }
    };

    @Override
    public GroupConverter getRootConverter() {
        return geometryConverter;
    }

    @Override
    public Geometry getCurrentRecord() {
        return geometryUpdater.getObject();
    }
}
