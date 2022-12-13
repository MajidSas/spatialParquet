package edu.ucr.cs.bdlab.spatialParquet;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class GeometryMaterializer extends RecordMaterializer<Geometry> {

    GeometryUpdater geometryUpdater = new GeometryUpdater();

    GroupConverter geometryConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i == 0) {
                return geometryTypeConverter;
            } else if(i == 1) {
                return geometryPartConverter;
            } else {
                return this;
            }
        }

        @Override
        public void start() {
            geometryUpdater.start();
        }

        @Override
        public void end() {
            geometryUpdater.end();
        }
    };
    PrimitiveConverter geometryTypeConverter = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return true;
        }
        @Override
        public void addInt(int value) {
            geometryUpdater.setType(value);
        }
    };
    PrimitiveConverter xConverter = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return true;
        }
        @Override
        public void addDouble(double value) {
            geometryUpdater.setX(value);
        }
    };
    PrimitiveConverter yConverter = new PrimitiveConverter() {
        @Override
        public boolean isPrimitive() {
            return true;
        }
        @Override
        public void addDouble(double value) {
            geometryUpdater.setY(value);
        }
    };
    GroupConverter pointConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            if(i == 0) {
                return xConverter;
            }
            return yConverter;
        }

        @Override
        public void start() {
            geometryUpdater.resetCoordinate();
        }

        @Override
        public void end() {
            geometryUpdater.appendCoordinate();
        }
    };
    GroupConverter geometryPartConverter = new GroupConverter() {
        @Override
        public Converter getConverter(int i) {
            return pointConverter;
        }

        @Override
        public void start() {
            geometryUpdater.resetPart();
        }

        @Override
        public void end() {
            geometryUpdater.appendPart();
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
