package edu.ucr.cs.bdlab.spatialParquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.locationtech.jts.geom.Geometry;

import java.util.Map;

public class GeometryReadSupport extends ReadSupport<Geometry> {

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<Geometry> prepareForRead(Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext) {
        return new GeometryMaterializer();
    }
}
