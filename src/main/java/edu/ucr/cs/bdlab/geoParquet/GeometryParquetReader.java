package edu.ucr.cs.bdlab.geoParquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.locationtech.jts.geom.Geometry;
import org.apache.parquet.filter2.compat.FilterCompat;

import java.io.IOException;

public class GeometryParquetReader extends ParquetReader<Geometry> {

    public GeometryParquetReader(Path file, ReadSupport<Geometry> readSupport) throws IOException {
        super(file, readSupport, (UnboundRecordFilter) FilterCompat.NOOP);

    }
}
