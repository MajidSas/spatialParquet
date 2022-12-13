package edu.ucr.cs.bdlab.geoParquet;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.util.ArrayList;

public class GeometryUpdater {
    Geometry g;
    WKBReader wkbReader = new WKBReader();
    public void setWKB(byte[] wkb) throws ParseException {
        g = wkbReader.read(wkb);
    }

    public void start() {
    }

    public void end() {
    }
    public Geometry getObject() {
        return g;
    }
}
