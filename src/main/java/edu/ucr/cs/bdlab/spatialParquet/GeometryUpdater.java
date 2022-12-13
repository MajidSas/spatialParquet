package edu.ucr.cs.bdlab.spatialParquet;

import org.locationtech.jts.geom.*;
import java.util.ArrayList;
import java.util.Arrays;

public class GeometryUpdater {
    int type = -1;
    ArrayList<CoordinateSequence> parts = new ArrayList<CoordinateSequence>();
    double[] xs = new double[16];
    double[] ys = new double[16];
    int numCoords = 0;
    GeometryFactory gf = new GeometryFactory();
    Geometry g;

    public void ensureCapacity(int newCapacity) {
        if (xs.length < newCapacity) {
            newCapacity = Integer.highestOneBit(newCapacity) * 2;
            xs = Arrays.copyOf(xs, newCapacity);
            ys = Arrays.copyOf(ys, newCapacity);
        }
    }

    public void setType(int value) {
        type = value;
    }
    public void setX(double value) { xs[numCoords] = value; }
    public void setY(double value) {
        ys[numCoords] = value;
    }
    public void resetCoordinate() {
        ensureCapacity(numCoords+1);
    }
    public void appendCoordinate() {
        numCoords++;
    }
    public void resetPart() {
        parts.clear();
    }
    public void appendPart() {
        // Move all current coordinates to the list of parts
        CoordinateSequence cs = gf.getCoordinateSequenceFactory().create(numCoords, 2);
        for (int i = 0; i < numCoords; i++) {
            cs.setOrdinate(i, 0, xs[i]);
            cs.setOrdinate(i, 1, ys[i]);
        }
        parts.add(cs);
        numCoords = 0;
    }
//    private Coordinate[] partToArray(ArrayList<Coordinate> part) {
//        return part.toArray(new Coordinate[currentPart.size()]);
//    }

    public void start() {
        type = -1;
        //parts = new ArrayList<Coordinate[]>();
        //currentPart = new ArrayList<Coordinate>();
        //resetCoordinate();
    }

    public void end() {
        switch(this.type) {
            case 1:  g = createPoint(0); break;
            case 2:  g = createLinearRing(0); break;
            case 3:  g = createLineString(0); break;
            case 4:  g = createMultiPoint(); break;
            case 5:  g = createPolygon(); break;
            case 6:  g = createMultiLineString(); break;
            case 7:  g = createMultiPolygon(); break;
            default: g = gf.createEmpty(2);
        }
    }
    public Geometry getObject() {
        return g;
    }

    private Geometry createPoint(int partIndex) {
        return gf.createPoint(parts.get(partIndex));
    }

    private LinearRing createLinearRing(int partIndex) {
        return gf.createLinearRing(parts.get(partIndex));
    }

    private Geometry createLineString(int partIndex) {
        return gf.createLineString(parts.get(partIndex));
    }

    private Geometry createMultiPoint() {
        return gf.createMultiPoint(parts.get(0));
    }

    private Geometry createMultiLineString() {
        int size = parts.size();
        LineString[] lineStrings = new LineString[size];
        for(int i = 0; i < size; i++) {
            lineStrings[i] = (LineString) createLineString(i);
        }
        return gf.createMultiLineString(lineStrings);
    }

    private Geometry createPolygon() {
        int size = parts.size();
        LinearRing shell = gf.createLinearRing(parts.get(0));
        LinearRing[] holes = new LinearRing[size-1];
        for(int i = 1; i < size; i++) {
            holes[i-1] = gf.createLinearRing(parts.get(i));
        }
        return gf.createPolygon(shell, holes);
    }

    private Geometry createMultiPolygon() {
        LinearRing outerShell = null;
        ArrayList<LinearRing> holes = new ArrayList<LinearRing>();
        ArrayList<Polygon> polygons = new ArrayList<>();
        for (int i = 0; i < parts.size(); i++) {
            if (isClockWiseOrder(parts.get(i))) {
                if (outerShell != null) {
                    // Create a polygon out of everything we have
                    Polygon polygon = gf.createPolygon(outerShell, holes.toArray(new LinearRing[0]));
                    polygons.add(polygon);
                    holes.clear();
                }
                outerShell = createLinearRing(i);
            } else {
                holes.add(createLinearRing(i));
            }
        }
        if (outerShell != null) {
            // Create the last polygon
            polygons.add(gf.createPolygon(outerShell, holes.toArray(new LinearRing[0])));
        }
        return gf.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    private static boolean isClockWiseOrder(CoordinateSequence cs) {
        // See https://stackoverflow.com/questions/1165647/how-to-determine-if-a-list-of-polygon-points-are-in-clockwise-order
        double sum = 0.0;
        int $i = 0;
        double x1 = cs.getX($i);
        double y1 = cs.getY($i);
        while (++$i < cs.size()) {
            double x2 = cs.getX($i);
            double y2 = cs.getY($i);
            sum += (x2 - x1) * (y2 + y1);
            x1 = x2;
            y1 = y2;
        }
        boolean cwOrder = sum > 0;
        return cwOrder;
    }

}
