
package org.xbib.elasticsearch.common.util;

import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Serializes {@link Shape} instances into GeoJSON format
 *
 * Example of the format used for points:
 *
 * { "type": "Point", "coordinates": [100.0, 0.0] }
 */
@Deprecated // no need to support geo stuff
public class GeoJSONShapeSerializer {

    private GeoJSONShapeSerializer() {
    }

    /**
     * Serializes the given {@link Shape} as GeoJSON format into the given
     * {@link XContentBuilder}
     *
     * @param shape   Shape that will be serialized
     * @param builder XContentBuilder it will be serialized to
     * @throws IOException Thrown if an error occurs while writing to the XContentBuilder
     */
    public static void serialize(Shape shape, XContentBuilder builder) throws IOException {
        if (shape instanceof JtsGeometry) {
            Geometry geometry = ((JtsGeometry) shape).getGeom();
            if (geometry instanceof Point) {
                serializePoint((Point) geometry, builder);
            } else if (geometry instanceof LineString) {
                serializeLineString((LineString) geometry, builder);
            } else if (geometry instanceof Polygon) {
                serializePolygon((Polygon) geometry, builder);
            } else if (geometry instanceof MultiPoint) {
                serializeMultiPoint((MultiPoint) geometry, builder);
            } else if (geometry instanceof MultiPolygon) {
                serializeMulitPolygon((MultiPolygon) geometry, builder);
            } else {
                throw new IllegalArgumentException("Geometry type [" + geometry.getGeometryType() + "] not supported");
            }
        } else if (shape instanceof com.spatial4j.core.shape.Point) {
            serializePoint((com.spatial4j.core.shape.Point) shape, builder);
        } else if (shape instanceof Rectangle) {
            serializeRectangle((Rectangle) shape, builder);
        } else {
            throw new IllegalArgumentException("Shape type [" + shape.getClass().getSimpleName() + "] not supported");
        }
    }

    /**
     * Serializes the given {@link Rectangle}
     *
     * @param rectangle Rectangle that will be serialized
     * @param builder   XContentBuilder it will be serialized to
     * @throws IOException Thrown if an error occurs while writing to the XContentBuilder
     */
    private static void serializeRectangle(Rectangle rectangle, XContentBuilder builder) throws IOException {
        builder.field("type", "Envelope")
                .startArray("coordinates")
                .startArray().value(rectangle.getMinX()).value(rectangle.getMaxY()).endArray()
                .startArray().value(rectangle.getMaxX()).value(rectangle.getMinY()).endArray()
                .endArray();
    }

    /**
     * Serializes the given {@link Point}
     *
     * @param point   Point that will be serialized
     * @param builder XContentBuilder it will be serialized to
     * @throws IOException Thrown if an error occurs while writing to the XContentBuilder
     */
    private static void serializePoint(Point point, XContentBuilder builder) throws IOException {
        builder.field("type", "Point")
                .startArray("coordinates")
                .value(point.getX()).value(point.getY())
                .endArray();
    }

    /**
     * Serializes the given {@link com.spatial4j.core.shape.Point}
     *
     * @param point   Point that will be serialized
     * @param builder XContentBuilder it will be serialized to
     * @throws IOException Thrown if an error occurs while writing to the XContentBuilder
     */
    private static void serializePoint(com.spatial4j.core.shape.Point point, XContentBuilder builder) throws IOException {
        builder.field("type", "Point")
                .startArray("coordinates")
                .value(point.getX()).value(point.getY())
                .endArray();
    }

    /**
     * Serializes the given {@link LineString}
     *
     * @param lineString LineString that will be serialized
     * @param builder    XContentBuilder it will be serialized to
     * @throws IOException Thrown if an error occurs while writing to the XContentBuilder
     */
    private static void serializeLineString(LineString lineString, XContentBuilder builder) throws IOException {
        builder.field("type", "LineString")
                .startArray("coordinates");
        for (Coordinate coordinate : lineString.getCoordinates()) {
            serializeCoordinate(coordinate, builder);
        }
        builder.endArray();
    }

    /**
     * Serializes the given {@link Polygon}
     *
     * @param polygon Polygon that will be serialized
     * @param builder XContentBuilder it will be serialized to
     * @throws IOException Thrown if an error occurs while writing to the XContentBuilder
     */
    private static void serializePolygon(Polygon polygon, XContentBuilder builder) throws IOException {
        builder.field("type", "Polygon")
                .startArray("coordinates");
        serializePolygonCoordinates(polygon, builder);
        builder.endArray();
    }

    /**
     * Serializes the actual coordinates of the given {@link Polygon}
     *
     * @param polygon Polygon whose coordinates will be serialized
     * @param builder XContentBuilder it will be serialized to
     * @throws IOException Thrown if an error occurs while writing to the XContentBuilder
     */
    private static void serializePolygonCoordinates(Polygon polygon, XContentBuilder builder) throws IOException {
        builder.startArray(); // start outer ring
        for (Coordinate coordinate : polygon.getExteriorRing().getCoordinates()) {
            serializeCoordinate(coordinate, builder);
        }
        builder.endArray(); // end outer ring
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            LineString interiorRing = polygon.getInteriorRingN(i);
            builder.startArray();
            for (Coordinate coordinate : interiorRing.getCoordinates()) {
                serializeCoordinate(coordinate, builder);
            }
            builder.endArray();
        }
    }

    /**
     * Serializes the given {@link MultiPolygon}
     *
     * @param multiPolygon MultiPolygon that will be serialized
     * @param builder XContentBuilder it will be serialized to
     * @throws IOException Thrown if an error occurs while writing to the XContentBuilder
     */
    private static void serializeMulitPolygon(MultiPolygon multiPolygon, XContentBuilder builder) throws IOException {
        builder.field("type", "MultiPolygon")
                .startArray("coordinates");
        for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
            builder.startArray();
            serializePolygonCoordinates((Polygon) multiPolygon.getGeometryN(i), builder);
            builder.endArray();
        }
        builder.endArray();
    }

    /**
     * Serializes the given {@link MultiPoint}
     *
     * @param multiPoint MultiPoint that will be serialized
     * @param builder    XContentBuilder it will be serialized to
     * @throws IOException Thrown if an error occurs while writing to the XContentBuilder
     */
    private static void serializeMultiPoint(MultiPoint multiPoint, XContentBuilder builder) throws IOException {
        builder.field("type", "MultiPoint")
                .startArray("coordinates");
        for (Coordinate coordinate : multiPoint.getCoordinates()) {
            serializeCoordinate(coordinate, builder);
        }
        builder.endArray();
    }

    /**
     * Serializes the given {@link Coordinate}
     *
     * @param coordinate Coordinate that will be serialized
     * @param builder    XContentBuilder it will be serialized to
     * @throws IOException Thrown if an error occurs while writing to the XContentBuilder
     */
    private static void serializeCoordinate(Coordinate coordinate, XContentBuilder builder) throws IOException {
        builder.startArray().value(coordinate.x).value(coordinate.y).endArray();
    }
}