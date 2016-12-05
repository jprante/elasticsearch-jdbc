package org.xbib.importer.elasticsearch.plugin.geo;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class GeoTests {

    @Test
    public void parsePolygon() throws ParseException, IOException {
        String s = "POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7,5 5))";
        SpatialContext ctx = JtsSpatialContext.GEO;
        Shape shape = ctx.getFormats().read(s);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        ElasticsearchGeoJSONShapeSerializer.serialize(shape, builder);
        builder.endObject();
        assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[0.0,0.0],[10.0,0.0],[10.0,10.0],[0.0,10.0],[0.0,0.0]],[[5.0,5.0],[7.0,5.0],[7.0,7.0],[5.0,7.0],[5.0,5.0]]]}",
                builder.string());

    }
}
