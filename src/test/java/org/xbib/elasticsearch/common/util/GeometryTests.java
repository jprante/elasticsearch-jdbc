package org.xbib.elasticsearch.common.util;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.testng.annotations.Test;

import java.io.IOException;
import java.text.ParseException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class GeometryTests {

    protected final static Logger logger = LogManager.getLogger("test.geo");

    @Test
    public void convert() throws ParseException, IOException {
        String s = "POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7,5 5))";
        SpatialContext ctx = JtsSpatialContext.GEO;
        Shape shape = ctx.readShapeFromWkt(s);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        GeoJSONShapeSerializer.serialize(shape, builder);
        builder.endObject();
        logger.info("geo={}", builder.string());

    }
}
