package org.xbib.importer.elasticsearch.plugin.geo;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.xbib.importer.Document;
import org.xbib.importer.plugin.DocumentPlugin;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 */
public class GeoPlugin  implements DocumentPlugin {

    private static final Logger logger = Logger.getLogger(GeoPlugin.class.getName());

    private List<String> geoKeys;

    public GeoPlugin() {
        logger.info("Geo plugin installed");
    }

    @Override
    public boolean execute(Document document, String key, Object value) throws IOException {
        if (value == null) {
            return false;
        }
        String s = value.toString();
        if (s.startsWith("POLYGON(") || s.startsWith("POINT(")) {
            SpatialContext ctx = JtsSpatialContext.GEO;
            Shape shape = ctx.getFormats().read(s);
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ElasticsearchGeoJSONShapeSerializer.serialize(shape, builder);
            builder.endObject();
            document.merge(key, builder.string());
            return true;
        }
        return false;
    }

    @Override
    public boolean executeMeta(Document document, String k, Object v) throws IOException {
        return false;
    }
}