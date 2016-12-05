package org.xbib.importer.elasticsearch.plugin.json;

import com.fasterxml.jackson.core.JsonParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.importer.Document;
import org.xbib.importer.plugin.DocumentPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 */
public class JsonPlugin implements DocumentPlugin {

    private static final Logger logger = Logger.getLogger(JsonPlugin.class.getName());

    public JsonPlugin() {
        logger.info("Json plugin installed");
    }

    @Override
    public boolean execute(Document document, String key, Object value) throws IOException {
        if (value == null) {
            return false;
        }
        Object o = value;
        try {
            XContentParser parser = JsonXContent.jsonXContent.createParser(value.toString());
            XContentParser.Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            if (token == XContentParser.Token.START_OBJECT) {
                o = parser.map();
            } else if (token == XContentParser.Token.START_ARRAY) {
                o = parser.list();
            }
        } catch (JsonParseException e) {
            return false;
        }
        // if result is null, use original value
        if (o == null || (o instanceof Map && ((Map) o).isEmpty())) {
            o = value;
        }
        document.merge(key, o);
        return true;
    }

    @Override
    public boolean executeMeta(Document document, String key, Object value) throws IOException {
        return false;
    }
}
