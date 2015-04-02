package org.xbib.elasticsearch.plugin.jdbc.json;

import org.elasticsearch.common.jackson.core.JsonParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A JsonXContentParser that can return a parsed list or a map
 */
public class CustomJsonXContentParser extends JsonXContentParser {

    public CustomJsonXContentParser(JsonParser parser) {
        super(parser);
    }

    public Object mapOrList() throws IOException {
        XContentParser.Token token = this.currentToken();
        if(token == null) {
            token = this.nextToken();
        }
        if(token == XContentParser.Token.START_OBJECT) {
            return readMap(this, SIMPLE_MAP_FACTORY);
        } else if(token == XContentParser.Token.START_ARRAY) {
            return readList(this, SIMPLE_MAP_FACTORY, token);
        }
        return null;
    }
    
    public Object mapOrListAndClose() throws IOException {
        try {
            return mapOrList();
        } finally {
            close();
        }
    }
    
    public List<Object> list() throws IOException {
        XContentParser.Token token = this.currentToken();
        if (token == null) {
            token = this.nextToken();
        }
        return readList(this, SIMPLE_MAP_FACTORY, token);
    }

    public List<Object> listAndClose() throws IOException {
        try {
            return list();
        } finally {
            close();
        }
    }

    static interface MapFactory {
        Map<String, Object> newMap();
    }

    static final MapFactory SIMPLE_MAP_FACTORY = new MapFactory() {
        @Override
        public Map<String, Object> newMap() {
            return new HashMap<>();
        }
    };

    static final MapFactory ORDERED_MAP_FACTORY = new MapFactory() {
        @Override
        public Map<String, Object> newMap() {
            return new LinkedHashMap<>();
        }
    };

    static Map<String, Object> readMap(XContentParser parser) throws IOException {
        return readMap(parser, SIMPLE_MAP_FACTORY);
    }

    static Map<String, Object> readOrderedMap(XContentParser parser) throws IOException {
        return readMap(parser, ORDERED_MAP_FACTORY);
    }

    static Map<String, Object> readMap(XContentParser parser, MapFactory mapFactory) throws IOException {
        Map<String, Object> map = mapFactory.newMap();
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            // Must point to field name
            String fieldName = parser.currentName();
            // And then the value...
            token = parser.nextToken();
            Object value = readValue(parser, mapFactory, token);
            map.put(fieldName, value);
        }
        return map;
    }

    private static List<Object> readList(XContentParser parser, MapFactory mapFactory, XContentParser.Token token) throws IOException {
        ArrayList<Object> list = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            list.add(readValue(parser, mapFactory, token));
        }
        return list;
    }

    private static Object readValue(XContentParser parser, MapFactory mapFactory, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return parser.text();
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = parser.numberType();
            if (numberType == XContentParser.NumberType.INT) {
                return parser.intValue();
            } else if (numberType == XContentParser.NumberType.LONG) {
                return parser.longValue();
            } else if (numberType == XContentParser.NumberType.FLOAT) {
                return parser.floatValue();
            } else if (numberType == XContentParser.NumberType.DOUBLE) {
                return parser.doubleValue();
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            return parser.booleanValue();
        } else if (token == XContentParser.Token.START_OBJECT) {
            return readMap(parser, mapFactory);
        } else if (token == XContentParser.Token.START_ARRAY) {
            return readList(parser, mapFactory, token);
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            return parser.binaryValue();
        }
        return null;
    }

}
