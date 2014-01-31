package org.xbib.elasticsearch.river.jdbc.support;

import org.elasticsearch.ElasticSearchParseException;

import java.util.Map;


public class CompatUtil {

    public static Map<String, Object> nodeMapValue(Object node, String desc) {
        if (node instanceof Map) {
            return (Map<String, Object>) node;
        } else {
            throw new ElasticSearchParseException(desc + " should be a hash but was of type: " + node.getClass());
        }
    }
}
