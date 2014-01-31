
package org.xbib.elasticsearch.river.jdbc.support;

import java.io.IOException;
import java.util.Map;

/**
 * A structured object is composed by an object data source together with
 * meta data about the object.
 */
public interface StructuredObject extends PseudoColumnNames, Comparable<StructuredObject> {

    StructuredObject optype(String optype);

    String optype();

    StructuredObject index(String index);

    String index();

    StructuredObject type(String type);

    String type();

    StructuredObject id(String id);

    String id();

    StructuredObject meta(String key, String value);

    String meta(String key);

    StructuredObject source(Map<String, ? super Object> source);

    Map source();

    String build() throws IOException;

    boolean isEmpty();

}
