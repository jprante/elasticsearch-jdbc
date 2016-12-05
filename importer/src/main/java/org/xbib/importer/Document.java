package org.xbib.importer;

import java.io.IOException;
import java.util.Map;

/**
 * A document is composed by a source together with meta data about the document.
 */
public interface Document {

    /**
     * Set the operation type, either "index", "create", or "delete".
     *
     * @param optype the operation type
     */
    void setOperationType(String optype);

    /**
     * Get the operation type.
     *
     * @return the operation type
     */
    String getOperationType();

    /**
     * Set the index.
     *
     * @param index the index
     */
    void setIndex(String index);

    /**
     * Get the index.
     *
     * @return the index
     */
    String getIndex();

    /**
     * Set the type.
     *
     * @param type the type
     */
    void setType(String type);

    /**
     * Get the type.
     *
     * @return the type
     */
    String getType();

    /**
     * Set the ID.
     *
     * @param id the ID
     */
    void setId(String id);

    /**
     * Get the ID.
     *
     * @return the ID
     */
    String getId();

    /**
     * Set meta data.
     *
     * @param key   the meta data key
     * @param value the meta data value
     */
    void setMeta(String key, String value);

    String getMeta(String key);

    boolean hasMeta(String key);

    void setSource(Map<String, Object> source);

    Map<String, Object> getSource();

    void merge(String key, Object value);

    String build() throws IOException;
}
