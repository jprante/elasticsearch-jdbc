package org.xbib.elasticsearch.plugin.jdbc;

import org.xbib.elasticsearch.river.jdbc.RiverMouth;

import java.io.IOException;

/**
 * This class consumes pairs from a key/value stream
 * and transports them to the river mouth.
 */
public class RiverMouthKeyValueStreamListener<K, V> extends PlainKeyValueStreamListener<K, V> {

    private RiverMouth output;

    public RiverMouthKeyValueStreamListener<K, V> output(RiverMouth output) {
        this.output = output;
        return this;
    }

    public RiverMouthKeyValueStreamListener<K, V> shouldIgnoreNull(boolean shouldIgnoreNull) {
        super.shouldIgnoreNull(shouldIgnoreNull);
        return this;
    }

    /**
     * The object is complete. Push it to the river mouth.
     *
     * @param object the object
     * @return this value listener
     * @throws java.io.IOException
     */
    public RiverMouthKeyValueStreamListener<K, V> end(IndexableObject object) throws IOException {
        if (object.isEmpty()) {
            return this;
        }
        if (output != null) {
            if (object.optype() == null) {
                output.index(object, false);
            } else if ("index".equals(object.optype())) {
                output.index(object, false);
            } else if ("create".equals(object.optype())) {
                output.index(object, true);
            } else if ("delete".equals(object.optype())) {
                output.delete(object);
            } else {
                throw new IllegalArgumentException("unknown optype: " + object.optype());
            }
        }
        return this;
    }

}
