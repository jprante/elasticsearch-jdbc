package org.xbib.elasticsearch.river.jdbc.support;

import org.xbib.elasticsearch.plugin.jdbc.util.IndexableObject;
import org.xbib.elasticsearch.plugin.jdbc.util.PlainKeyValueStreamListener;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;

import java.io.IOException;

/**
 * This class consumes pairs from a key/value stream
 * and transports them to the river mouth.
 */
public class StringKeyValueStreamListener extends PlainKeyValueStreamListener<String, String> {

    private RiverMouth output;

    public StringKeyValueStreamListener output(RiverMouth output) {
        this.output = output;
        return this;
    }

    /**
     * The object is complete. Push it to the river mouth.
     *
     * @param object the object
     * @return this value listener
     * @throws IOException
     */
    public StringKeyValueStreamListener end(IndexableObject object) throws IOException {
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
