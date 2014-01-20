
package org.xbib.elasticsearch.river.jdbc.support;

import java.io.IOException;
import java.util.Collection;

/**
 * Key/Value stream listener interface for listenering to a stream of key/value attributes,
 * like a sequence of rows of columns.
 */
public interface KeyValueStreamListener {

    /**
     * Begin a key/value sequence
     *
     * @return this value listener
     * @throws IOException
     */
    KeyValueStreamListener begin() throws IOException;

    /**
     * Declare the keys for the values
     *
     * @param keys the keys
     * @return this ValueListener
     */
    KeyValueStreamListener keys(Collection<String> keys) throws IOException;

    /**
     * Receive values for the declared keys
     *
     * @param values the values
     * @return this ValueListener
     * @throws IOException
     */
    KeyValueStreamListener values(Collection<? extends Object> values) throws IOException;

    /**
     * End a key/value sequence
     *
     * @return this listener
     * @throws IOException
     */
    KeyValueStreamListener end() throws IOException;

}
