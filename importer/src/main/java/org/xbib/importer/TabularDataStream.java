package org.xbib.importer;

import java.io.IOException;
import java.util.List;

/**
 *
 * @param <K> key type parameter
 * @param <V> value type parameter
 */
public interface TabularDataStream<K, V> {

    /**
     * Begin a tabular data stream.
     *
     * @throws IOException if this method fails
     */
    void begin() throws IOException;

    /**
     * Declare the keys for the values
     *
     * @param keys the keys
     * @throws IOException if this method fails
     */
    void keys(List<K> keys) throws IOException;

    /**
     * Receive values for the declared keys
     *
     * @param values the values
     * @throws IOException if this method fails
     */
    void values(List<V> values) throws IOException;

    /**
     * End tabular data stream.
     *
     * @throws IOException if this method fails
     */
    void end() throws IOException;
}
