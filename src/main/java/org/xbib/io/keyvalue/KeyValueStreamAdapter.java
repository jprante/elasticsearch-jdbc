
package org.xbib.io.keyvalue;

import java.io.IOException;
import java.util.List;

public class KeyValueStreamAdapter<K,V> implements KeyValueStreamListener<K,V> {

    /**
     * Begin a key/value sequence
     *
     * @return this value listener
     * @throws java.io.IOException
     */
    public KeyValueStreamListener<K,V> begin() throws IOException {
        return this;
    }

    /**
     * Receive key/value pair
     * @param keyValue the key/value pair
     */
    public KeyValueStreamListener<K,V> keyValue(KeyValue<K,V> keyValue) throws IOException {
        return this;
    }

    /**
     * Receive key/value pair
     * @param key the key
     * @param value the value
     */
    public KeyValueStreamListener<K,V> keyValue(K key, V value) throws IOException {
        return this;
    }

    /**
     * Declare the keys for the values
     *
     * @param keys the keys
     * @return this ValueListener
     */
    public KeyValueStreamListener<K,V> keys(List<K> keys) throws IOException {
        return this;
    }

    /**
     * Receive values for the declared keys
     *
     * @param values the values
     * @return this ValueListener
     * @throws java.io.IOException
     */
    public KeyValueStreamListener<K,V> values(List<V> values) throws IOException {
        return this;
    }

    /**
     * End a key/value sequence
     *
     * @return this listener
     * @throws java.io.IOException
     */
    public KeyValueStreamListener<K,V> end() throws IOException {
        return this;
    }
}
