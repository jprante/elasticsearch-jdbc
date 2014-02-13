
package org.xbib.io.keyvalue;

import java.io.IOException;
import java.util.List;

public interface KeyValueStreamListener<K,V> {

    /**
     * Begin a key/value sequence
     *
     * @return this value listener
     * @throws java.io.IOException
     */
    KeyValueStreamListener<K,V> begin() throws IOException;

    /**
     * Receive key/value pair
     * @param entity a key/value pair
     */
    KeyValueStreamListener<K,V> keyValue(KeyValue<K,V> entity) throws IOException;

    /**
     * Receive key/value pair
     * @param key the key
     * @param value the value
     */
    KeyValueStreamListener<K,V> keyValue(K key, V value) throws IOException;

    /**
     * Declare the keys for the values
     *
     * @param keys the keys
     * @return this ValueListener
     */
    KeyValueStreamListener<K,V> keys(List<K> keys) throws IOException;

    /**
     * Receive values for the declared keys
     *
     * @param values the values
     * @return this ValueListener
     * @throws java.io.IOException
     */
    KeyValueStreamListener<K,V> values(List<V> values) throws IOException;

    /**
     * End a key/value sequence
     *
     * @return this listener
     * @throws java.io.IOException
     */
    KeyValueStreamListener<K,V> end() throws IOException;

}
