
package org.xbib.io.keyvalue;

/**
 * This class represents two entities with an ordered pair relationship.
 * The first entity is the key and the second is the value.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class KeyValue<K,V> {

    private final K key;

    private final V value;

    public KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    public String toString() {
        return key + " => " + value;
    }
}
