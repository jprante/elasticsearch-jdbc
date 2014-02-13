
package org.xbib.elasticsearch.gatherer;

import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.io.keyvalue.KeyValue;
import org.xbib.io.keyvalue.KeyValueStreamListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

/**
 */
public class PlainKeyValueStreamListener<K,V> implements KeyValueStreamListener<K,V> {

    /**
     * The current structured object
     */
    private IndexableObject current;
    /**
     * The object before the current object
     */
    private IndexableObject prev;

    /**
     * The keys of the values. They are examined for the Elasticsearch index
     * attributes.
     */
    private List<K> keys;
    /**
     * The delimiter between the key names in the path, used for string split.
     * Should not be modified.
     */
    private char delimiter = '.';

    /**
     * Set custom delimiter
     * @param delimiter the delimiter
     * @return this listener
     */
    public PlainKeyValueStreamListener delimiter(char delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    /**
     *
     * @return this value listener
     * @throws java.io.IOException
     */
    @Override
    public KeyValueStreamListener<K, V> begin() throws IOException {
        return this;
    }

    @Override
    public KeyValueStreamListener<K, V> keyValue(K key, V value) throws IOException {
        return this;
    }

    @Override
    public KeyValueStreamListener<K, V> keyValue(KeyValue<K,V> value) throws IOException {
        return this;
    }

    /**
     * Set the keys.
     *
     * @param keys the keys
     * @return this value listener
     */
    @Override
    public KeyValueStreamListener<K, V> keys(List<K> keys) throws IOException {
        this.keys = keys;
        return this;
    }

    /**
     * Receive values.
     *
     * @param values the values
     * @return this value listener
     * @throws java.io.IOException
     */
    @Override
    public KeyValueStreamListener<K, V> values(List<V> values) throws IOException {
        boolean hasSource = false;
        if (current == null) {
            current = newObject();
        }
        if (prev == null) {
            prev = newObject();
        }
        // execute meta operations
        for (int i = 0; i < values.size(); i++) {
            // v may be null, then continue
            V v = values.get(i);
            if (v == null) {
                continue;
            }
            K k = keys.get(i);
            map(k, v, current);
            if (ControlKeys._source.name().equals(k)) {
                hasSource = true;
            }
        }
        if (hasSource) {
            end(current);
            current = newObject();
            return this;
        }
        // switch to next structured object if current is not equal to previous
        if (!current.equals(prev) || current.isEmpty()) {
            prev.source(current.source()); // "steal" source
            end(prev); // here, the element is being prepared for bulk indexing
            prev = current;
            current = newObject();
        }
        // create current object from values by sequentially merging the values
        for (int i = 0; i < keys.size(); i++) {
            Map map = null;
            try {
                // JSON content?
                map = JsonXContent.jsonXContent.createParser(values.get(i).toString()).mapAndClose();
            } catch (Exception e) {
                // ignore
            }
            Object v = map != null && map.size() > 0 ? map : values.get(i);
            Map m = merge(current.source(), keys.get(i), v);
            current.source(m);
        }
        return this;
    }

    protected void map(K k, V v, IndexableObject current) throws IOException {
        if (ControlKeys._optype.name().equals(k)) {
            current.optype(v.toString());
        } else if (ControlKeys._index.name().equals(k)) {
            current.index(v.toString());
        } else if (ControlKeys._type.name().equals(k)) {
            current.type(v.toString());
        } else if (ControlKeys._id.name().equals(k)) {
            current.id(v.toString());
        } else if (ControlKeys._version.name().equals(k)) {
            current.meta(k.toString(), v.toString());
        } else if (ControlKeys._routing.name().equals(k)) {
            current.meta(k.toString(), v.toString());
        } else if (ControlKeys._parent.name().equals(k)) {
            current.meta(k.toString(), v.toString());
        } else if (ControlKeys._timestamp.name().equals(k)) {
            current.meta(k.toString(), v.toString());
        } else if (ControlKeys._ttl.name().equals(k)) {
            current.meta(k.toString(), v.toString());
        } else if (ControlKeys._job.name().equals(k)) {
            current.meta(k.toString(), v.toString());
        } else if (ControlKeys._source.name().equals(k)) {
            current.source(JsonXContent.jsonXContent.createParser(v.toString()).mapAndClose());
        }
    }

    /**
     * End of values.
     *
     * @return this value listener
     * @throws java.io.IOException
     */
    public KeyValueStreamListener<K, V> end() throws IOException {
        if (prev != null) {
            prev.source(current.source());
            end(prev);
        }
        prev = newObject();
        current = newObject();
        return this;
    }

    /**
     * The object is complete
     *
     * @param object the object
     * @return this value listener
     * @throws java.io.IOException
     */
    public KeyValueStreamListener<K, V> end(IndexableObject object) throws IOException {
        return this;
    }

    /**
     * Merge key/value pair to a map holding a JSON object. The key consists of
     * a path pointing to the value position in the JSON object. The key,
     * representing a path, is divided into head/tail. The recursion terminates
     * if there is only a head and no tail. In this case, the value is added as
     * a tuple to the map. If the head key exists, the merge process is
     * continued by following the path represented by the key. If the path does
     * not exist, a new map is created. A conflict arises if there is no map at
     * a head key position. Then, the prefix given in the path is considered
     * illegal.
     *
     */
    private final static Set<String> controlKeys = ControlKeys.makeSet();

    protected Map<String, Values<Object>> merge(Map map, Object k, Object value) {
        String key = k.toString();
        if (controlKeys.contains(key)) {
            return map;
        }
        int i = key.indexOf(delimiter);
        String index = null;
        if (i <= 0) {
            Matcher matcher = p.matcher(key);
            boolean isSequence = matcher.matches();
            String head = key;
            if (isSequence) {
                head = matcher.group(1);
                index = matcher.group(2);
            }
            if (index == null || index.isEmpty()) {
                map.put(head, new Values(map.get(head), value, isSequence));
            } else {
                if (!map.containsKey(head)) {
                    map.put(head, newLinkedList());
                }
                Object o = map.get(head);
                if (o instanceof List) {
                    List l = (List) o;
                    int j = l.isEmpty() ? -1 : l.size() - 1;
                    if (j >= 0) {
                        Map<String, Values<Object>> m = (Map<String, Values<Object>>) l.get(j);
                        if (!m.containsKey(index)) {
                            l.set(j, merge(m, index, value)); // append
                        } else {
                            l.add(merge(new HashMap(), index, value));
                        }
                    } else {
                        l.add(merge(new HashMap(), index, value));
                    }
                }
            }
        } else {
            String head = key.substring(0, i);
            Matcher matcher = p.matcher(head);
            boolean isSequence = matcher.matches();
            if (isSequence) {
                head = matcher.group(1);
            }
            String tail = key.substring(i + 1);
            if (map.containsKey(head)) {
                Object o = map.get(head);
                if (o instanceof Map) {
                    merge((Map<String, Values<Object>>) o, tail, value);
                } else if (o instanceof Values) {
                    // head of Values a Map?
                    o = ((Values)o).getValues()[0];
                    if (o instanceof Map) {
                        merge((Map<String, Values<Object>>) o, tail, value);
                    } else {
                        throw new IllegalArgumentException("illegal head: " + head);
                    }
                } else {
                    throw new IllegalArgumentException("illegal head: " + head);
                }
            } else {
                Map<String, Values<Object>> m = new HashMap<String, Values<Object>>();
                map.put(head, m);
                merge(m, tail, value);
            }
        }
        return map;
    }

    private final static Pattern p = Pattern.compile("^(.*)\\[(.*?)\\]$");

    /**
     * Create a new structured object
     *
     * @return a new structured object
     */
    private IndexableObject newObject() {
        return new PlainIndexableObject();
    }

}
