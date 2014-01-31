
package org.xbib.elasticsearch.river.jdbc.support;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.common.xcontent.json.JsonXContent;

import org.xbib.elasticsearch.river.jdbc.RiverMouth;

/**
 * The SimpleValueListener class consumes values from a tabular source and transports
 * them to the river indexer.
 */
public class StructuredObjectKeyValueStreamListener<O extends Object> implements KeyValueStreamListener {

    /**
     * The current structured object
     */
    private StructuredObject current;
    /**
     * The object before the current object
     */
    private StructuredObject prev;
    /**
     * The river target where the structured objects will be moved
     */
    private RiverMouth output;
    /**
     * The keys of the values. They are examined for the Elasticsearch index
     * attributes.
     */
    private List<String> keys;

    public StructuredObjectKeyValueStreamListener output(RiverMouth output) {
        this.output = output;
        return this;
    }

    /**
     * Just some syntactic sugar.
     *
     * @return this value listener
     * @throws IOException
     */
    @Override
    public StructuredObjectKeyValueStreamListener begin() throws IOException {
        return this;
    }

    /**
     * Set the keys.
     *
     * @param keys the keys
     * @return this value listener
     */
    @Override
    public StructuredObjectKeyValueStreamListener keys(Collection<String> keys) throws IOException {
        this.keys = new LinkedList<String>(keys);
        return this;
    }

    /**
     * Receive values.
     *
     * @param values the values
     * @return this value listener
     * @throws IOException
     */
    @Override
    public StructuredObjectKeyValueStreamListener values(Collection<? extends Object> values) throws IOException {
        boolean hasSource = false;
        if (current == null) {
            current = newObject();
        }
        if (prev == null) {
            prev = newObject();
        }
        // execute meta operations on pseudo columns
        int i = 0;
        for (Object o : values) {
            if (o == null) {
                continue;
            }
            String v = o.toString();
            String k = keys.get(i);
            map(k, v, current);
            if (StructuredObject.SOURCE.equals(k)) {
                hasSource = true;
            }
            i++;
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
        i = 0;
        for (Object o : values) {
            Map map = null;
            try {
                map = JsonXContent.jsonXContent.createParser(o.toString()).mapAndClose();
            } catch (Exception e) {
                // ignore
            }
            Map m = merge(current.source(),
                    keys.get(i),
                    map != null && map.size() > 0 ? map : o);
            current.source(m);
            i++;
        }
        return this;
    }

    /**
     * End of value sequence
     *
     * @return this value listener
     * @throws IOException
     */
    @Override
    public StructuredObjectKeyValueStreamListener end() throws IOException {
        if (prev != null) {
            prev.source(current.source());
            end(prev);
        }
        prev = newObject();
        current = newObject();
        return this;
    }

    protected void map(String k, String v, StructuredObject current) throws IOException {
        if (StructuredObject.JOB.equals(k)) {
            current.meta(StructuredObject.JOB, v);
        } else if (StructuredObject.OPTYPE.equals(k)) {
            current.optype(v);
        } else if (StructuredObject.INDEX.equals(k)) {
            current.index(v);
        } else if (StructuredObject.TYPE.equals(k)) {
            current.type(v);
        } else if (StructuredObject.ID.equals(k)) {
            current.id(v);
        } else if (StructuredObject.VERSION.equals(k)) {
            current.meta(StructuredObject.VERSION, v);
        } else if (StructuredObject.ROUTING.equals(k)) {
            current.meta(StructuredObject.ROUTING, v);
        } else if (StructuredObject.PERCOLATE.equals(k)) {
            current.meta(StructuredObject.PERCOLATE, v);
        } else if (StructuredObject.PARENT.equals(k)) {
            current.meta(StructuredObject.PARENT, v);
        } else if (StructuredObject.TIMESTAMP.equals(k)) {
            current.meta(StructuredObject.TIMESTAMP, v);
        } else if (StructuredObject.TTL.equals(k)) {
            current.meta(StructuredObject.TTL, v);
        } else if (StructuredObject.SOURCE.equals(k)) {
            current.source(JsonXContent.jsonXContent.createParser(v).mapAndClose());
        }
    }


    /**
     * The object is complete. Push it to the river target.
     *
     * @param object the object
     * @return this value listener
     * @throws IOException
     */
    public StructuredObjectKeyValueStreamListener end(StructuredObject object) throws IOException {
        if (object.source().isEmpty()) {
            return this;
        }
        if (output != null) {
            if (object.optype() == null) {
                output.index(object, false);
            } else if (Operations.OP_INDEX.equals(object.optype())) {
                output.index(object, false);
            } else if (Operations.OP_CREATE.equals(object.optype())) {
                output.index(object, true);
            } else if (Operations.OP_DELETE.equals(object.optype())) {
                output.delete(object);
            } else {
                throw new IllegalArgumentException("unknown optype: " + object.optype());
            }
        }
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
     * @param map   the map for the JSON object
     * @param key   the key
     * @param value the value
     */
    protected Map<String, Values<O>> merge(Map map, String key, Object value) {
        if (PseudoColumnNames.INDEX.equals(key)
                || PseudoColumnNames.ID.equals(key)
                || PseudoColumnNames.TYPE.equals(key)
                || PseudoColumnNames.PARENT.equals(key)) {
            return map;
        }
        int i = key.indexOf('.');
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
                    map.put(head, new ArrayList());
                }
                Object o = map.get(head);
                if (o instanceof List) {
                    List l = (List) o;
                    int j = l.isEmpty() ? -1 : l.size() - 1;
                    if (j >= 0) {
                        Map<String, Values<O>> m = (Map<String, Values<O>>) l.get(j);
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
                    merge((Map<String, Values<O>>) o, tail, value);
                } else {
                    throw new IllegalArgumentException("illegal head: " + head);
                }
            } else {
                Map<String, Values<O>> m = new HashMap<String, Values<O>>();
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
    protected StructuredObject newObject() {
        return new PlainStructuredObject();
    }

}
