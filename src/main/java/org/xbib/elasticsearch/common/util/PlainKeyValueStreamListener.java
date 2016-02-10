/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.common.util;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.elasticsearch.common.keyvalue.KeyValueStreamListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class PlainKeyValueStreamListener<K, V> implements KeyValueStreamListener<K, V> {

    private final static Pattern p = Pattern.compile("^(.*)\\[(.*?)\\]$");

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

    private boolean shouldAutoGenID;

    private boolean shouldIgnoreNull = false;

    private boolean shouldDetectGeo = true;

    private boolean shouldDetectJson = true;

    /**
     * Set custom delimiter
     *
     * @param delimiter the delimiter
     * @return this listener
     */
    public PlainKeyValueStreamListener delimiter(char delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public PlainKeyValueStreamListener shouldIgnoreNull(boolean shouldIgnoreNull) {
        this.shouldIgnoreNull = shouldIgnoreNull;
        return this;
    }

    public PlainKeyValueStreamListener shouldDetectGeo(boolean shouldDetectGeo) {
        this.shouldDetectGeo = shouldDetectGeo;
        return this;
    }

    public PlainKeyValueStreamListener shouldDetectJson(boolean shouldDetectJson) {
        this.shouldDetectJson = shouldDetectJson;
        return this;
    }

    /**
     * @return this value listener
     * @throws java.io.IOException when beginning the object gives an error
     */
    @Override
    public KeyValueStreamListener<K, V> begin() throws IOException {
        return this;
    }

    @Override
    public KeyValueStreamListener<K, V> keyValue(K key, V value) throws IOException {
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
        this.shouldAutoGenID = shouldAutoGenID();
        return this;
    }

    /**
     * Receive values.
     *
     * @param values the values
     * @return this value listener
     * @throws java.io.IOException when value processing giv an error
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
        for (int i = 0; i < keys.size() && i < values.size(); i++) {
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
        if (!current.equals(prev) || current.isEmpty() || shouldAutoGenID) {
            prev.source(current.source()); // "steal" source
            end(prev); // here, the element is being prepared for bulk indexing
            prev = current;
            current = newObject();
        }
        // create current object from values by sequentially merging the values
        for (int i = 0; i < keys.size() && i < values.size(); i++) {
            Object v = null;
            try {
                String s = values.get(i).toString();
                // geo content?
                if (shouldDetectGeo && s.startsWith("POLYGON(") || s.startsWith("POINT(")) {
                    SpatialContext ctx = JtsSpatialContext.GEO;
                    Shape shape = ctx.readShapeFromWkt(s);
                    XContentBuilder builder = jsonBuilder();
                    builder.startObject();
                    GeoJSONShapeSerializer.serialize(shape, builder);
                    builder.endObject();
                    s = builder.string();
                }
                // JSON content?
                if (shouldDetectJson) {
                    XContentParser parser = JsonXContent.jsonXContent.createParser(s);
                    XContentParser.Token token = parser.currentToken();
                    if(token == null) {
                        token = parser.nextToken();
                    }
                    if (token == XContentParser.Token.START_OBJECT) {
                        v = parser.map();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        v = parser.list();
                    }
                }
            } catch (Exception e) {
                // ignore
            }
            if(v == null || (v instanceof Map && ((Map) v).isEmpty())) {
                v = values.get(i);
            }
            Map<String, Object> m = merge(current.source(), keys.get(i), v);
            current.source(m);
        }
        return this;
    }

    protected void map(K k, V v, IndexableObject current) throws IOException {
        if (ControlKeys._optype.name().equalsIgnoreCase(k.toString())) {
            current.optype(v.toString());
        } else if (ControlKeys._index.name().equalsIgnoreCase(k.toString())) {
            current.index(v.toString());
        } else if (ControlKeys._type.name().equalsIgnoreCase(k.toString())) {
            current.type(v.toString());
        } else if (ControlKeys._id.name().equalsIgnoreCase(k.toString())) {
            current.id(v.toString());
        } else if (ControlKeys._version.name().equalsIgnoreCase(k.toString())) {
            current.meta(ControlKeys._version.name(), v.toString());
        } else if (ControlKeys._routing.name().equalsIgnoreCase(k.toString())) {
            current.meta(ControlKeys._routing.name(), v.toString());
        } else if (ControlKeys._parent.name().equalsIgnoreCase(k.toString())) {
            current.meta(ControlKeys._parent.name(), v.toString());
        } else if (ControlKeys._timestamp.name().equalsIgnoreCase(k.toString())) {
            current.meta(ControlKeys._timestamp.name(), v.toString());
        } else if (ControlKeys._ttl.name().equalsIgnoreCase(k.toString())) {
            current.meta(ControlKeys._ttl.name(), v.toString());
        } else if (ControlKeys._job.name().equalsIgnoreCase(k.toString())) {
            current.meta(ControlKeys._job.name(), v.toString());
        } else if (ControlKeys._source.name().equalsIgnoreCase(k.toString())) {
            current.source(JsonXContent.jsonXContent.createParser(v.toString()).map());
        }
    }

    /**
     * End of values.
     *
     * @return this value listener
     * @throws java.io.IOException if this method fails
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
     * @throws java.io.IOException when ending the object gives an error
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
     */
    private final static Set<String> controlKeys = ControlKeys.makeSet();

    @SuppressWarnings({"unchecked"})
    protected Map<String, Object> merge(Map<String, Object> map, Object k, Object value) {
        String key = k.toString();
        if (controlKeys.contains(key)) {
            return map;
        }
        int i = key.indexOf(delimiter);
        String index = null;
        boolean isSequence = false;
        Matcher matcher = p.matcher(key);
        if (matcher.matches()) {
            isSequence = key.indexOf("[") < i;
        }
        if (i <= 0 || isSequence) {
            isSequence = matcher.matches();
            String head = key;
            if (isSequence) {
                head = matcher.group(1);
                index = matcher.group(2);
            }
            if (index == null || index.isEmpty()) {
                map.put(head, new Values(map.get(head), value, isSequence));
            } else {
                if (!map.containsKey(head)) {
                    map.put(head, new LinkedList());
                }
                Object o = map.get(head);
                if (o instanceof List) {
                    List l = (List) o;
                    int j = l.isEmpty() ? -1 : l.size() - 1;
                    if (j >= 0) {
                        Map<String, Object> m = (Map<String, Object>) l.get(j);
                        int delimiterOffset = index.indexOf(delimiter);
                        if (delimiterOffset >= 0) {
                            if (!containsKeyInDotNotation(index, m)) {
                                l.set(j, merge(m, index, value)); // append
                            } else {
                                l.add(merge(new LinkedHashMap(), index, value));
                            }
                        } else {
                            if (!m.containsKey(index)) {
                                l.set(j, merge(m, index, value)); // append
                            } else {
                                l.add(merge(new LinkedHashMap(), index, value));
                            }
                        }
                    } else {
                        l.add(merge(new LinkedHashMap(), index, value));
                    }
                }
            }
        } else {
            String head = key.substring(0, i);
            matcher = p.matcher(head);
            isSequence = matcher.matches();
            if (isSequence) {
                head = matcher.group(1);
            }
            String tail = key.substring(i + 1);
            if (map.containsKey(head)) {
                Object o = map.get(head);
                if (o instanceof Map) {
                    merge((Map<String, Object>) o, tail, value);
                } else if (o instanceof Values) {
                    // head of Values a Map?
                    o = ((Values) o).getValues()[0];
                    if (o instanceof Map) {
                        merge((Map<String, Object>) o, tail, value);
                    } else {
                        throw new IllegalArgumentException("illegal head: " + head);
                    }
                } else {
                    throw new IllegalArgumentException("illegal head: " + head);
                }
            } else {
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                map.put(head, m);
                merge(m, tail, value);
            }
        }
        return map;
    }

    /**
     * Create a new structured object
     *
     * @return a new structured object
     */
    private IndexableObject newObject() {
        Map<String,String> map = new HashMap<>();
        map.put("ignore_null", Boolean.toString(shouldIgnoreNull));
        map.put("force_array", "false");
        return new PlainIndexableObject(new ToXContent.MapParams(map));
    }

    private boolean shouldAutoGenID() {
        boolean b = true;
        for (K key : keys) {
            if (ControlKeys._id.name().equals(key)) {
                b = false;
                break;
            }
        }
        return b;
    }

    private boolean containsKeyInDotNotation(String key, Map mapToCheck) {
        int delimiterIndex = key.indexOf(delimiter);
        if (delimiterIndex < 0) {
            return mapToCheck.containsKey(key);
        } else {
            String head = key.substring(0, delimiterIndex);
            String tail = key.substring(delimiterIndex + 1);
            Object nextObject = mapToCheck.get(head);
            if (nextObject != null) {
                if (nextObject instanceof Map) {
                    Map nextMap = (Map) nextObject;
                    return containsKeyInDotNotation(tail, nextMap);
                }
                return false;
            } else {
                return false;
            }
        }
    }

}
