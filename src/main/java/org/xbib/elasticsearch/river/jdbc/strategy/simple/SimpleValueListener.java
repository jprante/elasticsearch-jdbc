/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.DigestStructuredObject;
import org.xbib.elasticsearch.river.jdbc.support.Operations;
import org.xbib.elasticsearch.river.jdbc.support.PlainStructuredObject;
import org.xbib.elasticsearch.river.jdbc.support.PseudoColumnNames;
import org.xbib.elasticsearch.river.jdbc.support.StructuredObject;
import org.xbib.elasticsearch.river.jdbc.support.ValueListener;
import org.xbib.elasticsearch.river.jdbc.support.Values;

/**
 * The SimpleValueListener class consumes values from a database and transports
 * them to the river target.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class SimpleValueListener<O extends Object> implements ValueListener {

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
    private RiverMouth target;
    /**
     * The keys of the values. They are examined for the Elasticsearch index
     * attributes.
     */
    private List<String> keys;
    /**
     * The delimiter between the key names in the path, used for string split.
     * Should not be modified.
     */
    private char delimiter = '.';
    /**
     * If digesting is enabled. This is the default.
     */
    private boolean digesting = true;

    private MessageDigest digest;

    public SimpleValueListener target(RiverMouth target) {
        this.target = target;
        return this;
    }

    public SimpleValueListener delimiter(char delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public SimpleValueListener digest(boolean digesting) {
        this.digesting = digesting;
        if (digesting) {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return this;
    }

    @Override
    public MessageDigest digest() {
        return digest;
    }

    /**
     * Just some syntactic sugar.
     *
     * @return this value listener
     * @throws IOException
     */
    public SimpleValueListener begin() throws IOException {
        return this;
    }

    /**
     * Set the keys.
     *
     * @param keys
     * @return this value listener
     */
    @Override
    public SimpleValueListener keys(List<String> keys) {
        this.keys = keys;
        return this;
    }

    /**
     * Receive values.
     *
     * @param values
     * @return this value listener
     * @throws IOException
     */
    @Override
    public SimpleValueListener values(List<? extends Object> values) throws IOException {
    	boolean hasSource = false;
        if (current == null) {
            current = newObject();
        }
        if (prev == null) {
            prev = newObject();
        }
        // execute meta operations on pseudo columns
        for (int i = 0; i < values.size(); i++) {
            // v may be null
            Object o = values.get(i);
            if (o == null) {
                continue;
            }
            String v = o.toString();
            // JAVA7: string switch
            String k = keys.get(i);
            map(k, v, current);
            if (StructuredObject.SOURCE.equals(k)) { 
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
        		map = JsonXContent.jsonXContent.createParser(values.get(i).toString()).mapAndClose();
        	} catch (Exception e) {}
        	
        	Map m = merge(current.source(), keys.get(i), map != null && map.size() > 0 ? map : values.get(i));
    		current.source(m);
        }
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
     * End of values.
     *
     * @return this value listener
     * @throws IOException
     */
    public SimpleValueListener end() throws IOException {
        if (prev != null) {
            prev.source(current.source());
            end(prev);
        }
        prev = newObject();
        current = newObject();
        return this;
    }

    /**
     * The object is complete. Push it to the river target.
     *
     * @param object the object
     * @return this value listener
     * @throws IOException
     */
    public SimpleValueListener end(StructuredObject object) throws IOException {
        if (object.source().isEmpty()) {
            return this;
        }
        if (target != null) {
            if (object.optype() == null) {
                target.index(object);
            } else if (Operations.OP_INDEX.equals(object.optype())) {
                target.index(object);
            } else if (Operations.OP_CREATE.equals(object.optype())) {
                target.create(object);
            } else if (Operations.OP_DELETE.equals(object.optype())) {
                target.delete(object);
            } else {
                throw new IllegalArgumentException("unknown optype: " + object.optype());
            }
        }
        return this;
    }

    /**
     * Reset this listener
     *
     * @throws IOException
     */
    @Override
    public SimpleValueListener reset() throws IOException {
        end();
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
    			|| PseudoColumnNames.PARENT.equals(key)){
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
                    map.put(head, new ArrayList());
                }
                Object o = map.get(head);
                if (o instanceof List) {
                    List l = (List)o;
                    int j = l.isEmpty() ? -1 : l.size()-1;
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
    private StructuredObject newObject() {
        return digesting ?
                new DigestStructuredObject().digest(digest) :
                new PlainStructuredObject();
    }

}
