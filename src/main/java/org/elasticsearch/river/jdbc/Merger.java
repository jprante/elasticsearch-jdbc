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
package org.elasticsearch.river.jdbc;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * The Merger class consumes SQL rows and produces Elasticsearch JSON sources.
 */
public class Merger implements RowListener {

    private ESLogger logger;

    private final static String OPTYPE_COLUMN = "_optype";
    private final static String INDEX_COLUMN = "_index";
    private final static String TYPE_COLUMN = "_type";
    private final static String ID_COLUMN = "_id";
    private final static String PARENT_COLUMN = "_parent";
    private final static String VERSION_COLUMN = "_version";
    private final static String OP_CREATE = "create";
    private final static String OP_INDEX = "index";
    private final static String OP_DELETE = "delete";
    
    private final static String DIGEST_ALGORITHM = "SHA-256";
    private final static String DIGEST_ENCODING = "UTF-8";
    
    private char delimiter;
    private XContentBuilder builder;
    private Action listener;
    private Map<String, Object> map;
    private String prevoptype;
    private String previndex;
    private String prevtype;
    private String previd;
    private String prevparent;
    private Long prevver;
    private String optype;
    private String index;
    private String type;
    private String id;
    private String parent;
    private Long ver;
    private final Long staticVersion;
    private final MessageDigest digest;
    private String digestString;
    private boolean closed;

    /**
     * Constructor for a new Merger object
     * 
     * @param action
     * @param logger ElasticSearch logger
     * @throws IOException
     * @throws NoSuchAlgorithmException 
     */
    public Merger(Action action, ESLogger logger) throws IOException, NoSuchAlgorithmException {
        this('.', action, -1L, logger);
    }
    
    /**
     * Constructor for a new Merger object
     *
     * @param action the action
     * @param version the version
     * @throws IOException
     */
    public Merger(Action action, long version) throws IOException, NoSuchAlgorithmException {
        this('.', action, version, null);
    }

    /**
     * Constructor for a new Merger object
     *
     * @param action the action
     * @param version the version
     * @param logger ElasticSearch logger
     * @throws IOException
     */
    public Merger(Action action, long version, ESLogger logger) throws IOException, NoSuchAlgorithmException {
        this('.', action, version, logger);
    }

    /**
     * Constructor for a new Merger object
     *
     * @param delimiter the delimiter
     * @param action the action
     * @param version the version
     * @param logger ElasticSearch logger
     * @throws IOException
     */
    public Merger(char delimiter, Action action, long version, ESLogger logger) throws IOException, NoSuchAlgorithmException {
        this.delimiter = delimiter;
        this.builder = jsonBuilder();
        this.listener = action;
        this.map = new HashMap<String, Object>();
        this.staticVersion = version;
        this.ver = version;
        this.digest = MessageDigest.getInstance(DIGEST_ALGORITHM);
        this.closed = false;
        this.logger = logger;
    }

    /**
     * Merge a row given by string arrays
     *
     * @param columns the column names for the row elements
     * @param row the row elements
     * @throws IOException
     */
    public void row(String[] columns, Object[] row) throws IOException {
        boolean changed = false;
        for (int i = 0; i < columns.length; i++) {
            if (OPTYPE_COLUMN.equals(columns[i])) {
                if (optype != null && !optype.equals(row[i])) {
                    changed = true;
                }
                prevoptype = optype;
                optype = (String) row[i];
            } else if (INDEX_COLUMN.equals(columns[i])) {
                if (index != null && !index.equals(row[i])) {
                    changed = true;
                }
                previndex = index;
                index = (String) row[i];
            } else if (TYPE_COLUMN.equals(columns[i])) {
                if (type != null && !type.equals(row[i])) {
                    changed = true;
                }
                prevtype = type;
                type = (String) row[i];
            } else if (ID_COLUMN.equals(columns[i])) {
                if (id != null && !id.equals(row[i])) {
                    changed = true;
                }
                previd = id;
                id = (String) row[i];
            } else if (PARENT_COLUMN.equals(columns[i])) {
            	if (parent != null && !parent.equals(row[i])) {
            		changed = true;
            	}
            	prevparent = parent;
            	parent = (String) row[i];
            } else if (VERSION_COLUMN.equals(columns[i])) {
            	if (ver != null && !ver.equals(row[i])) {
            		changed = true;
            	}
            	prevver = ver;
            	ver = (Long) row[i];
            }
        }
        if (changed) {
            flush(prevoptype, previndex, prevtype, previd, prevparent, prevver);
        }
        for (int i = 0; i < columns.length; i++) {
            if (!OPTYPE_COLUMN.equals(columns[i]) &&
                    !INDEX_COLUMN.equals(columns[i]) && 
                    !TYPE_COLUMN.equals(columns[i]) && 
                    !ID_COLUMN.equals(columns[i]) &&
                    !PARENT_COLUMN.equals(columns[i]) &&
                    !VERSION_COLUMN.equals(columns[i])) {
                merge(map, columns[i], row[i]);
            }
        }
    }

    /**
     * Implement RowListener interface. A row is coming in and needs to be merged.
     *
     * @param index the index 
     * @param type the type
     * @param id the id
     * @param keys the keys (column labels) of the row
     * @param values the values of the row
     * @throws IOException
     */
    @Override
    public void row(String optype, String index, String type, String id, String parent, Long version, List<String> keys, List<Object> values) 
            throws IOException {
        boolean changed = false;
        if (this.optype != null && !this.optype.equals(optype)) {
            changed = true;
        }
        this.prevoptype = this.optype;
        this.optype = optype;
        if (this.index != null && !this.index.equals(index)) {
            changed = true;
        }
        this.previndex = this.index;
        this.index = index;
        if (this.type != null && !this.type.equals(type)) {
            changed = true;
        }
        this.prevtype = this.type;
        this.type = type;
        if (this.id != null && !this.id.equals(id)) {
            changed = true;
        }
        this.previd = this.id;
        this.id = id;
        if (this.parent != null && !this.parent.equals(parent)) {
        	changed = true;
        }
        this.prevparent = this.parent;
        this.parent = parent;
        if (version == null) {
        	this.ver = staticVersion;
        	this.prevver = staticVersion;
        } else {
	        if ((this.ver != null) && (this.ver != version)) {
	        	changed = true;
	        }
	        this.prevver = this.ver;
	        this.ver = version;
        }
        if (changed) {
            flush(prevoptype, previndex, prevtype, previd, prevparent, prevver);
        }
        for (int i = 0; i < keys.size(); i++) {
        	String key = null;
        	Object value = null;
        	try {
        		key = keys.get(i);
        		value = values.get(i);
        		merge(map, key, value);
        	} catch (RuntimeException re) {
        		if (logger != null) {
	        		logger.error("unable to merge key #{} - {}:{}", i, key, value);
	        		logger.error("map: {}", map.toString());
        		}
        		throw re;
        	}
        }
    }

    public XContentBuilder getBuilder() {
        return builder;
    }

    /**
     * Flush and invoke appropriate operation type
     *
     * @param optype the operation type
     * @param index the index 
     * @param type the type
     * @param id the id
     * @throws IOException
     */
    public void flush(String optype, String index, String type, String id, String parent, long version) throws IOException {
        if (!map.isEmpty()) {
            if (index != null) {
                digest.update(index.getBytes(DIGEST_ENCODING));
            }
            if (type != null) {
                digest.update(type.getBytes(DIGEST_ENCODING));
            }
            if (id != null) {
                digest.update(id.getBytes(DIGEST_ENCODING));
            }
            build(map);
            if (listener != null) {
                if (OP_CREATE.equals(optype)) {
                    listener.create(index, type, id, parent, version, builder);
                } else if (OP_INDEX.equals(optype)) {
                    listener.index(index, type, id, parent, version, builder);
                } else if (OP_DELETE.equals(optype)) {
                    listener.delete(index, type, id);
                }
            }
            builder.close();
            builder = jsonBuilder();
            map = new HashMap<String, Object>();
        }
    }

    /**
     * Return a message digest (in base64-encoded form)
     *
     * @return the message digest
     */
    public String getDigest() {
        return digestString;
    }

    /**
     * Close the merger
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (!closed) {
            flush(optype, index, type, id, parent, ver);
            this.digestString = Base64.encodeBytes(digest.digest());
            closed = true;
        }
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
     * @param map the map for the JSON object
     * @param key the key
     * @param value the value
     */
    @SuppressWarnings("unchecked")
	protected void merge(Map<String, Object> map, String key, Object value) {
        int i = key.indexOf(delimiter);
        if (i <= 0) {
            map.put(key, new ValueSet(map.get(key), value));
        } else {
            String p = key.substring(0, i);
            String q = key.substring(i + 1);
            if (map.containsKey(p)) {
                Object o = map.get(p);
                if (o instanceof Map) {
                    merge((Map<String, Object>) o, q, value);
                } else {
                    throw new IllegalArgumentException("illegal prefix: " + p);
                }
            } else {
                Map<String, Object> m = new HashMap<String, Object>();
                map.put(p, m);
                merge(m, q, value);
            }
        }
    }

    /**
     * Build JSON with the help of XContentBuilder.
     *
     * @param map the map holding the JSON object
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
	private void build(Map<String, Object> map) throws IOException {
        builder.startObject();
        for (String k : map.keySet()) {
            builder.field(k);
            digest.update(k.getBytes(DIGEST_ENCODING));
            Object o = map.get(k);
            if (o instanceof ValueSet) {
                ((ValueSet) o).build(builder, digest, DIGEST_ENCODING);
            } else if (o instanceof Map) {
                build((Map<String, Object>) o);
            }
        }
        builder.endObject();
    }
}
