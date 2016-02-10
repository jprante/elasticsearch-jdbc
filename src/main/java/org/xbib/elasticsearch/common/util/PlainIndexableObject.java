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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * A plain indexable object. The indexable object can store meta data and core data.
 * The indexable object can be iterated and passed to an XContentBuilder. The individual values
 * are formatted for JSON, which should be the correct format
 */
public class PlainIndexableObject implements IndexableObject, ToXContent, Comparable<IndexableObject> {

    private final Map<String, String> meta;

    private Map<String, Object> core;

    private final Params params;

    public PlainIndexableObject() {
        this(ToXContent.EMPTY_PARAMS);
    }

    public PlainIndexableObject(Params params) {
        this.meta = new LinkedHashMap<String, String>();
        this.core = new LinkedHashMap<String, Object>();
        this.params = params;
    }

    @Override
    public IndexableObject optype(String optype) {
        meta.put(ControlKeys._optype.name(), optype);
        return this;
    }

    @Override
    public String optype() {
        return meta.get(ControlKeys._optype.name());
    }

    @Override
    public IndexableObject index(String index) {
        meta.put(ControlKeys._index.name(), index);
        return this;
    }

    @Override
    public String index() {
        return meta.get(ControlKeys._index.name());
    }

    @Override
    public IndexableObject type(String type) {
        meta.put(ControlKeys._type.name(), type);
        return this;
    }

    @Override
    public String type() {
        return meta.get(ControlKeys._type.name());
    }

    @Override
    public IndexableObject id(String id) {
        meta.put(ControlKeys._id.name(), id);
        return this;
    }

    @Override
    public String id() {
        return meta.get(ControlKeys._id.name());
    }

    @Override
    public IndexableObject meta(String key, String value) {
        meta.put(key, value);
        return this;
    }

    @Override
    public String meta(String key) {
        return meta.get(key);
    }

    @Override
    public IndexableObject source(Map<String, Object> source) {
        this.core = source;
        return this;
    }

    @Override
    public Map<String, Object> source() {
        return core;
    }

    /**
     * Build a string that can be used for indexing.
     *
     * @throws java.io.IOException when build gave an error
     */
    @Override
    public String build() throws IOException {
        XContentBuilder builder = jsonBuilder();
        toXContent(builder, params);
        return builder.string();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        toXContent(builder, params, core);
        return builder;
    }

    /**
     * Recursive method to build XContent from a key/value map of Values
     *
     * @param builder the builder
     * @param params the params
     * @param map     the map
     * @return the XContent builder
     * @throws java.io.IOException when method gave an error
     */
    @SuppressWarnings({"unchecked"})
    protected XContentBuilder toXContent(XContentBuilder builder, Params params, Map<String, Object> map) throws IOException {
        builder.startObject();
        if (checkCollapsedMapLength(map)) {
            builder.endObject();
            return builder;
        }
        for (Map.Entry<String, Object> k : map.entrySet()) {
            Object o = k.getValue();
            if (params.paramAsBoolean("ignore_null", false) && (o == null || (o instanceof Values) && ((Values) o).isNull())) {
                continue;
            }
            builder.field(k.getKey());
            if (o instanceof Values) {
                Values v = (Values) o;
                v.toXContent(builder, params);
            } else if (o instanceof Map) {
                toXContent(builder, params, (Map<String, Object>) o);
            } else if (o instanceof List) {
                toXContent(builder, params, (List) o);
            } else {
                try {
                    builder.value(o);
                } catch (Throwable e) {
                    throw new IOException("unknown object class for value:" + o.getClass().getName() + " " + o);
                }
            }
        }
        builder.endObject();
        return builder;
    }

    /**
     * Check if the map is empty, after optional null value removal.
     *
     * @param map the map to check
     * @return true if map is empty, false if not
     */
    protected boolean checkCollapsedMapLength(Map<String, Object> map) {
        int exists = 0;
        for (Map.Entry<String, Object> k : map.entrySet()) {
            Object o = k.getValue();
            if (params.paramAsBoolean("ignore_null", false)  && (o == null || (o instanceof Values) && ((Values) o).isNull())) {
                continue;
            }
            exists++;
        }
        return exists == 0;
    }

    @SuppressWarnings({"unchecked"})
    protected XContentBuilder toXContent(XContentBuilder builder, Params params, List list) throws IOException {
        builder.startArray();
        for (Object o : list) {
            if (o instanceof Values) {
                Values v = (Values) o;
                v.toXContent(builder, ToXContent.EMPTY_PARAMS);
            } else if (o instanceof Map) {
                if (!checkCollapsedMapLength((Map<String, Object>) o)) {
                    toXContent(builder, params, (Map<String, Object>) o);
                }
            } else if (o instanceof List) {
                toXContent(builder, params, (List) o);
            } else {
                try {
                    builder.value(o);
                } catch (Exception e) {
                    throw new IOException("unknown object class:" + o.getClass().getName());
                }
            }
        }
        builder.endArray();
        return builder;
    }

    @Override
    public boolean isEmpty() {
        return optype() == null && index() == null && type() == null && id() == null && core.isEmpty();
    }

    @Override
    public String toString() {
        return "[" + optype() + "/" + index() + "/" + type() + "/" + id() + "]->" + core;
    }

    @Override
    public int compareTo(IndexableObject o) {
        if (o == null) {
            return -1;
        }
        String s1 = optype() + '/' + index() + '/' + type() + '/' + id();
        String s2 = o.optype() + '/' + o.index() + '/' + o.type() + '/' + o.id();
        return s1.compareTo(s2);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof  IndexableObject)) {
            return false;
        }
        IndexableObject indexableObject = (IndexableObject)o;
        return new EqualsBuilder()
                .append(optype(), indexableObject.optype())
                .append(index(), indexableObject.index())
                .append(type(), indexableObject.type())
                .append(id(), indexableObject.id())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(optype())
                .append(index())
                .append(type())
                .append(id())
                .toHashCode();
    }

}
