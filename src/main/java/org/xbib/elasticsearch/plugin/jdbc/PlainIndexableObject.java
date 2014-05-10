package org.xbib.elasticsearch.plugin.jdbc;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * A basic indexable object. The indexable object can store meta data and core data.
 * The indexable object can be iterated and passed to an XContentBuilder. The individual values
 * are formatted for JSON, which should be the correct format
 *
 */
public class PlainIndexableObject implements IndexableObject, ToXContent {

    private Map<String, String> meta;

    private Map<String, Object> core;

    public PlainIndexableObject() {
        this.meta = new TreeMap<String, String>();
        this.core = new TreeMap<String, Object>();

    }

    public IndexableObject optype(String optype) {
        meta.put(ControlKeys._optype.name(), optype);
        return this;
    }

    public String optype() {
        return meta.get(ControlKeys._optype.name());
    }

    public IndexableObject index(String index) {
        meta.put(ControlKeys._index.name(), index);
        return this;
    }

    public String index() {
        return meta.get(ControlKeys._index.name());
    }

    public IndexableObject type(String type) {
        meta.put(ControlKeys._type.name(), type);
        return this;
    }

    public String type() {
        return meta.get(ControlKeys._type.name());
    }

    public IndexableObject id(String id) {
        meta.put(ControlKeys._id.name(), id);
        return this;
    }

    public String id() {
        return meta.get(ControlKeys._id.name());
    }

    public IndexableObject meta(String key, String value) {
        meta.put(key, value);
        return this;
    }

    public String meta(String key) {
        return meta.get(key);
    }

    public IndexableObject source(Map<String, Object> source) {
        this.core = source;
        return this;
    }

    public Map<String, Object> source() {
        return core;
    }

    /**
     * Build a string that can be used for indexing.
     *
     * @throws java.io.IOException when build gave an error
     */
    public String build() throws IOException {
        XContentBuilder builder = jsonBuilder();
        toXContent(builder, ToXContent.EMPTY_PARAMS);
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
     * @param map     the map
     * @return the XContent builder
     * @throws java.io.IOException when method gave an error
     */
    @SuppressWarnings({"unchecked"})
    protected XContentBuilder toXContent(XContentBuilder builder, Params params, Map<String, Object> map) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Object> k : map.entrySet()) {
            builder.field(k.getKey());
            Object o = k.getValue();
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

    @SuppressWarnings({"unchecked"})
    protected XContentBuilder toXContent(XContentBuilder builder, Params params, List list) throws IOException {
        builder.startArray();
        for (Object o : list) {
            if (o instanceof Values) {
                Values v = (Values) o;
                v.toXContent(builder, ToXContent.EMPTY_PARAMS);
            } else if (o instanceof Map) {
                toXContent(builder, params, (Map<String, Object>) o);
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
        if (o == null) {
            return false;
        }
        if (!(o instanceof IndexableObject)) {
            return false;
        }
        IndexableObject c = (IndexableObject) o;
        return equal(optype(), c.optype()) &&
                equal(index(), c.index()) &&
                equal(type(), c.type()) &&
                equal(id(), c.id());
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 37 * hash + (optype() != null ? optype().hashCode() : 0);
        hash = 37 * hash + (index() != null ? index().hashCode() : 0);
        hash = 37 * hash + (type() != null ? type().hashCode() : 0);
        hash = 37 * hash + (id() != null ? id().hashCode() : 0);
        return hash;
    }

    private boolean equal(Object a, Object b) {
        if (a == null && b == null) {
            return true;
        }
        if (a != null && b != null) {
            if (a.equals(b)) {
                return true;
            }
        }
        return false;
    }
}
