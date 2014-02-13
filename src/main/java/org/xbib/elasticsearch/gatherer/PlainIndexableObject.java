
package org.xbib.elasticsearch.gatherer;

import org.elasticsearch.common.base.Objects;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * A basic indexable object. The indexable object can store meta data and core data.
 */
public class PlainIndexableObject implements IndexableObject, ToXContent {

    private Map<String, String> meta;

    private Map<String, Object> core;

    public PlainIndexableObject() {
        this.meta = newHashMap();
        this.core = newHashMap();
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

    public Map source() {
        return core;
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
        return Objects.equal(optype(), c.optype()) &&
                Objects.equal(index(), c.index()) &&
                Objects.equal(type(), c.type()) &&
                id() != null && id().equals(c.id());
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

    @Override
    public int compareTo(IndexableObject o) {
        int i = 0;
        if (o == null) {
            return -1;
        }
        if (optype() != null) {
            i = optype().compareTo(o.optype());
        }
        if (i != 0) {
            return i;
        }
        if (index() != null) {
            i = index().compareTo(o.index());
        }
        if (i != 0) {
            return i;
        }
        if (type() != null) {
            i = type().compareTo(o.type());
        }
        if (i != 0) {
            return i;
        }
        if (id() != null) {
            i = id().compareTo(o.id());
        }
        return i;
    }

    /**
     * Build a string that can be used for indexing.
     *
     * @throws java.io.IOException
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
     * @throws java.io.IOException
     */
    protected XContentBuilder toXContent(XContentBuilder builder, Params params, Map<String, Object> map) throws IOException {
        builder.startObject();
        for (String k : map.keySet()) {
            builder.field(k);
            Object o = map.get(k);
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
                } catch (Exception e) {
                    throw new IOException("unknown object class:" + o.getClass().getName());
                }
            }
        }
        builder.endObject();
        return builder;
    }

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

    public void clear() {
        this.meta = null;
        this.core = null;
    }

    @Override
    public String toString() {
        return "[" + optype() + "/" + index() + "/" + type() + "/" + id() + "]->" + core;
    }

}
