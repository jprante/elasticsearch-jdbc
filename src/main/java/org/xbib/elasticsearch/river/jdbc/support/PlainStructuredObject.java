package org.xbib.elasticsearch.river.jdbc.support;

import org.elasticsearch.common.base.Objects;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class PlainStructuredObject implements StructuredObject {

    private Map<String, String> meta;

    private Map<String, ? super Object> source;

    public PlainStructuredObject() {
        this.meta = new HashMap();
        this.source = new HashMap();
    }

    public StructuredObject optype(String optype) {
        meta.put(OPTYPE, optype);
        return this;
    }

    public String optype() {
        return meta.get(OPTYPE);
    }

    public StructuredObject index(String index) {
        meta.put(INDEX, index);
        return this;
    }

    public String index() {
        return meta.get(INDEX);
    }

    public StructuredObject type(String type) {
        meta.put(TYPE, type);
        return this;
    }

    public String type() {
        return meta.get(TYPE);
    }

    public StructuredObject id(String id) {
        meta.put(ID, id);
        return this;
    }

    public String id() {
        return meta.get(ID);
    }

    public StructuredObject meta(String key, String value) {
        meta.put(key, value);
        return this;
    }

    public String meta(String key) {
        return meta.get(key);
    }

    public StructuredObject source(Map<String, ? super Object> source) {
        this.source = source;
        return this;
    }

    public Map source() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof StructuredObject)) {
            return false;
        }
        StructuredObject c = (StructuredObject) o;
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
    public int compareTo(StructuredObject o) {
        int i = 0;
        if (o == null) {
            return -1;
        }
        if (optype() != null && o.optype() != null) {
            i = optype().compareTo(o.optype());
        }
        if (i != 0) {
            return i;
        }
        if (index() != null && o.index() != null) {
            i = index().compareTo(o.index());
        }
        if (i != 0) {
            return i;
        }
        if (type() != null && o.type() != null) {
            i = type().compareTo(o.type());
        }
        if (i != 0) {
            return i;
        }
        if (id() != null && o.id() != null) {
            i = id().compareTo(o.id());
        }
        return i;
    }

    /**
     * Build JSON with the help of XContentBuilder.
     *
     * @throws java.io.IOException
     */
    public String build() throws IOException {
        if (index() != null) {
            checksum(index());
        }
        if (type() != null) {
            checksum(type());
        }
        if (id() != null) {
            checksum(id());
        }
        XContentBuilder builder = jsonBuilder();
        build(builder, source);
        return builder.string();
    }

    /**
     * Recursive method to build XContent from a map of ValueSets
     *
     * @param builder the builder
     * @param map     the map
     * @throws IOException
     */
    protected void build(XContentBuilder builder, Map<String, ? super Object> map) throws IOException {
        builder.startObject();
        for (String k : map.keySet()) {
            builder.field(k);
            checksum(k);
            Object o = map.get(k);
            if (o instanceof Values) {
                Values v = (Values) o;
                v.build(builder);
                for (Object vv : v.getValues()) {
                    if (vv != null) {
                        checksum(vv.toString());
                    }
                }
            } else if (o instanceof Map) {
                build(builder, (Map<String, ? super Object>) o);
            } else if (o instanceof List) {
                build(builder, (List) o);
            } else {
                try {
                    builder.value(o);
                } catch (Exception e) {
                    throw new IOException("unknown object class:" + o.getClass().getName());
                }
            }
        }
        builder.endObject();
    }

    protected void build(XContentBuilder builder, List list) throws IOException {
        builder.startArray();
        for (Object o : list) {
            if (o instanceof Values) {
                Values v = (Values) o;
                v.build(builder);
                for (Object vv : v.getValues()) {
                    if (vv != null) {
                        checksum(vv.toString());
                    }
                }
            } else if (o instanceof Map) {
                build(builder, (Map<String, ? super Object>) o);
            } else if (o instanceof List) {
                build(builder, (List) o);
            } else {
                try {
                    builder.value(o);
                } catch (Exception e) {
                    throw new IOException("unknown object class:" + o.getClass().getName());
                }
            }
        }
        builder.endArray();
    }

    public boolean isEmpty() {
        return index() == null && type() == null && id() == null && source.isEmpty();
    }

    public void clear() {
        this.meta = null;
        this.source = null;
    }

    public StructuredObject digest(MessageDigest digest) {
        return this;
    }

    public MessageDigest digest() {
        return null;
    }

    public void checksum(String data) throws IOException {
        // skip checksum
    }

    public String checksum() {
        return null;
    }

    @Override
    public String toString() {
        return optype() + "/" + index() + "/" + type() + "/" + id() + " " + source;
    }
}
