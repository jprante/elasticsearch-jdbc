package org.xbib.importer.elasticsearch;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.xbib.importer.Document;
import org.xbib.importer.util.EqualsBuilder;
import org.xbib.importer.util.HashCodeBuilder;
import org.xbib.importer.util.Values;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The ElasticsearchDocument can store meta data and payload data.
 * It can be iterated and passed to an XContentBuilder.
 */
public class ElasticsearchDocument implements Document, ToXContent, Comparable<Document> {

    private static final Pattern p = Pattern.compile("^(.*)\\[(.*?)\\]$");

    /**
     * The delimiter between the key names in the path, used for string split.
     */
    private static final char delimiter = '.';

    private final Params params;

    private final Map<String, String> meta;

    private Map<String, Object> map;

    public ElasticsearchDocument() {
        this(ToXContent.EMPTY_PARAMS);
    }

    public ElasticsearchDocument(Params params) {
        this.params = params;
        this.meta = new LinkedHashMap<>();
        this.map = new LinkedHashMap<>();
    }

    @Override
    public void setOperationType(String optype) {
        meta.put("_optype", optype);
    }

    @Override
    public String getOperationType() {
        return meta.get("_optype");
    }

    @Override
    public void setIndex(String index) {
        meta.put("_index", index);
    }

    @Override
    public String getIndex() {
        return meta.get("_index");
    }

    @Override
    public void setType(String type) {
        meta.put("_type", type);
    }

    @Override
    public String getType() {
        return meta.get("_type");
    }

    @Override
    public void setId(String id) {
        meta.put("_id", id);
    }

    @Override
    public String getId() {
        return meta.get("_id");
    }

    @Override
    public void setMeta(String key, String value) {
        meta.put(key, value);
    }

    @Override
    public String getMeta(String key) {
        return meta.get(key);
    }

    @Override
    public boolean hasMeta(String key) {
        return meta.containsKey(key);
    }

    @Override
    public void setSource(Map<String, Object> source) {
        this.map = source;
    }

    @Override
    public Map<String, Object> getSource() {
        return map;
    }

    /**
     * Build a string that can be used for indexing.
     *
     * @throws IOException when build gave an error
     */
    @Override
    public String build() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        toXContent(builder, params);
        return builder.string();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        toXContent(builder, params, map);
        return builder;
    }

    public void merge(String key, Object value) {
        setSource(merge(getSource(), key, value));
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
     * @param map map
     * @param k key
     * @param value value
     * @return map
     */
    @SuppressWarnings({"unchecked"})
    private Map<String, Object> merge(Map<String, Object> map, Object k, Object value) {
        String key = k.toString();
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

    /**
     * Recursive method to build XContent from a key/value map of Values.
     *
     * @param builder the builder
     * @param params the params
     * @param map     the map
     * @return the XContent builder
     * @throws IOException when method gave an error
     */
    @SuppressWarnings({"unchecked"})
    private XContentBuilder toXContent(XContentBuilder builder, Params params, Map<String, Object> map) throws IOException {
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
    private boolean checkCollapsedMapLength(Map<String, Object> map) {
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
    public String toString() {
        return "[" + getOperationType() + "/" + getIndex() + "/" + getType() + "/" + getId() + "]->" + map;
    }

    @Override
    public int compareTo(Document o) {
        if (o == null) {
            return -1;
        }
        String s1 = getOperationType() + '/' + getIndex() + '/' + getType() + '/' + getId();
        String s2 = o.getOperationType() + '/' + o.getIndex() + '/' + o.getType() + '/' + o.getId();
        return s1.compareTo(s2);
    }

    @Override
    public boolean equals(Object o) {
        if ((!(o instanceof Document))) {
            return false;
        } else {
            Document document = (Document) o;
            return new EqualsBuilder()
                    .append(getOperationType(), document.getOperationType())
                    .append(getIndex(), document.getIndex())
                    .append(getType(), document.getType())
                    .append(getId(), document.getId())
                    .isEquals();
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(getOperationType())
                .append(getIndex())
                .append(getType())
                .append(getId())
                .toHashCode();
    }
}
