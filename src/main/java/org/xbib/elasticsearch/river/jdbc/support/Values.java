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
package org.xbib.elasticsearch.river.jdbc.support;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A ValueSet represents one or many values. Each value is represented at most
 * once. New values are appended. The ValueSet is the base for building arrays
 * in JSON.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class Values<O extends Object> {

    private O[] value = (O[]) new Object[0];

    /**
     * Create a new valueset from an existing valueset adding a value v. If
     * valueset is null, ignore valueset.
     *
     * @param values the valueset
     * @param value the value
     * @param sequence True if value is to be splitted by commas to multiple values
     */
    public Values(Object values, O value, boolean sequence) {
        if (values instanceof Values) {
            O[] vals = (O[]) ((Values) values).getValues();
            if (vals != null) {
                this.value = vals;
            }
        }
        O[] newValues = sequence && value != null ?
            (O[]) value.toString().split(",") :
            (O[]) new Object[] { value };
        for (O v : newValues) {
            addValue(v);
        }
    }

    /**
     * Adds the given value
     * @param v
     */
    public void addValue(O v) {
        int l = value.length;
        for (O aValue : value) {
            if (v != null && v.equals(aValue)) {
                //value already found, do nothing
                return;
            }
        }
        if (l == 0) {
            value = (O[]) new Object[] { v };
        } else {
            //never add a null-value to an existing list of values
            if (v != null) {
                if (l == 1 && value[0] == null) {
                    //if there's one existing value and it's null, replace it with the new one
                    value = (O[]) new Object[] { v };
                } else {
                    //otherwise copy the existing value(s) and add the new one
                    O[] oldValues = value;
                    value = (O[]) new Object[l + 1];
                    System.arraycopy(oldValues, 0, value, 0, l);
                    value[l] = v;
                }
            }
        }
    }

    /**
     * Get the values.
     *
     * @return the values
     */
    public O[] getValues() {
        return value;
    }

    public boolean isNull() {
        return value.length == 0 || (value.length == 1 && value[0] == null);
    }

    /**
     * Build ValueSet as XContent
     *
     * @param builder the XContentBuilder
     * @throws IOException
     */
    public void build(XContentBuilder builder) throws IOException {
        if (value.length > 1) {
            builder.startArray();
        }
        for (O aValue : value) {
            builder.value(aValue);
        }
        if (value.length > 1) {
            builder.endArray();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (value.length > 1) {
            sb.append('[');
        }
        for (int i = 0; i < value.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(format(value[i]));
        }
        if (value.length > 1) {
            sb.append(']');
        }
        return sb.toString();
    }

    /**
     * Format a value for JSON.
     *
     * @param o the value
     * @return the formtted value
     */
    private String format(Object o) {
        if (o == null) {
            return "null";
        }
        if (o instanceof Integer) {
            return Integer.toString((Integer) o);
        }
        if (o instanceof Long) {
            return Long.toString((Long) o);
        }
        if (o instanceof Float) {
            return Float.toString((Float) o);
        }
        if (o instanceof Double) {
            return Double.toString((Double) o);
        }
        if (o instanceof Boolean) {
            return Boolean.toString((Boolean) o);
        }
        // stringify
        String t = o.toString();
        t = t.replaceAll("\"", "\\\"");
        return '"' + t + '"';
    }
}
