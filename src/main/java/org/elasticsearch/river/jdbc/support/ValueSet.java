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
package org.elasticsearch.river.jdbc.support;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A ValueSet represents one or many values. Each value is represented at most
 * once. New values are appended. The ValueSet is the base for building arrays
 * in JSON.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class ValueSet<O extends Object> {

    private O[] value = (O[]) new Object[0];

    /**
     * Create a new valueset from an existing valueset adding a value v. If
     * valueset is null, ignore valueset.
     *
     * @param valueset the valueset
     * @param v        the value
     * @param expandValue True to expand the value to multiple values by splitting on commas.
     */
    public ValueSet(Object valueset, O v, boolean expandValue) {
        if (valueset instanceof ValueSet) {
            O[] values = (O[]) ((ValueSet) valueset).getValues();
            if (values != null) {
                value = values;
            }
        }

        O[] newValues;
        if (expandValue && v != null) {
            newValues = (O[]) v.toString().split(",");
        } else {
            newValues = (O[]) new Object[] { v };
        }

        for (O value : newValues) {
            addValue(value);
        }
    }

    /**
     * Adds the given value
     * @param v
     */
    public void addValue(O v) {
        int l = value.length;

        for (int i = 0; i < l; i++) {
            if (v != null && v.equals(value[i])) {
                //value already found, do nothing
                return;
            }
        }

        if (l > 0) {
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
        } else {
            value = (O[]) new Object[] { v };
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
        for (int i = 0; i < value.length; i++) {
            builder.value(value[i]);
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
