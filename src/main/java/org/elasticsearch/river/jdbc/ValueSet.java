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

import java.io.IOException;
import java.security.MessageDigest;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * A ValueSet represents one or many values.
 * Each value is represented at most once. New values are appended.
 * The ValueSet is the base for building arrays in JSON.
 */
public class ValueSet {

    /**
     * the values
     */
    private Object[] value = new Object[0];

    /**
     * Create a new valueset from an existing valueset adding a value v. If valueset is
     * null, ignore valueset.
     *
     * @param valueset the valueset
     * @param v the value
     */
    public ValueSet(Object valueset, Object v) {
        if (valueset instanceof ValueSet) {
            ValueSet t = (ValueSet) valueset;
            Object[] values = t.getValues();
            int l = values.length;
            boolean found = false;
            for (int i = 0; i < l; i++) {
                found = v != null && v.equals(values[i]);
                if (found) {
                    break;
                }
            }
            if (!found && v != null) {
                value = new Object[l + 1];
                System.arraycopy(values, 0, value, 0, l);
                value[l] = v;
            }
        } else {
            value = new Object[1];
            value[0] = v;
        }
    }

    /**
     * Get the values.
     *
     * @return the values
     */
    public Object[] getValues() {
        return value;
    }

    /**
     * Build ValueSet as XContent
     *
     * @param builder the XContentBuilder
     * @throws IOException
     */
    public void build(XContentBuilder builder, MessageDigest digest, String encoding) throws IOException {
    	if (value == null || value.length < 1) {
    		builder.value((String) null);
    	} else {
	        if (value.length > 1) {
	            builder.startArray();
	        }
	        for (int i = 0; i < value.length; i++) {
	            builder.value(value[i]);
	            if (value[i] != null) {
	                digest.update(value[i].toString().getBytes(encoding));
	            }
	        }
	        if (value.length > 1) {
	            builder.endArray();
	        }
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
     * Format a value.
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
        String t = (String) o;
        t = t.replaceAll("\"", "\\\"");
        return "\"" + t + "\"";
    }
}
