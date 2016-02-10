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

/**
 * This class represents one or many values. Each value is represented at most
 * once. New values are appended.
 */
public class Values<O extends Object> implements ToXContent {

    /**
     * The values are implemented as an object array
     */
    @SuppressWarnings({"unchecked"})
    private O[] values = (O[]) new Object[0];

    /**
     * Create new Values from an existing values by appending a value.
     *
     * @param old      existing values or null if values should be created
     * @param value    a new value
     * @param sequence true if value should be splitted by commas to multiple values
     */
    @SuppressWarnings({"unchecked"})
    public Values(Object old, O value, boolean sequence) {
        if (old instanceof Values) {
            O[] vals = (O[]) ((Values) old).getValues();
            if (vals != null) {
                this.values = vals;
            }
        }
        O[] newValues;
        if (sequence && value != null) {
            newValues = (O[]) value.toString().split(",");
        } else if (value instanceof Object[]) {
            newValues = (O[]) value;
        } else {
            newValues = (O[]) new Object[]{value};
        }
        for (O v : newValues) {
            addValue(v);
        }
    }

    /**
     * Append value to existing values.
     *
     * @param v the value to add
     */
    @SuppressWarnings({"unchecked"})
    public void addValue(O v) {
        int l = this.values.length;
        for (O aValue : this.values) {
            if (v != null && v.equals(aValue)) {
                //value already found, do nothing
                return;
            }
        }
        if (l == 0) {
            this.values = (O[]) new Object[]{v};
        } else {
            // never add a null value to an existing list of values
            if (v != null) {
                if (l == 1 && this.values[0] == null) {
                    // if there's one existing value and it's null, replace it with the new one
                    this.values = (O[]) new Object[]{v};
                } else {
                    // otherwise copy the existing value(s) and add the new one
                    O[] oldValues = this.values;
                    this.values = (O[]) new Object[l + 1];
                    System.arraycopy(oldValues, 0, this.values, 0, l);
                    this.values[l] = v;
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
        return this.values;
    }

    public boolean isNull() {
        return this.values.length == 0 || (this.values.length == 1 && this.values[0] == null);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (this.values.length > 1) {
            sb.append('[');
        }
        for (int i = 0; i < this.values.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(format(this.values[i]));
        }
        if (this.values.length > 1) {
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
        if (o instanceof Boolean) {
            return Boolean.toString((Boolean) o);
        }
        if (o instanceof Float) {
            // suppress scientific notation
            return String.format("%.12f", (Float) o);
        }
        if (o instanceof Double) {
            // suppress scientific notation
            return String.format("%.12f", (Double) o);
        }
        // stringify
        String t = o.toString();
        t = t.replaceAll("\"", "\\\"");
        return '"' + t + '"';
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (this.values.length == 0) {
            builder.nullValue();
            return builder;
        }
        if (this.values.length > 1 || params.paramAsBoolean("force_array", false)) {
            builder.startArray();
        }
        for (O aValue : this.values) {
            builder.value(aValue);
        }
        if (this.values.length > 1 || params.paramAsBoolean("force_array", false)) {
            builder.endArray();
        }
        return builder;
    }
}
