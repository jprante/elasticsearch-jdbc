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
package org.xbib.elasticsearch.plugin.jdbc.state;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NULL;

/**
 * A river state represents a point in time when a river has a defined behavior with a set of parameters
 */
public class RiverState implements Streamable, ToXContent, Comparable<RiverState> {

    /**
     * The name of the river instance
     */
    private String name;

    /**
     * The type of the river instance
     */
    private String type;

    /**
     * The time the river instance was started
     */
    private DateTime started;

    /*
     * The time of the last river activity
     */
    private DateTime lastActiveBegin;

    /*
     * The begin time of the current river activity
     */
    private DateTime currentActiveBegin;

    /*
     * The time when the last river activity ended
     */
    private DateTime lastActiveEnd;

    /**
     * A custom map for more information about the river
     */
    private Map<String, Object> map = newHashMap();

    public RiverState() {
    }

    public RiverState setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public RiverState setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return type;
    }

    public RiverState setMap(Map<String, Object> map) {
        this.map = map;
        return this;
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public RiverState setStarted(DateTime started) {
        this.started = started;
        return this;
    }

    public DateTime getStarted() {
        return started;
    }

    /**
     * Set the last river activity. Null means, time is unknown
     *
     * @param begin when the last river activity began
     * @param end   when the last river activity ended
     * @return this state
     */
    public RiverState setLastActive(DateTime begin, DateTime end) {
        this.lastActiveBegin = begin;
        this.lastActiveEnd = end;
        return this;
    }

    /**
     * Set the current river begin DateTime. It will be used to
     * update lastActiveBegin after the run being done
     *
     * @param begin when the current river activity began
     * @return this state
     */
    public RiverState setCurrentActive(DateTime begin) {
        this.currentActiveBegin = begin;
        return this;
    }

    /**
     * @return the begin of the last river activity
     */
    public DateTime getLastActiveBegin() {
        return lastActiveBegin;
    }

    /**
     * @return the begin of the current river activity
     */
    public DateTime getCurrentActiveBegin() {
        return currentActiveBegin;
    }

    /**
     * @return the end of the last river activity
     */
    public DateTime getLastActiveEnd() {
        return lastActiveEnd;
    }

    public RiverState setCounter(Integer counter) {
        map.put("counter", counter);
        return this;
    }

    public Integer getCounter() {
        return map.containsKey("counter") ? (Integer) map.get("counter") : 0;
    }

    public RiverState setCustom(Map<String, Object> custom) {
        this.map.put("custom", custom);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getCustom() {
        return (Map<String, Object>) this.map.get("custom");
    }

    public boolean isSuspended() {
        return map.containsKey("suspended") ? (Boolean) map.get("suspended") : false;
    }

    public RiverState setLastStartDate(long lastStartDate) {
        this.map.put("lastStartDate", lastStartDate);
        return this;
    }

    public long getLastStartDate() {
        return (long) this.map.get("lastStartDate");
    }

    public RiverState setLastEndDate(long lastEndDate) {
        this.map.put("lastEndDate", lastEndDate);
        return this;
    }

    public long getLastEndDate() {
        return (long) this.map.get("lastEndDate");
    }

    public RiverState setLastExecutionStartDate(long lastExecutionStartDate) {
        this.map.put("lastExecutionStartDate", lastExecutionStartDate);
        return this;
    }

    public long getLastExecutionStartDate() {
        return (long) this.map.get("lastExecutionStartDate");
    }

    public RiverState setLastExecutionEndDate(long lastExecutionEndDate) {
        this.map.put("lastExecutionEndDate", lastExecutionEndDate);
        return this;
    }

    public long getLastExecutionEndDate() {
        return (long) this.map.get("lastExecutionEndDate");
    }

    public RiverState fromXContent(XContentParser parser) throws IOException {
        DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC);
        Long startTimestamp = 0L;
        Long begin = null;
        Long end = null;
        String name = null;
        String type = null;
        String currentFieldName = null;
        Map<String, Object> map = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != END_OBJECT) {
            if (token == FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue() || token == VALUE_NULL) {
                switch (currentFieldName) {
                    case "name":
                        name = parser.text();
                        break;
                    case "type":
                        type = parser.text();
                        break;
                    case "started":
                        startTimestamp = parser.text() != null && !"null".equals(parser.text()) ?
                                dateTimeFormatter.parseMillis(parser.text()) : 0L;
                        break;
                    case "last_active_begin":
                        begin = parser.text() != null && !"null".equals(parser.text()) ?
                                dateTimeFormatter.parseMillis(parser.text()) : null;
                        break;
                    case "last_active_end":
                        end = parser.text() != null && !"null".equals(parser.text()) ?
                                dateTimeFormatter.parseMillis(parser.text()) : null;
                        break;
                }
            } else if (token == START_OBJECT) {
                map = parser.map();
            }
        }
        return new RiverState()
                .setName(name)
                .setType(type)
                .setStarted(new DateTime(startTimestamp))
                .setLastActive(begin != null ? new DateTime(begin) : null,
                        end != null ? new DateTime(end) : null)
                .setMap(map);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .field("name", name)
                .field("type", type)
                .field("started", getStarted())
                .field("last_active_begin", getLastActiveBegin())
                .field("current_active_begin", getCurrentActiveBegin())
                .field("last_active_end", getLastActiveEnd())
                .field("map", map);
        builder.endObject();
        return builder;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readOptionalString();
        this.type = in.readOptionalString();
        if (in.readBoolean()) {
            this.started = new DateTime(in.readLong());
        }
        if (in.readBoolean()) {
            this.lastActiveBegin = new DateTime(in.readLong());
        }
        if (in.readBoolean()) {
            this.currentActiveBegin = new DateTime(in.readLong());
        }
        if (in.readBoolean()) {
            this.lastActiveEnd = new DateTime(in.readLong());
        }
        map = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeOptionalString(type);
        if (started != null) {
            out.writeBoolean(true);
            out.writeLong(started.getMillis());
        } else {
            out.writeBoolean(false);
        }
        if (lastActiveBegin != null) {
            out.writeBoolean(true);
            out.writeLong(lastActiveBegin.getMillis());
        } else {
            out.writeBoolean(false);
        }
        if (currentActiveBegin != null) {
            out.writeBoolean(true);
            out.writeLong(currentActiveBegin.getMillis());
        } else {
            out.writeBoolean(false);
        }
        if (lastActiveEnd != null) {
            out.writeBoolean(true);
            out.writeLong(lastActiveEnd.getMillis());
        } else {
            out.writeBoolean(false);
        }
        out.writeMap(map);
    }

    @Override
    public int compareTo(RiverState o) {
        return (getName() + "/" + getType()).compareTo(o.getName() + "/" + o.getType());
    }

    @Override
    public String toString() {
        try {
            return toXContent(jsonBuilder(), EMPTY_PARAMS).string();
        } catch (IOException e) {
            // ignore
        }
        return "";
    }
}
