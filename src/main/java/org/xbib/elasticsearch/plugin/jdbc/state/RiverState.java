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

    private final static DateTime EMPTY_DATETIME = new DateTime(0L);

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
    private DateTime begin;

    /*
     * The time when the last river activity ended
     */
    private DateTime end;

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
        if (begin != null) {
            this.begin = begin;
        }
        if (end != null) {
            this.end = end;
        }
        return this;
    }

    /**
     * @return the begin of the last river activity
     */
    public DateTime getLastActiveBegin() {
        return begin != null ? begin : EMPTY_DATETIME;
    }

    /**
     * @return the end of the last river activity
     */
    public DateTime getLastActiveEnd() {
        return end != null ? end : EMPTY_DATETIME;
    }

    /**
     * Was the river active at a certain time? Only the last activity can be checked.
     *
     * @param instant the time to check
     * @return true if river was active, false if not
     */
    public boolean wasActiveAt(DateTime instant) {
        return instant != null
                && begin != null && begin.getMillis() != 0L && begin.isBefore(instant)
                && (end == null || end.getMillis() == 0L || end.isAfter(instant));
    }

    public boolean wasInactiveAt(DateTime instant) {
        return !wasActiveAt(instant);
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

    public boolean isAborted() {
        return map.containsKey("aborted") ? (Boolean) map.get("aborted") : false;
    }

    public boolean isSuspended() {
        return map.containsKey("suspended") ? (Boolean) map.get("suspended") : false;
    }

    public RiverState fromXContent(XContentParser parser) throws IOException {
        DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC);
        Long startTimestamp = 0L;
        Long begin = 0L;
        Long end = 0L;
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
                                dateTimeFormatter.parseMillis(parser.text()) : 0L;
                        break;
                    case "last_active_end":
                        end = parser.text() != null && !"null".equals(parser.text()) ?
                                dateTimeFormatter.parseMillis(parser.text()) : 0L;
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
                .setLastActive(new DateTime(begin), new DateTime(end))
                .setMap(map);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .field("name", name)
                .field("type", type)
                .field("started", getStarted())
                .field("last_active_begin", getLastActiveBegin())
                .field("last_active_end", getLastActiveEnd())
                .field("map", map);
        builder.endObject();
        return builder;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readOptionalString();
        this.type = in.readOptionalString();
        this.started = new DateTime(in.readLong());
        this.begin = new DateTime(in.readLong());
        this.end = new DateTime(in.readLong());
        map = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeOptionalString(type);
        out.writeLong(started != null ? started.getMillis() : 0L);
        out.writeLong(begin != null ? begin.getMillis() : 0L);
        out.writeLong(end != null ? end.getMillis() : 0L);
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
