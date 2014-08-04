package org.xbib.elasticsearch.action.river.jdbc.state;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.newHashMap;

/**
 * A river state represents a point in time when a river has a defined behavior with a set of parameters
 */
public class RiverState implements Streamable, Comparable<RiverState> {

    /**
     * The name of the river
     */
    private String name;

    /**
     * The type of the river
     */
    private String type;

    private Settings settings = ImmutableSettings.EMPTY;

    /**
     * A custom map for more information about the river
     */
    private Map<String, Object> map = newHashMap();

    public RiverState() {
    }

    public RiverState(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public RiverState setSettings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public Settings getSettings() {
        return settings;
    }

    public RiverState setMap(Map<String,Object> map) {
        this.map = map;
        return this;
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public RiverState setStarted(Date started) {
        map.put("started", started);
        return this;
    }

    public Date getStarted() {
        return (Date) map.get("started");
    }

    public RiverState setCounter(Long counter) {
        map.put("counter", counter);
        return this;
    }

    public Long getCounter() {
        return map.containsKey("counter") ? (Long) map.get("counter") : 0L;
    }

    public RiverState setTimestamp(Date timestamp) {
        map.put("timestamp", timestamp);
        return this;
    }

    public Date getTimestamp() {
        return (Date) map.get("timestamp");
    }

    public RiverState setEnabled(Boolean enabled) {
        map.put("enabled", enabled);
        return this;
    }

    public Boolean isEnabled() {
        return (Boolean) map.get("enabled");
    }

    public RiverState setActive(Boolean active) {
        map.put("active", active);
        return this;
    }

    public Boolean isActive() {
        return map.containsKey("active") ? (Boolean) map.get("active") : false;
    }


    public RiverState setCustom(Map<String, Object> custom) {
        this.map.put("custom", custom);
        return this;
    }

    public Map<String, Object> getCustom() {
        return (Map<String, Object>) this.map.get("custom");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readOptionalString();
        this.type = in.readOptionalString();
        ImmutableSettings.readSettingsFromStream(in);
        map = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeOptionalString(type);
        ImmutableSettings.writeSettingsToStream(settings, out);
        out.writeMap(map);
    }

    @Override
    public int compareTo(RiverState o) {
        return toString().compareTo(o.toString());
    }

    @Override
    public String toString() {
        return "[name="+name+",type="+type+",settings="+settings.getAsMap()+",map="+map+"]";
    }
}
