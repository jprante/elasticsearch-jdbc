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
    private Map<String, Object> custom = newHashMap();

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

    public RiverState setStarted(Date started) {
        custom.put("started", started);
        return this;
    }

    public Date getStarted() {
        return (Date) custom.get("started");
    }

    public RiverState setCounter(Long counter) {
        custom.put("counter", counter);
        return this;
    }

    public Long getCounter() {
        return custom.containsKey("counter") ? (Long) custom.get("counter") : 0L;
    }

    public RiverState setTimestamp(Date timestamp) {
        custom.put("timestamp", timestamp);
        return this;
    }

    public Date getTimestamp() {
        return (Date) custom.get("timestamp");
    }

    public RiverState setEnabled(Boolean enabled) {
        custom.put("enabled", enabled);
        return this;
    }

    public Boolean isEnabled() {
        return (Boolean) custom.get("enabled");
    }

    public RiverState setActive(Boolean active) {
        custom.put("active", active);
        return this;
    }

    public Boolean isActive() {
        return custom.containsKey("active") ? (Boolean) custom.get("active") : false;
    }

    public RiverState setSettings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public Settings getSettings() {
        return settings;
    }

    public RiverState setCustom(Map<String, Object> custom) {
        custom.put("custom", custom);
        return this;
    }

    public Map<String, Object> getCustom() {
        return (Map<String, Object>) custom.get("custom");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readOptionalString();
        this.type = in.readOptionalString();
        ImmutableSettings.readSettingsFromStream(in);
        custom = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeOptionalString(type);
        ImmutableSettings.writeSettingsToStream(settings, out);
        out.writeMap(custom);
    }

    @Override
    public int compareTo(RiverState o) {
        return toString().compareTo(o.toString());
    }

    @Override
    public String toString() {
        return "[name="+name+",type="+type+",settings="+settings.getAsMap()+",custom="+custom+"]";
    }
}
