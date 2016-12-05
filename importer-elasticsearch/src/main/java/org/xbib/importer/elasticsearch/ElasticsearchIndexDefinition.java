package org.xbib.importer.elasticsearch;

import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 *
 */
public class ElasticsearchIndexDefinition {

    private String index;
    private String concreteIndexName;
    private String type;
    private String id;
    private Settings settings;
    private Map<String, String> mapping;
    private String timeWindow;
    private boolean mock;
    private boolean ignoreErrors;
    private boolean switchAliases;
    private boolean hasRetention;
    private int timestampDiff = 0;
    private int minToKeep = 0;
    private int replicaLevel = 0;

    public ElasticsearchIndexDefinition(String index,
                                        String concreteIndexName,
                                        String type,
                                        String id,
                                        Settings settings,
                                        Map<String, String> mapping,
                                        String timeWindow,
                                        boolean mock,
                                        boolean ignoreErrors,
                                        boolean switchAliases,
                                        boolean hasRetention,
                                        int timestampDiff,
                                        int minToKeep,
                                        int replicaLevel
    ) {
        this.index = index;
        this.concreteIndexName = concreteIndexName;
        this.type = type;
        this.id = id;
        this.settings = settings;
        this.mapping = mapping;
        this.timeWindow = timeWindow;
        this.mock = mock;
        this.ignoreErrors = ignoreErrors;
        this.switchAliases = switchAliases;
        this.hasRetention = hasRetention;
        this.timestampDiff = timestampDiff;
        this.minToKeep = minToKeep;
        this.replicaLevel = replicaLevel;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getIndex() {
        return index;
    }

    public void setConcreteIndexName(String concreteIndexName) {
        this.concreteIndexName = concreteIndexName;
    }

    public String getConcreteIndex() {
        return concreteIndexName;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Settings getSettings() {
        return settings;
    }

    public Map<String, String> getMapping() {
        return mapping;
    }

    public String getTimeWindow() {
        return timeWindow;
    }

    public boolean isMock() {
        return mock;
    }

    public boolean ignoreErrors() {
        return ignoreErrors;
    }

    public boolean isSwitchAliases() {
        return switchAliases;
    }

    public boolean hasRetention() {
        return hasRetention;
    }

    public int getTimestampDiff() {
        return timestampDiff;
    }

    public int getMinToKeep() {
        return minToKeep;
    }

    public int getReplicaLevel() {
        return replicaLevel;
    }

    @Override
    public String toString() {
        return "IndexDefinition[name=" + getIndex() +
                ",type=" + getType() +
                ",timewindow=" + getTimeWindow() +
                ",concrete=" + getConcreteIndex() +
                ",settings=" + getSettings().getAsMap() +
                ",mapping=" + getMapping() +
                ",mock=" + isMock() +
                ",ignoreErrors=" + ignoreErrors() +
                ",switch=" + isSwitchAliases() +
                ",retention=" + hasRetention() +
                ",timestampDiff=" + getTimestampDiff() +
                ",minToKeep=" + getMinToKeep() +
                ",replicaLevel=" + getReplicaLevel() +
                "]";
    }
}
