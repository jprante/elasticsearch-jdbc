package org.xbib.importer.elasticsearch;

import org.elasticsearch.common.unit.TimeValue;
import org.xbib.content.settings.Settings;
import org.xbib.elasticsearch.extras.client.ClientBuilder;
import org.xbib.elasticsearch.extras.client.ClientMethods;
import org.xbib.elasticsearch.extras.client.IndexAliasAdder;
import org.xbib.elasticsearch.extras.client.SimpleBulkControl;
import org.xbib.elasticsearch.extras.client.SimpleBulkMetric;
import org.xbib.elasticsearch.extras.client.transport.MockTransportClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.xbib.content.json.JsonXContent.contentBuilder;

/**
 *
 */
public class ElasticsearchOutput {

    private static final Logger logger = Logger.getLogger(ElasticsearchOutput.class.getName());

    public ClientMethods createClient(Settings settings) throws IOException {
        if (!settings.containsSetting("cluster")) {
            return null;
        }
        org.elasticsearch.common.settings.Settings elasticsearchSettings =
                org.elasticsearch.common.settings.Settings.builder()
                .put(settings.getAsMap())
                .put("cluster.name", settings.get("cluster", "elasticsearch"))
                .put("sniff", settings.getAsBoolean("sniff", false))
                .put("autodiscover", settings.getAsBoolean("autodiscover", false))
                .putArray("host", settings.getAsArray("host", new String[]{"localhost"}))
                .build();
        ClientBuilder clientBuilder = ClientBuilder.builder()
                .put(elasticsearchSettings)
                .put("client.transport.nodes_sampler_interval", settings.get("timeout", "30s"))
                .put("client.transport.ping_timeout", settings.get("timeout", "30s"))
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, settings.getAsInt("maxbulkactions", 1000))
                .put(ClientBuilder.MAX_CONCURRENT_REQUESTS, settings.getAsInt("maxconcurrentbulkrequests",
                        Runtime.getRuntime().availableProcessors()))
                .setMetric(new SimpleBulkMetric())
                .setControl(new SimpleBulkControl());
        if (settings.getAsBoolean("mock", false)) {
            logger.info("mock");
            return clientBuilder.toMockTransportClient();
        }
        return clientBuilder.toBulkTransportClient();
    }

    public Map<String, ElasticsearchIndexDefinition> makeIndexDefinitions(final ClientMethods clientMethods,
                                                                          Map<String, Settings> map) throws IOException {
        boolean mock = clientMethods.client() instanceof MockTransportClient;
        Map<String, ElasticsearchIndexDefinition> defs = new LinkedHashMap<>();
        for (Map.Entry<String, Settings> entry : map.entrySet()) {
            Settings settings = entry.getValue();
            String indexName = settings.get("name", entry.getKey());
            String concreteIndexName;
            String timeWindow = settings.get("timewindow");
            if (timeWindow != null) {
                String timeWindowStr = DateTimeFormatter.ofPattern(timeWindow)
                        .withZone(ZoneId.systemDefault()) // not GMT
                        .format(LocalDate.now());
                concreteIndexName = indexName + timeWindowStr;
                logger.log(Level.INFO, "concrete index name = {0}", concreteIndexName);
            } else {
                // reuse existing index
                concreteIndexName = clientMethods.resolveMostRecentIndex(indexName);
                logger.log(Level.INFO, "index name " + indexName + " resolved to concrete index name = " + concreteIndexName);
            }
            defs.put(entry.getKey(), new ElasticsearchIndexDefinition(indexName, concreteIndexName,
                    settings.get("type"),
                    null,
                    parseSettings(indexName, settings),
                    parseMapping(indexName, settings),
                    timeWindow,
                    settings.getAsBoolean("mock", mock),
                    settings.getAsBoolean("skiperrors", false),
                    settings.getAsBoolean("aliases", true),
                    settings.getAsBoolean("retention.enabled", false),
                    settings.getAsInt("retention.diff", 0),
                    settings.getAsInt("retention.mintokeep", 0),
                    settings.getAsInt("replica", 0)));
        }
        logger.log(Level.INFO, "defs=" + defs.toString());
        return defs;
    }

    private org.elasticsearch.common.settings.Settings parseSettings(String indexName, Settings settings)
            throws IOException {
        Object o = settings.getAsStructuredMap().get("settings");
        if (o instanceof Map) {
            return org.elasticsearch.common.settings.Settings.builder()
                    .put(settings.getAsSettings("settings").getAsMap())
                    .build();
        } else {
            String string = settings.get("settings",
                    "org/xbib/importer/elasticsearch/settings.json");
            try (InputStream indexSettingsInput = createInputStream(string)) {
                return org.elasticsearch.common.settings.Settings.builder()
                        .loadFromStream(".json", indexSettingsInput)
                        .build();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseMapping(String indexName, Settings settings) throws IOException {
        Object o = settings.getAsStructuredMap().get("mapping");
        Map<String, String> mapping = new HashMap<>();
        if (o instanceof Map) {
            Map<String, Object> map = (Map<String, Object>)o;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                mapping.put(entry.getKey(), contentBuilder().map((Map<String, Object>) entry.getValue()).string());
            }
        } else {
            String string = settings.get("mapping",
                    "org/xbib/importer/elasticsearch/mapping.json");
            try (InputStream indexMappingsInput = createInputStream(string)) {
                Map<String, Object> map = org.elasticsearch.common.settings.Settings.builder()
                        .loadFromStream(".json", indexMappingsInput)
                        .build().getAsStructuredMap();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    mapping.put(entry.getKey(), contentBuilder().map((Map<String, Object>) entry.getValue()).string());
                }
            }
        }
        return mapping;
    }

    private InputStream createInputStream(String string) throws IOException {
        URL url = getClass().getClassLoader().getResource(string);
        return url != null ? url.openStream() : new URL(string).openStream();
    }

    @SuppressWarnings("unchecked")
    public void createIndex(final ClientMethods client, final ElasticsearchIndexDefinition elasticsearchIndexDefinition)
            throws IOException {
        if (client == null || client.client() == null) {
            return;
        }
        client.waitForCluster("YELLOW", TimeValue.timeValueSeconds(30));
        try {
            logger.log(Level.INFO, "new index: name=" + elasticsearchIndexDefinition.getConcreteIndex() +
                    " settings=" + elasticsearchIndexDefinition.getSettings().getAsStructuredMap() +
                    " mappings=" + elasticsearchIndexDefinition.getMapping());
            client.newIndex(elasticsearchIndexDefinition.getConcreteIndex(),
                    elasticsearchIndexDefinition.getSettings(),
                    elasticsearchIndexDefinition.getMapping());
        } catch (Exception e) {
            if (elasticsearchIndexDefinition.ignoreErrors()) {
                logger.log(Level.WARNING, e.getMessage(), e);
                logger.log(Level.WARNING,
                        MessageFormat.format("warning while creating index '{0}' with settings at {1} and mappings at {2}",
                        elasticsearchIndexDefinition.getConcreteIndex(),
                                elasticsearchIndexDefinition.getSettings().getAsMap(),
                                elasticsearchIndexDefinition.getMapping()));
            } else {
                logger.log(Level.SEVERE,
                        MessageFormat.format("error while creating index '{0}' with settings at {1} and mappings at {2}",
                        elasticsearchIndexDefinition.getConcreteIndex(),
                                elasticsearchIndexDefinition.getSettings().getAsMap(),
                                elasticsearchIndexDefinition.getMapping()));
                throw new IOException(e);
            }
        }
    }

    public void startup(ClientMethods clientMethods, Map<String, ElasticsearchIndexDefinition> defs) throws IOException {
        if (clientMethods == null || clientMethods.client() == null) {
            return;
        }
        for (Map.Entry<String, ElasticsearchIndexDefinition> entry : defs.entrySet()) {
            ElasticsearchIndexDefinition def = entry.getValue();
            clientMethods.startBulk(def.getConcreteIndex(), -1, 1);
        }
    }

    public void close(ClientMethods clientMethods, Map<String, ElasticsearchIndexDefinition> defs) throws IOException {
        if (clientMethods == null || clientMethods.client() == null || defs == null) {
            return;
        }
        try {
            logger.info("flush bulk");
            clientMethods.flushIngest();
            logger.info("waiting for all bulk responses from Elasticsearch cluster");
            clientMethods.waitForResponses(TimeValue.timeValueSeconds(120));
            logger.info("all bulk responses received");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.SEVERE, e.getMessage(), e);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        } finally {
            logger.log(Level.INFO, "updating cluster settings of " + defs.keySet());
            for (Map.Entry<String, ElasticsearchIndexDefinition> entry : defs.entrySet()) {
                ElasticsearchIndexDefinition def = entry.getValue();
                clientMethods.stopBulk(def.getConcreteIndex());
            }
        }
    }

    public void switchIndex(ClientMethods clientMethods, ElasticsearchIndexDefinition elasticsearchIndexDefinition,
                            List<String> extraAliases) {
        if (clientMethods == null || clientMethods.client() == null) {
            return;
        }
        if (extraAliases == null) {
            return;
        }
        if (elasticsearchIndexDefinition.isSwitchAliases()) {
            // filter out null/empty values
            List<String> validAliases = extraAliases.stream()
                    .filter(a -> a != null && !a.isEmpty())
                    .collect(Collectors.toList());
            try {
                clientMethods.switchAliases(elasticsearchIndexDefinition.getIndex(),
                        elasticsearchIndexDefinition.getConcreteIndex(), validAliases);
            } catch (Exception e) {
                logger.log(Level.WARNING, "switching index failed: " + e.getMessage(), e);
            }
        }
    }

    public void switchIndex(ClientMethods clientMethods, ElasticsearchIndexDefinition elasticsearchIndexDefinition,
                            List<String> extraAliases, IndexAliasAdder indexAliasAdder) {
        if (clientMethods == null || clientMethods.client() == null) {
            return;
        }
        if (extraAliases == null) {
            return;
        }
        if (elasticsearchIndexDefinition.isSwitchAliases()) {
            // filter out null/empty values
            List<String> validAliases = extraAliases.stream()
                    .filter(a -> a != null && !a.isEmpty())
                    .collect(Collectors.toList());
            try {
                clientMethods.switchAliases(elasticsearchIndexDefinition.getIndex(),
                        elasticsearchIndexDefinition.getConcreteIndex(),
                        validAliases, indexAliasAdder);
            } catch (Exception e) {
                logger.log(Level.WARNING, "switching index failed: " + e.getMessage(), e);
            }
        }
    }

    public void retention(ClientMethods clientMethods, ElasticsearchIndexDefinition elasticsearchIndexDefinition) {
        if (clientMethods == null || clientMethods.client() == null) {
            return;
        }
        logger.log(Level.INFO, MessageFormat.format("retention parameters: name={0} enabled={1} timestampDiff={2} minToKeep={3}",
                elasticsearchIndexDefinition.getIndex(),
                elasticsearchIndexDefinition.hasRetention(),
                elasticsearchIndexDefinition.getTimestampDiff(),
                elasticsearchIndexDefinition.getMinToKeep()));
        if (elasticsearchIndexDefinition.hasRetention() && (elasticsearchIndexDefinition.getTimestampDiff() > 0 ||
                elasticsearchIndexDefinition.getMinToKeep() > 0)) {
            clientMethods.performRetentionPolicy(elasticsearchIndexDefinition.getIndex(),
                    elasticsearchIndexDefinition.getConcreteIndex(),
                    elasticsearchIndexDefinition.getTimestampDiff(),
                    elasticsearchIndexDefinition.getMinToKeep());
        }
    }

    public void replica(ClientMethods clientMethods, ElasticsearchIndexDefinition elasticsearchIndexDefinition) {
        if (clientMethods == null || clientMethods.client() == null) {
            return;
        }
        if (elasticsearchIndexDefinition.getReplicaLevel() > 0) {
            try {
                clientMethods.updateReplicaLevel(elasticsearchIndexDefinition.getConcreteIndex(),
                        elasticsearchIndexDefinition.getReplicaLevel());
            } catch (Exception e) {
                logger.log(Level.WARNING,"setting replica failed: " + e.getMessage(), e);
            }
        }
    }

    public void shutdown(ClientMethods clientMethods) {
        if (clientMethods == null || clientMethods.client() == null) {
            return;
        }
        clientMethods.shutdown();
    }

}
