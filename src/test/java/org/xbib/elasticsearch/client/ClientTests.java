package org.xbib.elasticsearch.client;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.common.metrics.ElasticsearchIngestMetric;
import org.xbib.elasticsearch.helper.client.ClientBuilder;
import org.xbib.elasticsearch.helper.client.ClientAPI;

import java.io.IOException;
import java.security.Security;
import java.util.Map;

public class ClientTests {

    @Test
    public void testClient() throws IOException {
        // disable DNS caching for failover
        Security.setProperty("networkaddress.cache.ttl", "0");

        Settings settings = Settings.settingsBuilder()
                .putArray("elasticsearch.host", new String[]{"localhost:9300", "localhost:9301"})
                // found.no transport module
                .put("transport.type", "org.elasticsearch.transport.netty.FoundNettyTransport")
                .put("transport.found.ssl-ports", 9443)
                .build();
        Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                Runtime.getRuntime().availableProcessors() * 2);
        ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m", ""));
        TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
        Settings.Builder clientSettings = Settings.settingsBuilder()
                .put("cluster.name", settings.get("elasticsearch.cluster", "elasticsearch"))
                .putArray("host", settings.getAsArray("elasticsearch.host"))
                .put("port", settings.getAsInt("elasticsearch.port", 9300))
                .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                .put("name", "feeder") // prevents lookup of names.txt, we don't have it, and marks this node as "feeder". See also module load skipping in JDBCRiverPlugin
                .put("client.transport.ignore_cluster_name", false) // do not ignore cluster name setting
                .put("client.transport.ping_timeout", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(10))) //  ping timeout
                .put("client.transport.nodes_sampler_interval", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) // for sniff sampling
                .put("path.plugins", ".dontexist") // pointing to a non-exiting folder means, this disables loading site plugins
                .put("path.home", System.getProperty("path.home"))
                ;
        if (settings.get("transport.type") != null) {
            clientSettings.put("transport.type", settings.get("transport.type"));
        }
        // copy found.no transport settings
        Settings foundTransportSettings = settings.getAsSettings("transport.found");
        if (foundTransportSettings != null) {
            Map<String,String> foundTransportSettingsMap = foundTransportSettings.getAsMap();
            for (Map.Entry<String,String> entry : foundTransportSettingsMap.entrySet()) {
                clientSettings.put("transport.found." + entry.getKey(), entry.getValue());
            }
        }
        try {
            ClientAPI clientAPI = ClientBuilder.builder()
                    .put(settings)
                    .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, maxbulkactions)
                    .put(ClientBuilder.MAX_CONCURRENT_REQUESTS, maxconcurrentbulkrequests)
                    .put(ClientBuilder.MAX_VOLUME_PER_REQUEST, maxvolume)
                    .put(ClientBuilder.FLUSH_INTERVAL, flushinterval)
                    .setMetric( new ElasticsearchIngestMetric())
                    .toBulkTransportClient();
        } catch (NoNodeAvailableException e) {
            // ok
        }
    }

    @Test
    public void testFoundSettings() throws IOException {
        // disable DNS caching for failover
        Security.setProperty("networkaddress.cache.ttl", "0");

        Settings settings = Settings.settingsBuilder()
                .putArray("elasticsearch.host", new String[]{"localhost:9300", "localhost:9301"})
                // enable found.no transport module
                .put("transport.type", "org.elasticsearch.transport.netty.FoundNettyTransport")
                .put("transport.found.ssl-ports", 9443)
                .build();

        Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                Runtime.getRuntime().availableProcessors() * 2);
        ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m", ""));
        TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
        Settings.Builder clientSettings = Settings.settingsBuilder()
                .put("cluster.name", settings.get("elasticsearch.cluster", "elasticsearch"))
                .putArray("host", settings.getAsArray("elasticsearch.host"))
                .put("port", settings.getAsInt("elasticsearch.port", 9300))
                .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                .put("name", "feeder") // prevents lookup of names.txt, we don't have it, and marks this node as "feeder". See also module load skipping in JDBCRiverPlugin
                .put("client.transport.ignore_cluster_name", false) // do not ignore cluster name setting
                .put("client.transport.ping_timeout", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) //  ping timeout
                .put("client.transport.nodes_sampler_interval", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) // for sniff sampling
                .put("path.plugins", ".dontexist") // pointing to a non-exiting folder means, this disables loading site plugins
                .put("path.home", System.getProperty("path.home"))
                ;
        try {
            ClientAPI clientAPI = ClientBuilder.builder()
                    .put(settings)
                    .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, maxbulkactions)
                    .put(ClientBuilder.MAX_CONCURRENT_REQUESTS, maxconcurrentbulkrequests)
                    .put(ClientBuilder.MAX_VOLUME_PER_REQUEST, maxvolume)
                    .put(ClientBuilder.FLUSH_INTERVAL, flushinterval)
                    .setMetric( new ElasticsearchIngestMetric())
                    .toBulkTransportClient();
        } catch (NoNodeAvailableException e) {
            // ok
        }
    }

}
