package org.xbib.elasticsearch.ingest;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.classloader.uri.URIClassLoader;
import org.xbib.elasticsearch.plugin.jdbc.client.transport.BulkTransportClient;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.Security;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class IngestTests {

    @Test
    public void testIngest() throws IOException {
        // disable DNS caching for failover
        Security.setProperty("networkaddress.cache.ttl", "0");

        Settings settings = settingsBuilder()
                .putArray("elasticsearch.host", new String[]{"localhost:9300", "localhost:9301"})
                // found.no transport module
                .put("transport.type", "org.elasticsearch.transport.netty.FoundNettyTransport")
                .put("transport.found.ssl-ports", 9443)
                .build();
        Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                Runtime.getRuntime().availableProcessors() * 2);
        ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m"));
        TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
        File home = new File(settings.get("home", "."));
        BulkTransportClient ingest = new BulkTransportClient();
        ImmutableSettings.Builder clientSettings = ImmutableSettings.settingsBuilder()
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
                        // adding our custom class loader is tricky, actions may not be registered to ActionService
                .classLoader(getClassLoader(getClass().getClassLoader(), home));
        if (settings.get("transport.type") != null) {
            clientSettings.put("transport.type", settings.get("transport.type"));
        }
        // copy found.no transport settings
        Settings foundTransportSettings = settings.getAsSettings("transport.found");
        if (foundTransportSettings != null) {
            ImmutableMap<String,String> foundTransportSettingsMap = foundTransportSettings.getAsMap();
            for (Map.Entry<String,String> entry : foundTransportSettingsMap.entrySet()) {
                clientSettings.put("transport.found." + entry.getKey(), entry.getValue());
            }
        }
        try {
            ingest.maxActionsPerBulkRequest(maxbulkactions)
                    .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                    .maxVolumePerBulkRequest(maxvolume)
                    .flushIngestInterval(flushinterval)
                    .newClient(clientSettings.build());
        } catch (UnknownHostException e) {
            // ok
        } catch (NoNodeAvailableException e) {
            // ok
        }
    }

    private ClassLoader getClassLoader(ClassLoader parent, File home) {
        URIClassLoader classLoader = new URIClassLoader(parent);
        File[] libs = new File(home + "/lib").listFiles();
        if (libs != null) {
            for (File file : libs) {
                if (file.getName().toLowerCase().endsWith(".jar")) {
                    classLoader.addURI(file.toURI());
                }
            }
        }
        return classLoader;
    }
}
