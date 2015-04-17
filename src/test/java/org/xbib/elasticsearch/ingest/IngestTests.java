package org.xbib.elasticsearch.ingest;

import org.elasticsearch.client.transport.NoNodeAvailableException;
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

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class IngestTests {

    @Test
    public void testIngest() throws IOException {
        Settings settings = settingsBuilder()
                .putArray("elasticsearch.host", new String[]{"name1:9300", "name2:9301"})
                .build();
        Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
        Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                Runtime.getRuntime().availableProcessors() * 2);
        ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m"));
        TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
        File home = new File(settings.get("home", "."));
        BulkTransportClient ingest = new BulkTransportClient();
        Settings clientSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", settings.get("elasticsearch.cluster", "elasticsearch"))
                .putArray("host", settings.getAsArray("elasticsearch.host"))
                .put("port", settings.getAsInt("elasticsearch.port", 9300))
                .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                .put("name", "feeder") // prevents lookup of names.txt, we don't have it, and marks this node as "feeder". See also module load skipping in JDBCRiverPlugin
                .put("client.transport.ignore_cluster_name", true) // ignore cluster name setting
                .put("client.transport.ping_timeout", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(10))) //  ping timeout
                .put("client.transport.nodes_sampler_interval", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) // for sniff sampling
                .put("path.plugins", ".dontexist") // pointing to a non-exiting folder means, this disables loading site plugins
                        // adding our custom class loader is tricky, actions may not be registered to ActionService
                .classLoader(getClassLoader(getClass().getClassLoader(), home))
                .build();
        try {
            ingest.maxActionsPerBulkRequest(maxbulkactions)
                    .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                    .maxVolumePerBulkRequest(maxvolume)
                    .flushIngestInterval(flushinterval)
                    .newClient(clientSettings);
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
