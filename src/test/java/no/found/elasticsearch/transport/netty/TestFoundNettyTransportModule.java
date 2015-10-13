package no.found.elasticsearch.transport.netty;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.netty.FoundNettyTransport;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


public class TestFoundNettyTransportModule {

    @Test
    public void testInjection() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("transport.type", "org.elasticsearch.transport.netty.FoundNettyTransport")
                .put("path.home", System.getProperty("path.home"))
                .put("plugin.types", FoundTransportPlugin.class)
                .build();

        TransportClient client = TransportClient.builder().settings(settings).build();

        Field injectorField = client.getClass().getDeclaredField("injector");
        injectorField.setAccessible(true);
        Injector injector = (Injector)injectorField.get(client);

        assertEquals(FoundNettyTransport.class, injector.getInstance(Transport.class).getClass());
    }

    @Test
    public void testNotInjected() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("path.home", System.getProperty("path.home"))
                .put("plugin.types", FoundTransportPlugin.class)
                .build();

        TransportClient client = TransportClient.builder().settings(settings).build();

        Field injectorField = client.getClass().getDeclaredField("injector");
        injectorField.setAccessible(true);
        Injector injector = (Injector)injectorField.get(client);

        assertNotEquals(FoundNettyTransport.class, injector.getInstance(Transport.class).getClass());
    }

    @Test
    public void testBackwardsCompatibilityWithOnlyClientUsingModule() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("transport.type", "org.elasticsearch.transport.netty.FoundNettyTransport")
                .put("path.home", System.getProperty("path.home"))
                .put("plugin.types", FoundTransportPlugin.class)
                .build();

        Settings nodeSettings = Settings.settingsBuilder()
                .put("path.home", System.getProperty("path.home"))
                .put("plugin.types", FoundTransportPlugin.class)
                .build();

        Node node1 = null;
        TransportClient transportClient = null;

        try {
            node1 = NodeBuilder.nodeBuilder().settings(nodeSettings).node();
            NodesInfoResponse nodesInfo = node1.client().admin().cluster().prepareNodesInfo().setTransport(true).get();
            TransportAddress transportAddress = nodesInfo.getNodes()[0].getTransport().address().publishAddress();
            transportClient = TransportClient.builder().settings(settings).build();
            transportClient.addTransportAddress(transportAddress);
            ClusterHealthStatus status = transportClient.admin().cluster().prepareHealth().get().getStatus();
            assertTrue(status == ClusterHealthStatus.YELLOW || status == ClusterHealthStatus.GREEN);
        } finally {
            try {
                if(transportClient != null) transportClient.close();
            } catch (Exception e) { /* ignore */ }

            try {
                if(node1 != null) node1.close();
            } catch (Exception e) { /* ignore */ }
        }
    }

    @Test
    public void testBackwardsCompatibilityWithClientAndServerUsingModule() throws Exception {
        Settings settings = Settings.settingsBuilder()
                .put("transport.type", "org.elasticsearch.transport.netty.FoundNettyTransport")
                .put("path.home", System.getProperty("path.home"))
                .put("plugin.types", FoundTransportPlugin.class)
                .build();

        Node node1 = null;
        TransportClient transportClient = null;

        try {
            node1 = NodeBuilder.nodeBuilder().settings(settings).node();
            NodesInfoResponse nodesInfo = node1.client().admin().cluster().prepareNodesInfo().setTransport(true).get();
            TransportAddress transportAddress = nodesInfo.getNodes()[0].getTransport().address().publishAddress();
            transportClient = TransportClient.builder().settings(settings).build();
            transportClient.addTransportAddress(transportAddress);
            ClusterHealthStatus status = transportClient.admin().cluster().prepareHealth().get().getStatus();
            assertTrue(status == ClusterHealthStatus.YELLOW || status == ClusterHealthStatus.GREEN);
        } finally {
            try {
                if(transportClient != null) transportClient.close();
            } catch (Exception e) { /* ignore */ }

            try {
                if(node1 != null) node1.close();
            } catch (Exception e) { /* ignore */ }
        }
    }
}