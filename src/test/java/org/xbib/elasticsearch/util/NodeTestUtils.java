package org.xbib.elasticsearch.util;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.TransportInfo;
import org.testng.Assert;
import org.xbib.elasticsearch.helper.client.ClientHelper;
import org.xbib.elasticsearch.helper.network.NetworkUtils;
import org.xbib.elasticsearch.plugin.helper.HelperPlugin;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public class NodeTestUtils extends Assert {

    protected final static ESLogger logger = ESLoggerFactory.getLogger("test");

    private Map<String, Node> nodes = new HashMap<>();

    private Map<String, AbstractClient> clients = new HashMap<>();

    private AtomicInteger counter = new AtomicInteger();

    private String cluster;

    // note, this must be same name as in json specs
    protected final String index = "my_index";

    protected final String type = "my_type";

    private List<String> hosts;

    public void startNodes() {
        try {
            setClusterName();
            startNode("1");
            findNodeAddresses();
            ClientHelper.waitForCluster(client("1"), ClusterHealthStatus.GREEN, TimeValue.timeValueSeconds(30));
            logger.info("ready");
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    public void stopNodes() {
        try {
            closeNodes();
        } catch (Exception e) {
            logger.error("can not close nodes", e);
        } finally {
            try {
                deleteFiles();
                logger.info("data files wiped");
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    protected void setClusterName() {
        this.cluster = "test-helper-plugin-cluster-"
                + NetworkUtils.getLocalAddress().getHostName()
                + "-" + System.getProperty("user.name")
                + "-" + counter.incrementAndGet();
    }

    protected String getClusterName() {
        return cluster;
    }

    protected String[] getHosts() {
        return hosts != null ? hosts.toArray(new String[hosts.size()]) : new String[]{};
    }

    protected Settings getNodeSettings() {
        return settingsBuilder()
                .put("cluster.name", cluster)
                .put("cluster.routing.schedule", "50ms")
                .put("cluster.routing.allocation.disk.threshold_enabled", false)
                .put("discovery.zen.multicast.enabled", true)
                .put("discovery.zen.multicast.ping_timeout", "5s")
                .put("http.enabled", false)
                .put("threadpool.bulk.size", Runtime.getRuntime().availableProcessors())
                .put("threadpool.bulk.queue_size", 16 * Runtime.getRuntime().availableProcessors()) // default is 50, too low
                .put("index.number_of_replicas", 0)
                .put("path.home", getHome())
                .build();
    }


    protected String getHome() {
        return System.getProperty("path.home");
    }

    public static Node createNode() {
        Settings nodeSettings = Settings.settingsBuilder()
                .put("path.home", System.getProperty("path.home"))
                .put("index.number_of_shards", 1)
                .put("index.number_of_replica", 0)
                .build();
        // ES 2.1 renders NodeBuilder as useless
        //Node node = NodeBuilder.nodeBuilder().settings(nodeSettings).local(true).build().start();
        Set<Class<? extends Plugin>> plugins = new HashSet<>();
        plugins.add(HelperPlugin.class);
        Node node = new MockNode(nodeSettings, plugins);
        node.start();
        return node;
    }

    public void startNode(String id) throws IOException {
        buildNode(id).start();
    }

    public AbstractClient client(String id) {
        return clients.get(id);
    }

    private void closeNodes() throws IOException {
        logger.info("closing all clients");
        for (AbstractClient client : clients.values()) {
            client.close();
        }
        clients.clear();
        logger.info("closing all nodes");
        for (Node node : nodes.values()) {
            if (node != null) {
                node.close();
            }
        }
        nodes.clear();
        logger.info("all nodes closed");
    }

    protected void findNodeAddresses() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
        NodesInfoResponse response = client("1").admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        Iterator<NodeInfo> it = response.iterator();
        hosts = new LinkedList<>();
        hosts = new LinkedList<>();
        while (it.hasNext()) {
            NodeInfo nodeInfo = it.next();
            TransportInfo transportInfo = nodeInfo.getTransport();
            TransportAddress address = transportInfo.getAddress().publishAddress();
            if (address instanceof InetSocketTransportAddress) {
                InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) address;
                hosts.add(inetSocketTransportAddress.address().getHostName() + ":" + inetSocketTransportAddress.address().getPort());
            }
        }
    }

    private Node buildNode(String id) throws IOException {
        Settings nodeSettings = settingsBuilder()
                .put(getNodeSettings())
                .put("name", id)
                .build();
        logger.info("settings={}", nodeSettings.getAsMap());
        // ES 2.1 renders NodeBuilder as useless
        Set<Class<? extends Plugin>> plugins = new HashSet<>();
        plugins.add(HelperPlugin.class);
        Node node = new MockNode(nodeSettings, plugins);
        AbstractClient client = (AbstractClient)node.client();
        nodes.put(id, node);
        clients.put(id, client);
        logger.info("clients={}", clients);
        return node;
    }

    private static void deleteFiles() throws IOException {
        Path directory = Paths.get(System.getProperty("path.home") + "/data");
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

        });

    }

    protected void assertHits(String id, int expectedHits) {
        client(id).admin().indices().prepareRefresh(index).execute().actionGet();
        long hitsFound = client(id).prepareSearch(index).setTypes(type).execute().actionGet().getHits().getTotalHits();
        logger.info("{}/{} = {} hits", index, type, hitsFound);
        assertEquals(hitsFound, expectedHits);
    }

    protected void assertTimestampSort(String id, int expectedHits) {
        client(id).admin().indices().prepareRefresh(index).execute().actionGet();
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        SortBuilder sortBuilder = SortBuilders.fieldSort("_timestamp").order(SortOrder.DESC);
        SearchHits hits = client(id).prepareSearch(index).setTypes(type)
                .setQuery(queryBuilder)
                .addSort(sortBuilder)
                .addFields("_source", "_timestamp")
                .setSize(expectedHits)
                .execute().actionGet().getHits();
        Long prev = Long.MAX_VALUE;
        for (SearchHit hit : hits) {
            if (hit.getFields().get("_timestamp") == null) {
                logger.warn("type mapping was not correctly applied for _timestamp field");
            }
            Long curr = hit.getFields().get("_timestamp").getValue();
            logger.info("timestamp = {}", curr);
            assertTrue(curr <= prev);
            prev = curr;
        }
        logger.info("{}/{} = {} hits", index, type, hits.getTotalHits());
        assertEquals(hits.getTotalHits(), expectedHits);
    }
}
