package org.xbib.importer.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.Netty4Plugin;
import org.xbib.elasticsearch.extras.client.NetworkUtils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class NodeTestBase {

    private static final Logger logger = LogManager.getLogger("test");

    // note, this must be same name as in json specs
    protected final String index = "my_index";

    protected final String type = "my_type";

    private Map<String, Node> nodes = new HashMap<>();

    private Map<String, AbstractClient> clients = new HashMap<>();

    private AtomicInteger counter = new AtomicInteger();

    private String clustername;

    private String host;

    private int port;

    public void startNodes() {
        try {
            setClusterName();
            logger.info("cluster name is {}", getClusterName());
            startNode("1");
            findNodeAddress();
            ClusterHealthResponse healthResponse = client("1").execute(ClusterHealthAction.INSTANCE,
                    new ClusterHealthRequest().waitForStatus(ClusterHealthStatus.GREEN)
                            .timeout(TimeValue.timeValueSeconds(30))).actionGet();
            if (healthResponse != null && healthResponse.isTimedOut()) {
                throw new IOException("cluster state is " + healthResponse.getStatus().name()
                        + ", from here on, everything will fail!");
            }
            logger.info("startNodes complete");
        } catch (Throwable t) {
            logger.error("start of node failed", t);
        }
    }

    public void stopNodes() {
        try {
            logger.info("stopping nodes");
            closeNodes();
        } catch (Throwable e) {
            logger.error("can not close nodes", e);
        } finally {
            try {
                deleteFiles();
                logger.info("data files wiped");
                Thread.sleep(2000L); // let OS commit changes
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    protected void setClusterName() {
        this.clustername = "test-helper-cluster-"
                + NetworkUtils.getLocalAddress().getHostName()
                + "-" + System.getProperty("user.name")
                + "-" + counter.incrementAndGet();
    }

    protected String getClusterName() {
        return clustername;
    }

    protected Settings getNodeSettings() {
        String hostname = NetworkUtils.getLocalAddress().getHostName();
        return Settings.builder()
                .put("cluster.name", clustername)
                // required to build a cluster, replica tests will test this.
                .put("discovery.zen.ping.unicast.hosts", hostname)
                .put("transport.type", Netty4Plugin.NETTY_TRANSPORT_NAME)
                .put("network.host", hostname)
                .put("http.enabled", false)
                .put("path.home", getHome())
                // maximum five nodes on same host
                .put("node.max_local_storage_nodes", 5)
                .put("thread_pool.bulk.size", Runtime.getRuntime().availableProcessors())
                // default is 50 which is too low
                .put("thread_pool.bulk.queue_size", 16 * Runtime.getRuntime().availableProcessors())
                .build();
    }


    protected Settings getClientSettings() {
        if (host == null) {
            throw new IllegalStateException("host is null");
        }
        // the host to which transport client should connect to
        return Settings.builder()
                .put("cluster.name", clustername)
                .put("host", host + ":" + port)
                .build();
    }

    protected String getHome() {
        return System.getProperty("path.home");
    }

    protected String getHost() {
        return host;
    }

    public void startNode(String id) throws IOException {
        try {
            buildNode(id).start();
        } catch (NodeValidationException e) {
            throw new IOException(e);
        }
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

    protected void findNodeAddress() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
        NodesInfoResponse response = client("1").admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        Object obj = response.getNodes().iterator().next().getTransport().getAddress()
                .publishAddress();
        if (obj instanceof InetSocketTransportAddress) {
            InetSocketTransportAddress address = (InetSocketTransportAddress) obj;
            host = address.address().getHostName();
            port = address.address().getPort();
        } else if (obj instanceof LocalTransportAddress) {
            LocalTransportAddress address = (LocalTransportAddress) obj;
            host = address.getHost();
            port = address.getPort();
        } else {
            logger.info("class=" + obj.getClass());
        }
        if (host == null) {
            throw new IllegalArgumentException("host not found");
        }
    }

    private Node buildNode(String id) throws IOException {
        Settings nodeSettings = Settings.builder()
                .put(getNodeSettings())
                .build();
        logger.info("settings={}", nodeSettings.getAsMap());
        Node node = new MockNode(nodeSettings, Netty4Plugin.class);
        AbstractClient client = (AbstractClient) node.client();
        nodes.put(id, node);
        clients.put(id, client);
        logger.info("clients={}", clients);
        return node;
    }

    protected void assertHits(String id, int expectedHits) {
        client(id).admin().indices().prepareRefresh(index).execute().actionGet();
        long hitsFound = client(id).prepareSearch(index).setTypes(type).execute().actionGet().getHits().getTotalHits();
        logger.info("{}/{} = {} hits", index, type, hitsFound);
        assertEquals(hitsFound, expectedHits);
    }

    protected void assertTimestampSort(String clientId, String field, int expectedHits) {
        client(clientId).admin().indices().prepareRefresh(index).execute().actionGet();
        SearchHits hits = client(clientId).prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.fieldSort(field).order(SortOrder.DESC))
                .addStoredField(field)
                .setSize(expectedHits)
                .execute().actionGet().getHits();
        Long prev = Long.MAX_VALUE;
        logger.info("{}/{} = {} hits", index, type, hits.getTotalHits());
        for (SearchHit hit : hits) {
            logger.info("hit: {}/{}/{} fields={}", hit.getIndex(), hit.getType(), hit.getId(), hit.getFields());
            if (hit.getFields().get(field) == null) {
                logger.warn("type mapping was not correctly applied, source=" + hit.getSourceAsString());
            } else {
                Long curr = Instant.parse(hit.getFields().get(field).getValue()).toEpochMilli();
                logger.info("timestamp = {}", curr);
                assertTrue(curr <= prev);
                prev = curr;
            }
        }
        assertEquals(hits.getTotalHits(), expectedHits);
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
}
