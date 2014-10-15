package org.xbib.elasticsearch.support.helper;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.Assert;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public abstract class AbstractNodeTestHelper extends Assert {

    protected final static ESLogger logger = ESLoggerFactory.getLogger("test");

    private final static AtomicInteger clusterCount = new AtomicInteger();

    protected String cluster;

    // note, this must be same name as in json river specs
    protected final String index = "my_jdbc_river_index";

    protected final String type = "my_jdbc_river_type";

    private Map<String, Node> nodes = newHashMap();

    private Map<String, Client> clients = newHashMap();

    protected void setClusterName() {
        this.cluster = "test-jdbc-cluster-" + NetworkUtils.getLocalAddress().getHostName() + "-" + clusterCount.incrementAndGet();
    }

    protected String getClusterName() {
        return cluster;
    }

    protected Settings getNodeSettings() {
        return ImmutableSettings
                .settingsBuilder()
                .put("cluster.name", getClusterName())
                .put("index.number_of_shards", 1)
                .put("index.number_of_replica", 0)
                .put("cluster.routing.schedule", "50ms")
                .put("gateway.type", "none")
                .put("index.store.type", "ram")
                .put("http.enabled", false)
                .put("discovery.zen.multicast.enabled", false)
                .build();
    }

    public void startNodes() throws Exception {
        setClusterName();

        // we need more than one node, for better resilience of the river state actions
        startNode("1");
        startNode("2");

        // find node address
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
        NodesInfoResponse response = client("1").admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        Object obj = response.iterator().next().getTransport().getAddress().publishAddress();
        if (obj instanceof InetSocketTransportAddress) {
            InetSocketTransportAddress address = (InetSocketTransportAddress) obj;
            // ... do we need our transport address?
        }
        if (obj instanceof LocalTransportAddress) {
            LocalTransportAddress address = (LocalTransportAddress) obj;
            // .... do we need local transport?
        }
    }

    public Node startNode(String id) {
        return buildNode(id).start();
    }

    public Node buildNode(String id) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(getNodeSettings())
                .put("name", id)
                .build();
        Node node = nodeBuilder().local(true).settings(finalSettings).build();
        Client client = node.client();
        nodes.put(id, node);
        clients.put(id, client);
        return node;
    }

    public void waitForYellow(String id) throws IOException {
        logger.info("wait for healthy cluster...");
        ClusterHealthResponse clusterHealthResponse = client(id).admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        if (clusterHealthResponse.isTimedOut()) {
            throw new IOException("error, cluster health is " + clusterHealthResponse.getStatus().name());
        }
        logger.info("cluster health is {}", clusterHealthResponse.getStatus().name());
    }

    public void assertHits(String id, int expectedHits) {
        client(id).admin().indices().prepareRefresh(index).execute().actionGet();
        long hitsFound = client(id).prepareSearch(index).setTypes(type).execute().actionGet().getHits().getTotalHits();
        logger.info("{}/{} = {} hits", index, type, hitsFound);
        assertEquals(hitsFound, expectedHits);
    }

    public void assertTimestampSort(String id, int expectedHits) {
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

    public Client client(String id) {
        Client client = clients.get(id);
        if (client == null) {
            client = nodes.get(id).client();
            clients.put(id, client);
        }
        return client;
    }

    public void stopNodes() {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            node.stop();
            node.close();
        }
        nodes.clear();
    }

}
