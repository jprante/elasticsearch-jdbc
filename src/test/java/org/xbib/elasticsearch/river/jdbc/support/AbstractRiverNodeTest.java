
package org.xbib.elasticsearch.river.jdbc.support;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public abstract class AbstractRiverNodeTest extends AbstractRiverTest {

    private static final ESLogger logger = Loggers.getLogger(AbstractRiverNodeTest.class.getSimpleName());

    public final String INDEX = "my_jdbc_river_index";

    public final String TYPE = "my_jdbc_river_type";

    private final static AtomicLong counter = new AtomicLong();

    private Map<String, Node> nodes = newHashMap();

    private Map<String, Client> clients = newHashMap();

    private Settings defaultSettings() {
        return ImmutableSettings
            .settingsBuilder()
            .put("cluster.name", "testing-jdbc-river-on-" + NetworkUtils.getLocalAddress().getHostName() + "-" + counter.incrementAndGet())
            .build();
    }

    @BeforeMethod
    public void createIndices() throws Exception {
        startNode("1").client();
        client("1").admin().indices().create(new CreateIndexRequest(INDEX)).actionGet();
    }

    @AfterMethod
    public void deleteIndices() {
        try {
            // clear test index
            client("1").admin().indices()
                    .delete(new DeleteIndexRequest().indices(INDEX))
                    .actionGet();
        } catch (IndexMissingException e) {
            logger.error(e.getMessage());
        }
        try {
            // clear rivers
            client("1").admin().indices()
                    .delete(new DeleteIndexRequest().indices("_river"))
                    .actionGet();
        } catch (IndexMissingException e) {
            logger.error(e.getMessage());
        }
        closeNode("1");
        closeAllNodes();
    }


    public Node startNode(String id) {
        return buildNode(id).start();
    }

    public Node buildNode(String id) {
        return buildNode(id, defaultSettings());
    }

    public Node buildNode(String id, Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(settings)
                .put("name", id)
                .build();

        if (finalSettings.get("gateway.type") == null) {
            // default to non gateway
            finalSettings = settingsBuilder().put(finalSettings).put("gateway.type", "none").build();
        }

        Node node = nodeBuilder()
                .settings(finalSettings)
                .build();
        nodes.put(id, node);
        clients.put(id, node.client());
        return node;
    }

    public void closeNode(String id) {
        Client client = clients.remove(id);
        if (client != null) {
            client.close();
        }
        Node node = nodes.remove(id);
        if (node != null) {
            node.close();
        }
    }

    public Client client(String id) {
        return clients.get(id);
    }

    public void closeAllNodes() {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            node.close();
        }
        nodes.clear();
    }
}
