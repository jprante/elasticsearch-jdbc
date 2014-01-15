
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.StructuredObject;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A river mouth implementation for the 'simple' strategy.
 * <p/>
 * This mouth receives StructuredObjects in the
 * create(), index(), or delete() methods and passes them to the bulk indexing client.
 * <p/>
 * Bulk indexing is implemented concurrently. Therefore, many JDBC rivers can pass their
 * data through this river target to ElasticSearch, without having to take precaution
 * of overwhelming the index.
 * <p/>
 * The default size of a bulk request is 100 documents, the maximum number of concurrent requests is 30.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class SimpleRiverMouth implements RiverMouth {

    private static final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverMouth.class.getSimpleName());

    private static final AtomicInteger outstandingBulkRequests = new AtomicInteger(0);

    protected RiverContext context;

    protected String index;

    protected String type;

    protected String id;

    protected Client client;

    private int maxBulkActions = 100;

    private int maxConcurrentBulkRequests = 30;

    private boolean acknowledge = false;

    private volatile boolean error;

    private BulkProcessor bulk;

    protected ESLogger logger() {
        return logger;
    }

    private final BulkProcessor.Listener listener = new BulkProcessor.Listener() {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            long l = outstandingBulkRequests.incrementAndGet();
            logger().info("new bulk [{}] of [{} items], {} outstanding bulk requests",
                    executionId, request.numberOfActions(), l);
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (acknowledge()) {
                try {
                    context.riverSource().acknowledge(response);
                } catch (IOException e) {
                    logger().error("bulk [" + executionId + "] acknowledge error", e);
                }
            }
            outstandingBulkRequests.decrementAndGet();
            logger().info("bulk [{}] success [{} items] [{}ms]",
                    executionId, response.getItems().length, response.getTookInMillis());

        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            outstandingBulkRequests.decrementAndGet();
            logger().error("bulk [" + executionId + "] error", failure);
            error = true;
        }
    };

    @Override
    public String strategy() {
        return "simple";
    }

    @Override
    public SimpleRiverMouth riverContext(RiverContext context) {
        this.context = context;
        return this;
    }

    @Override
    public SimpleRiverMouth client(Client client) {
        this.client = client;
        this.bulk = BulkProcessor.builder(client, listener)
                .setBulkActions(maxBulkActions - 1)
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(TimeValue.timeValueSeconds(1))
                .build();

        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public SimpleRiverMouth index(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String index() {
        return index;
    }

    @Override
    public SimpleRiverMouth type(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public SimpleRiverMouth id(String id) {
        this.id = id;
        return this;
    }

    public String id() {
        return id;
    }

    @Override
    public SimpleRiverMouth maxBulkActions(int bulkSize) {
        this.maxBulkActions = bulkSize;
        return this;
    }

    @Override
    public int maxBulkActions() {
        return maxBulkActions;
    }

    @Override
    public SimpleRiverMouth maxConcurrentBulkRequests(int max) {
        this.maxConcurrentBulkRequests = max;
        return this;
    }

    @Override
    public int maxConcurrentBulkRequests() {
        return maxConcurrentBulkRequests;
    }

    @Override
    public SimpleRiverMouth acknowledge(boolean enable) {
        this.acknowledge = enable;
        return this;
    }

    @Override
    public boolean acknowledge() {
        return acknowledge;
    }

    @Override
    public void create(StructuredObject object) throws IOException {
        index(object, true);
    }

    @Override
    public void index(StructuredObject object) throws IOException {
        index(object, false);
    }

    public void index(StructuredObject object, boolean create) throws IOException {
        if (Strings.hasLength(object.index())) {
            index(object.index());
        }
        if (Strings.hasLength(object.type())) {
            type(object.type());
        }
        if (Strings.hasLength(object.id())) {
            id(object.id());
        }
        IndexRequest request = Requests.indexRequest(index())
                .type(type())
                .id(id())
                .source(object.build());
        if (create) {
            request.create(create);
        }
        if (object.meta(StructuredObject.VERSION) != null) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(object.meta(StructuredObject.VERSION)));
        }
        if (object.meta(StructuredObject.ROUTING) != null) {
            request.routing(object.meta(StructuredObject.ROUTING));
        }
        if (object.meta(StructuredObject.PARENT) != null) {
            request.parent(object.meta(StructuredObject.PARENT));
        }
        if (object.meta(StructuredObject.TIMESTAMP) != null) {
            request.timestamp(object.meta(StructuredObject.TIMESTAMP));
        }
        if (object.meta(StructuredObject.TTL) != null) {
            request.ttl(Long.parseLong(object.meta(StructuredObject.TTL)));
        }
        bulk.add(request);
    }


    @Override
    public void delete(StructuredObject object) {
        if (Strings.hasLength(object.index())) {
            index(object.index());
        }
        if (Strings.hasLength(object.type())) {
            type(object.type());
        }
        if (Strings.hasLength(object.id())) {
            id(object.id());
        }
        if (id == null) {
            return; // skip if no doc is specified to delete
        }
        DeleteRequest request = Requests.deleteRequest(index()).type(type()).id(id());
        if (object.meta(StructuredObject.ROUTING) != null) {
            request.routing(object.meta(StructuredObject.ROUTING));
        }
        if (object.meta(StructuredObject.PARENT) != null) {
            request.parent(object.meta(StructuredObject.PARENT));
        }
        if (object.meta(StructuredObject.VERSION) != null) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(object.meta(StructuredObject.VERSION)));
        }
        bulk.add(request);
    }

    @Override
    public void flush() throws IOException {
        if (error) {
            return;
        }
        //bulk.flush();
    }

    @Override
    public void close() {
        bulk.close();
    }

    @Override
    public void createIndexIfNotExists(String settings, String mapping) {

        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists()) {
            if (Strings.hasLength(settings)) {
                client.admin().indices().prepareUpdateSettings(index).setSettings(settings).execute().actionGet();
            }
            if (Strings.hasLength(mapping)) {
                client.admin().indices().preparePutMapping(index).setType(type).setSource(mapping).execute().actionGet();
            }
            return;
        }
        CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate(index);
        if (Strings.hasLength(settings)) {
            builder.setSettings(settings);
        }
        builder.execute().actionGet();
        if (Strings.hasLength(mapping)) {
            client.admin().indices().preparePutMapping(index).setType(type).setSource(mapping).execute().actionGet();
        }
    }

    @Override
    public void waitForCluster() throws IOException {
        waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
    }

    public void waitForCluster(ClusterHealthStatus status, TimeValue timeout) throws IOException {
        try {
            logger().info("waiting for cluster state {}", status.name());
            ClusterHealthResponse healthResponse =
                    client.admin().cluster().prepareHealth().setWaitForStatus(status).setTimeout(timeout).execute().actionGet();
            if (healthResponse.isTimedOut()) {
                throw new IOException("cluster state is " + healthResponse.getStatus().name()
                        + " and not " + status.name()
                        + ", cowardly refusing to continue with operations");
            } else {
                logger().info("... cluster state ok");
            }
        } catch (ElasticsearchTimeoutException e) {
            throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue with operations");
        }
    }
}
