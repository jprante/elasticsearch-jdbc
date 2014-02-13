
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.indices.IndexAlreadyExistsException;

import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.gatherer.ControlKeys;
import org.xbib.elasticsearch.gatherer.IndexableObject;

/**
 * Simple river mouth
 */
public class SimpleRiverMouth implements RiverMouth {

    private final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverMouth.class.getName());

    private static final AtomicInteger outstandingBulkRequests = new AtomicInteger(0);

    protected RiverContext context;

    protected Map<String,Object> settings;

    protected Map<String,Object> mapping;

    protected String index;

    protected String type;

    protected String id;

    protected Client client;

    private BulkProcessor bulk;

    private int maxBulkActions = 100;

    private int maxConcurrentBulkRequests = 30;

    private TimeValue flushInterval = TimeValue.timeValueSeconds(5);

    private volatile boolean error;

    private boolean started;

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "simple";
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
    public SimpleRiverMouth riverContext(RiverContext context) {
        this.context = context;
        return this;
    }

    @Override
    public SimpleRiverMouth client(Client client) {
        this.client = client;
        this.bulk = BulkProcessor.builder(client, listener)
                .setBulkActions(maxBulkActions - 1) // yes, offset by one!
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(flushInterval)
                .build();
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public SimpleRiverMouth setSettings(Map<String,Object> settings) {
        this.settings = settings;
        return this;
    }

    @Override
    public SimpleRiverMouth setMapping(Map<String,Object> mapping) {
        this.mapping = mapping;
        return this;
    }

    @Override
    public SimpleRiverMouth setIndex(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public SimpleRiverMouth setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public SimpleRiverMouth setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public SimpleRiverMouth setMaxBulkActions(int bulkSize) {
        this.maxBulkActions = bulkSize;
        return this;
    }

    @Override
    public SimpleRiverMouth setMaxConcurrentBulkRequests(int max) {
        this.maxConcurrentBulkRequests = max;
        return this;
    }

    @Override
    public SimpleRiverMouth setFlushInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    @Override
    public void index(IndexableObject object, boolean create) throws IOException {
        if (error) {
            logger().error("error, not indexing");
            return;
        }
        if (!started) {
            started = true;
            startup();
        }
        if (Strings.hasLength(object.index())) {
            setIndex(object.index());
        }
        if (Strings.hasLength(object.type())) {
            setType(object.type());
        }
        if (Strings.hasLength(object.id())) {
            setId(object.id());
        }
        IndexRequest request = Requests.indexRequest(getIndex())
                .type(getType())
                .id(getId())
                .source(object.build());
        if (object.meta(ControlKeys._version.name()) != null) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(object.meta(ControlKeys._version.name())));
        }
        if (object.meta(ControlKeys._routing.name()) != null) {
            request.routing(object.meta(ControlKeys._routing.name()));
        }
        if (object.meta(ControlKeys._parent.name()) != null) {
            request.parent(object.meta(ControlKeys._parent.name()));
        }
        if (object.meta(ControlKeys._timestamp.name()) != null) {
            request.timestamp(object.meta(ControlKeys._timestamp.name()));
        }
        if (object.meta(ControlKeys._ttl.name()) != null) {
            request.ttl(Long.parseLong(object.meta(ControlKeys._ttl.name())));
        }
        bulk.add(request);
    }

    @Override
    public void delete(IndexableObject object) {
        if (error) {
            logger().error("error, not indexing");
            return;
        }
        if (!started) {
            started = true;
            startup();
        }
        if (Strings.hasLength(object.index())) {
            setIndex(object.index());
        }
        if (Strings.hasLength(object.type())) {
            setType(object.type());
        }
        if (Strings.hasLength(object.id())) {
            setId(object.id());
        }
        if (getId() == null) {
            return; // skip if no doc is specified to delete
        }
        DeleteRequest request = Requests.deleteRequest(getIndex()).type(getType()).id(getId());
        if (object.meta(ControlKeys._version.name()) != null) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(object.meta(ControlKeys._version.name())));
        }
        if (object.meta(ControlKeys._routing.name()) != null) {
            request.routing(object.meta(ControlKeys._routing.name()));
        }
        if (object.meta(ControlKeys._parent.name()) != null) {
            request.parent(object.meta(ControlKeys._parent.name()));
        }
        bulk.add(request);
    }

    @Override
    public void flush() throws IOException {
        try {
            // we must wait for flush interval... not really cool
            Thread.sleep(flushInterval.millis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted (maybe harmless)");
        }
    }

    @Override
    public void close() {
        bulk.close();
    }

    private RiverMouth startup() {
        try {
            //updateSettings();
            //updateMapping();
            createIndexIfNotExists();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                logger().warn("index {} already exists", index);
            } else {
                logger().warn("failed to create index", e);
                error = true;
            }
        }
        return this;
    }

    private void updateSettings() {
        if (error) {
            logger().error("error, not updating settings");
            return;
        }
        if (settings != null) {
            client.admin().indices().prepareUpdateSettings(index)
                    .setSettings(settings)
                    .execute().actionGet();
        }
    }

    private void updateMapping() {
        if (error) {
            logger().error("error, not updating mapping");
            return;
        }
        if (mapping != null) {
            client.admin().indices().preparePutMapping(index)
                    .setType(type)
                    .setSource(mapping)
                    .execute().actionGet();
        }
    }

    private void createIndexIfNotExists() {
        if (error) {
            logger().error("error, not creating index");
            return;
        }
        if (client.admin().indices().prepareExists(index).execute().actionGet().isExists()) {
            logger().warn("index {} already exists, ignoring index creation", index);
            return;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                client.admin().indices().prepareCreate(index);
        if (settings != null) {
            createIndexRequestBuilder.setSettings(settings);
        }
        if (mapping != null) {
            createIndexRequestBuilder.addMapping(type, mapping);
        }
        createIndexRequestBuilder.execute().actionGet();
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
