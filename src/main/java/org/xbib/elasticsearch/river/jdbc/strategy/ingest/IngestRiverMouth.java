
package org.xbib.elasticsearch.river.jdbc.strategy.ingest;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import org.xbib.elasticsearch.action.ingest.IngestProcessor;
import org.xbib.elasticsearch.action.ingest.IngestRequest;
import org.xbib.elasticsearch.action.ingest.IngestResponse;
import org.xbib.elasticsearch.gatherer.ControlKeys;
import org.xbib.elasticsearch.gatherer.IndexableObject;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;

import java.io.IOException;
import java.util.Map;

/**
 * Ingest processor based river mouth
 */
public class IngestRiverMouth implements RiverMouth {

    private final ESLogger logger = ESLoggerFactory.getLogger(IngestRiverMouth.class.getName());

    protected RiverContext context;

    protected Map<String,Object> settings;

    protected Map<String,Object> mapping;

    protected String index;

    protected String type;

    protected String id;

    protected Client client;

    private IngestProcessor bulk;

    private int maxBulkActions = 100;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 4;

    private ByteSizeValue maxVolumePerBulkRequest = new ByteSizeValue(10, ByteSizeUnit.MB);

    private TimeValue maxWait = TimeValue.timeValueSeconds(60);

    private volatile boolean error;

    private boolean started;

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "simple";
    }

    private final IngestProcessor.Listener listener = new IngestProcessor.Listener() {

        @Override
        public void beforeBulk(long bulkId, int concurrency, IngestRequest request) {
            logger().info("new bulk [{}] of [{} items], {} outstanding bulk requests",
                    bulkId, request.numberOfActions(), concurrency);
        }

        @Override
        public void afterBulk(long bulkId, int concurrency, IngestResponse response) {
            logger().info("bulk [{}] success [{} items] [{}ms]",
                    bulkId, response.successSize(), response.getTookInMillis());
        }

        @Override
        public void afterBulk(long bulkId, int concurrency, Throwable failure) {
            logger().error("bulk [" + bulkId + "] error", failure);
            error = true;
        }
    };

    @Override
    public IngestRiverMouth riverContext(RiverContext context) {
        this.context = context;
        return this;
    }

    @Override
    public IngestRiverMouth client(Client client) {
        this.client = client;
        this.bulk = new IngestProcessor(client,
                maxBulkActions,
                maxConcurrentBulkRequests,
                maxVolumePerBulkRequest,
                maxWait
        );
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public IngestRiverMouth setSettings(Map<String,Object> settings) {
        this.settings = settings;
        return this;
    }

    @Override
    public IngestRiverMouth setMapping(Map<String,Object> mapping) {
        this.mapping = mapping;
        return this;
    }

    @Override
    public IngestRiverMouth setIndex(String index) {
        this.index = index;
        return this;
    }

    @Override
    public String getIndex() {
        return index;
    }

    @Override
    public IngestRiverMouth setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public IngestRiverMouth setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public IngestRiverMouth setMaxBulkActions(int bulkSize) {
        this.maxBulkActions = bulkSize;
        return this;
    }

    @Override
    public IngestRiverMouth setMaxConcurrentBulkRequests(int max) {
        this.maxConcurrentBulkRequests = max;
        return this;
    }

    @Override
    public IngestRiverMouth setMaxVolumePerBulkRequest(ByteSizeValue maxVolumePerBulkRequest) {
        this.maxVolumePerBulkRequest = maxVolumePerBulkRequest;
        return this;
    }

    @Override
    public IngestRiverMouth setFlushInterval(TimeValue flushInterval) {
        // ignore
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
        bulk.flush();
    }

    @Override
    public void close() {
        try {
            bulk.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage(), e);
        }
    }

    private RiverMouth startup() {
        try {
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
        logger().debug("creating index with request {}", createIndexRequestBuilder.toString());
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
