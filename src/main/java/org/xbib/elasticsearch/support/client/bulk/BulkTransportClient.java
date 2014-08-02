package org.xbib.elasticsearch.support.client.bulk;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.support.client.BaseIngestTransportClient;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.State;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Client using the BulkProcessor of Elasticsearch
 */
public class BulkTransportClient extends BaseIngestTransportClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkTransportClient.class.getSimpleName());
    /**
     * The default size of a bulk request
     */
    private int maxActionsPerBulkRequest = 100;
    /**
     * The default number of maximum concurrent requests
     */
    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 2;
    /**
     * The maximum volume
     */
    private ByteSizeValue maxVolumePerBulkRequest = new ByteSizeValue(10, ByteSizeUnit.MB);

    private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

    /**
     * The concurrent requests
     */
    private final AtomicLong concurrentRequestCounter = new AtomicLong(0L);

    /**
     * The BulkProcessor
     */
    private BulkProcessor bulkProcessor;

    private State state;

    private Throwable throwable;

    private boolean closed = false;

    @Override
    public BulkTransportClient maxActionsPerBulkRequest(int maxActionsPerBulkRequest) {
        this.maxActionsPerBulkRequest = maxActionsPerBulkRequest;
        return this;
    }

    @Override
    public BulkTransportClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public BulkTransportClient maxVolumePerBulkRequest(ByteSizeValue maxVolumePerBulkRequest) {
        this.maxVolumePerBulkRequest = maxVolumePerBulkRequest;
        return this;
    }

    @Override
    public BulkTransportClient maxRequestWait(TimeValue timeout) {
        // ignore, not supported
        return this;
    }

    @Override
    public BulkTransportClient flushIngestInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    public BulkTransportClient newClient(Client client) {
        return this.newClient(findURI());
    }

    /**
     * Create new client
     * The URI describes host and port of the node the client should connect to,
     * with the parameter <tt>es.cluster.name</tt> for the cluster name.
     *
     * @param uri the cluster URI
     * @return this client
     */
    @Override
    public BulkTransportClient newClient(URI uri) {
        return this.newClient(uri, defaultSettings(uri));
    }

    @Override
    public BulkTransportClient newClient(URI uri, Settings settings) {
        super.newClient(uri, settings);
        resetSettings();
        this.state = new State();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = concurrentRequestCounter.getAndIncrement();
                if (state != null) {
                    int n = request.numberOfActions();
                    state.getSubmitted().inc(n);
                    state.getCurrentIngestNumDocs().inc(n);
                    state.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                }
                if (logger.isInfoEnabled()) {
                    logger.info("before bulk [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                            executionId,
                            request.numberOfActions(),
                            request.estimatedSizeInBytes(),
                            l);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                long l = concurrentRequestCounter.decrementAndGet();
                if (state != null) {
                    state.getSucceeded().inc(response.getItems().length);
                    state.getTotalIngest().inc(response.getTookInMillis());
                }
                int n = 0;
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (itemResponse.isFailed()) {
                        n++;
                        state.getSucceeded().dec(1);
                        state.getFailed().inc(1);
                    }
                }
                if (logger.isInfoEnabled()) {
                    logger.info("after bulk [{}] [succeeded={}] [failed={}] [{}ms] [concurrent requests={}]",
                            executionId,
                            state.getSucceeded().count(),
                            state.getFailed().count(),
                            response.getTook().millis(),
                            l);
                }
                if (n > 0) {
                    logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                            executionId, n, response.buildFailureMessage());
                } else {
                    state.getCurrentIngestNumDocs().dec(response.getItems().length);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest requst, Throwable failure) {
                concurrentRequestCounter.decrementAndGet();
                throwable = failure;
                closed = true;
                logger.error("bulk [" + executionId + "] error", failure);
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener)
                .setBulkActions(maxActionsPerBulkRequest)
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(flushInterval);
        if (maxVolumePerBulkRequest != null) {
            builder.setBulkSize(maxVolumePerBulkRequest);
        }
        this.bulkProcessor = builder.build();
        this.closed = false;
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    public BulkTransportClient shards(int value) {
        super.shards(value);
        return this;
    }

    public BulkTransportClient replica(int value) {
        super.replica(value);
        return this;
    }

    @Override
    public BulkTransportClient newIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.newIndex(index);
        return this;
    }

    @Override
    public BulkTransportClient deleteIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        super.deleteIndex(index);
        return this;
    }

    @Override
    public BulkTransportClient startBulk(String index) throws IOException {
        if (state == null) {
            return this;
        }
        if (!state.isBulk(index)) {
            state.startBulk(index);
            ClientHelper.disableRefresh(client, index);
        }
        return this;
    }

    @Override
    public BulkTransportClient stopBulk(String index) throws IOException {
        if (state == null) {
            return this;
        }
        if (state.isBulk(index)) {
            state.stopBulk(index);
            ClientHelper.enableRefresh(client, index);
        }
        return this;
    }

    @Override
    public BulkTransportClient flush(String index) {
        ClientHelper.flush(client, index);
        return this;
    }

    @Override
    public BulkTransportClient refresh(String index) {
        ClientHelper.refresh(client, index);
        return this;
    }

    @Override
    public BulkTransportClient index(String index, String type, String id, String source) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            state.getCurrentIngest().inc();
            bulkProcessor.add(new IndexRequest(index).type(type).id(id).create(false).source(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public BulkTransportClient bulkIndex(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            state.getCurrentIngest().inc();
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public BulkTransportClient delete(String index, String type, String id) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            state.getCurrentIngest().inc();
            bulkProcessor.add(new DeleteRequest(index).type(type).id(id));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete request failed: " + e.getMessage(), e);
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }

    @Override
    public BulkTransportClient bulkDelete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            state.getCurrentIngest().inc();
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete request failed: " + e.getMessage(), e);
        } finally {
            state.getCurrentIngest().dec();
        }
        return this;
    }


    @Override
    public synchronized BulkTransportClient flushIngest() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        logger.info("flushing bulk processor");
        // hacked BulkProcessor to execute the submission of remaining docs. Wait always 30 seconds at most.
        BulkProcessorHelper.flush(bulkProcessor);
        return this;
    }

    @Override
    public synchronized BulkTransportClient waitForResponses(TimeValue maxWaitTime) throws InterruptedException {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        BulkProcessorHelper.waitFor(bulkProcessor, maxWaitTime);
        return this;
    }

    @Override
    public BulkTransportClient waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException {
        ClientHelper.waitForCluster(client, status, timeValue);
        return this;
    }

    @Override
    public int waitForRecovery(String index) throws IOException {
        return ClientHelper.waitForRecovery(client, index);
    }

    @Override
    public int updateReplicaLevel(String index, int level) throws IOException {
        return ClientHelper.updateReplicaLevel(client, index, level);
    }

    @Override
    public synchronized void shutdown() {
        if (closed) {
            super.shutdown();
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return;
        }
        try {
            if (bulkProcessor != null) {
                logger.info("closing bulk processor...");
                bulkProcessor.close();
            }
            if (state.indices() != null && !state.indices().isEmpty()) {
                logger.info("stopping bulk mode for indices {}...", state.indices());
                for (String index : ImmutableSet.copyOf(state.indices())) {
                    stopBulk(index);
                }
            }
            logger.info("shutting down...");
            super.shutdown();
            logger.info("shutting down completed");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public State getState() {
        return state;
    }

    @Override
    public boolean hasThrowable() {
        return throwable != null;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }
}
