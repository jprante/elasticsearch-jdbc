package org.xbib.elasticsearch.support.client.node;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.support.client.ClientHelper;
import org.xbib.elasticsearch.support.client.ConfigHelper;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.State;
import org.xbib.elasticsearch.support.client.bulk.BulkProcessorHelper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node client support
 */
public class BulkNodeClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger(BulkNodeClient.class.getSimpleName());

    private int maxActionsPerBulkRequest = 100;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 2;

    private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

    private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

    private final ConfigHelper configHelper = new ConfigHelper();

    private final AtomicLong concurrentRequestCounter = new AtomicLong(0L);

    private Client client;

    private BulkProcessor bulkProcessor;

    private State state;

    private boolean closed = false;

    private Throwable throwable;

    @Override
    public BulkNodeClient shards(int shards) {
        configHelper.setting("index.number_of_shards", shards);
        return this;
    }

    @Override
    public BulkNodeClient replica(int replica) {
        configHelper.setting("index.number_of_replica", replica);
        return this;
    }

    @Override
    public BulkNodeClient maxActionsPerBulkRequest(int maxActionsPerBulkRequest) {
        this.maxActionsPerBulkRequest = maxActionsPerBulkRequest;
        return this;
    }

    @Override
    public BulkNodeClient maxConcurrentBulkRequests(int maxConcurrentBulkRequests) {
        this.maxConcurrentBulkRequests = maxConcurrentBulkRequests;
        return this;
    }

    @Override
    public BulkNodeClient maxVolumePerBulkRequest(ByteSizeValue maxVolume) {
        this.maxVolume = maxVolume;
        return this;
    }

    @Override
    public BulkNodeClient maxRequestWait(TimeValue timeValue) {
        // ignore, not implemented
        return this;
    }

    @Override
    public BulkNodeClient flushIngestInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    @Override
    public BulkNodeClient newClient(URI uri) {
        throw new UnsupportedOperationException();
    }

    public BulkNodeClient newClient(Client client) {
        this.client = client;
        this.state = new State();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                long l = concurrentRequestCounter.getAndIncrement();
                int n = request.numberOfActions();
                state.getSubmitted().inc(n);
                state.getCurrentIngestNumDocs().inc(n);
                state.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                logger.debug("before bulk [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                            executionId,
                            request.numberOfActions(),
                            request.estimatedSizeInBytes(),
                            l);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                long l = concurrentRequestCounter.decrementAndGet();
                state.getSucceeded().inc(response.getItems().length);
                state.getFailed().inc(0);
                state.getTotalIngest().inc(response.getTookInMillis());
                int n = 0;
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (itemResponse.isFailed()) {
                        n++;
                        state.getSucceeded().dec(1);
                        state.getFailed().inc(1);
                    }
                }
                logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] [concurrent requests={}]",
                            executionId,
                            state.getSucceeded().count(),
                            state.getFailed().count(),
                            response.getTook().millis(),
                            l);
                if (n > 0) {
                    logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                            executionId, n, response.buildFailureMessage());
                } else {
                    state.getCurrentIngestNumDocs().dec(response.getItems().length);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                concurrentRequestCounter.decrementAndGet();
                throwable = failure;
                closed = true;
                logger.error("after bulk [" + executionId + "] error", failure);
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder(client, listener)
                .setBulkActions(maxActionsPerBulkRequest)  // off-by-one
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(flushInterval);
        if (maxVolume != null) {
            builder.setBulkSize(maxVolume);
        }
        this.bulkProcessor = builder.build();
        try {
            waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30));
            closed = false;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            closed = true;
        }
        return this;
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public BulkNodeClient putMapping(String index) {
        if (client == null) {
            logger.warn("no client for put mapping");
            return this;
        }
        configHelper.putMapping(client, index);
        return this;
    }

    @Override
    public BulkNodeClient deleteMapping(String index, String type) {
        if (client == null) {
            logger.warn("no client for delete mapping");
            return this;
        }
        configHelper.deleteMapping(client, index, type);
        return this;
    }

    @Override
    public BulkNodeClient index(String index, String type, String id, String source) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            if (state != null) {
                state.getCurrentIngest().inc();
            }
            bulkProcessor.add(new IndexRequest(index).type(type).id(id).create(false).source(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        } finally {
            if (state != null) {
                state.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public BulkNodeClient bulkIndex(IndexRequest indexRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            if (state != null) {
                state.getCurrentIngest().inc();
            }
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        } finally {
            if (state != null) {
                state.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public BulkNodeClient delete(String index, String type, String id) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            if (state != null) {
                state.getCurrentIngest().inc();
            }
            bulkProcessor.add(new DeleteRequest(index).type(type).id(id));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        } finally {
            if (state != null) {
                state.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public BulkNodeClient bulkDelete(DeleteRequest deleteRequest) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        try {
            if (state != null) {
                state.getCurrentIngest().inc();
            }
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        } finally {
            if (state != null) {
                state.getCurrentIngest().dec();
            }
        }
        return this;
    }

    @Override
    public BulkNodeClient flushIngest() {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        logger.debug("flushing bulk processor");
        BulkProcessorHelper.flush(bulkProcessor);
        return this;
    }

    @Override
    public BulkNodeClient waitForResponses(TimeValue maxWaitTime) throws InterruptedException {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        BulkProcessorHelper.waitFor(bulkProcessor, maxWaitTime);
        return this;
    }

    @Override
    public BulkNodeClient startBulk(String index) throws IOException {
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
    public BulkNodeClient stopBulk(String index) throws IOException {
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
    public BulkNodeClient flush(String index) {
        ClientHelper.flush(client, index);
        return this;
    }

    @Override
    public BulkNodeClient refresh(String index) {
        ClientHelper.refresh(client, index);
        return this;
    }

    @Override
    public int updateReplicaLevel(String index, int level) throws IOException {
        return ClientHelper.updateReplicaLevel(client, index, level);
    }


    @Override
    public BulkNodeClient waitForCluster(ClusterHealthStatus status, TimeValue timeout) throws IOException {
        ClientHelper.waitForCluster(client, status, timeout);
        return this;
    }

    @Override
    public int waitForRecovery(String index) throws IOException {
        return ClientHelper.waitForRecovery(client, index);
    }

    @Override
    public synchronized void shutdown() {
        try {
            if (bulkProcessor != null) {
                logger.debug("closing bulk processor...");
                bulkProcessor.close();
            }
            if (state != null && state.indices() != null && !state.indices().isEmpty()) {
                logger.debug("stopping bulk mode for indices {}...", state.indices());
                for (String index : ImmutableSet.copyOf(state.indices())) {
                    stopBulk(index);
                }
            }
            logger.debug("shutting down...");
            client.close();
            logger.debug("shutting down completed");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public BulkNodeClient newIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequest request = new CreateIndexRequest(index);
        if (getSettings() != null) {
            request.settings(getSettings());
        }
        if (getMappings() != null) {
            for (Map.Entry<String, String> me : getMappings().entrySet()) {
                request.mapping(me.getKey(), me.getValue());
            }
        }
        logger.info("creating index {} with settings = {}, mappings = {}",
                index, getSettings() != null ? getSettings().getAsMap() : "", getMappings());
        try {
            client.admin().indices().create(request).actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    @Override
    public BulkNodeClient deleteIndex(String index) {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to delete index");
            return this;
        }
        try {
            client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    @Override
    public boolean hasThrowable() {
        return throwable != null;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }


    public ImmutableSettings.Builder getSettingsBuilder() {
        return configHelper.settingsBuilder();
    }

    public void addSetting(InputStream in) throws IOException {
        configHelper.setting(in);
    }

    public void addSetting(String key, String value) {
        configHelper.setting(key, value);
    }

    public void addSetting(String key, Boolean value) {
        configHelper.setting(key, value);
    }

    public void addSetting(String key, Integer value) {
        configHelper.setting(key, value);
    }

    public void setSettings(Settings settings) {
        configHelper.settings(settings);
    }

    public Settings getSettings() {
        return configHelper.settings();
    }

    public void addMapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type, in);
    }

    public void addMapping(String type, String mapping) {
        configHelper.mapping(type, mapping);
    }

    public Map<String, String> getMappings() {
        return configHelper.mappings();
    }

}
