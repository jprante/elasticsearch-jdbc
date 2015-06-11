/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.plugin.jdbc.client.node;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
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
import org.xbib.elasticsearch.plugin.jdbc.client.BulkProcessorHelper;
import org.xbib.elasticsearch.plugin.jdbc.client.ClientHelper;
import org.xbib.elasticsearch.plugin.jdbc.client.ConfigHelper;
import org.xbib.elasticsearch.plugin.jdbc.client.Ingest;
import org.xbib.elasticsearch.plugin.jdbc.client.Metric;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Node client support with bulk processing
 */
public class BulkNodeClient implements Ingest {

    private final static ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.BulkNodeClient");

    private int maxActionsPerBulkRequest = 100;

    private int maxConcurrentBulkRequests = Runtime.getRuntime().availableProcessors() * 2;

    private ByteSizeValue maxVolume = new ByteSizeValue(10, ByteSizeUnit.MB);

    private TimeValue flushInterval = TimeValue.timeValueSeconds(30);

    private final ConfigHelper configHelper = new ConfigHelper();

    private Client client;

    private BulkProcessor bulkProcessor;

    private boolean isShutdown = false;

    private Metric metric = new Metric();

    private boolean closed = false;

    private boolean suspended = false;

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
    public BulkNodeClient flushIngestInterval(TimeValue flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    @Override
    public BulkNodeClient newClient(Settings settings) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BulkNodeClient newClient(Client client) {
        this.client = client;
        this.metric = new Metric();
        metric.start();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                metric.getCurrentIngest().inc();
                long l = metric.getCurrentIngest().count();
                int n = request.numberOfActions();
                metric.getSubmitted().inc(n);
                metric.getCurrentIngestNumDocs().inc(n);
                metric.getTotalIngestSizeInBytes().inc(request.estimatedSizeInBytes());
                logger.debug("before bulk [{}] [actions={}] [bytes={}] [concurrent requests={}]",
                        executionId,
                        request.numberOfActions(),
                        request.estimatedSizeInBytes(),
                        l);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                metric.getCurrentIngest().dec();
                long l = metric.getCurrentIngest().count();
                metric.getSucceeded().inc(response.getItems().length);
                metric.getFailed().inc(0);
                metric.getTotalIngest().inc(response.getTookInMillis());
                int n = 0;
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (itemResponse.isFailed()) {
                        n++;
                        metric.getSucceeded().dec(1);
                        metric.getFailed().inc(1);
                    }
                }
                logger.debug("after bulk [{}] [succeeded={}] [failed={}] [{}ms] {} concurrent requests",
                        executionId,
                        metric.getSucceeded().count(),
                        metric.getFailed().count(),
                        response.getTook().millis(),
                        l);
                if (n > 0) {
                    logger.error("bulk [{}] failed with {} failed items, failure message = {}",
                            executionId, n, response.buildFailureMessage());
                } else {
                    metric.getCurrentIngestNumDocs().dec(response.getItems().length);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                metric.getCurrentIngest().dec();
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
    public Metric getMetric() {
        return metric;
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
            if (metric != null) {
                metric.getCurrentIngest().inc();
            }
            bulkProcessor.add(new IndexRequest(index).type(type).id(id).create(false).source(source));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        } finally {
            if (metric != null) {
                metric.getCurrentIngest().dec();
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
            if (suspended) {
                Thread.sleep(1000L);
                return this;
            }
            if (metric != null) {
                metric.getCurrentIngest().inc();
            }
            bulkProcessor.add(indexRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of index request failed: " + e.getMessage(), e);
        } finally {
            if (metric != null) {
                metric.getCurrentIngest().dec();
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
            if (suspended) {
                Thread.sleep(1000L);
                return this;
            }
            if (metric != null) {
                metric.getCurrentIngest().inc();
            }
            bulkProcessor.add(new DeleteRequest(index).type(type).id(id));
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        } finally {
            if (metric != null) {
                metric.getCurrentIngest().dec();
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
            if (suspended) {
                Thread.sleep(1000L);
                return this;
            }
            if (metric != null) {
                metric.getCurrentIngest().inc();
            }
            bulkProcessor.add(deleteRequest);
        } catch (Exception e) {
            throwable = e;
            closed = true;
            logger.error("bulk add of delete failed: " + e.getMessage(), e);
        } finally {
            if (metric != null) {
                metric.getCurrentIngest().dec();
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
        if (metric.getCurrentIngest().count() == 0) {
            logger.debug("no current ingests");
            return this;
        }
        BulkProcessorHelper.waitFor(bulkProcessor, maxWaitTime);
        return this;
    }

    @Override
    public BulkNodeClient startBulk(String index, long startRefreshInterval, long stopRefreshIterval) throws IOException {
        if (metric == null) {
            return this;
        }
        if (!metric.isBulk(index)) {
            metric.setupBulk(index, startRefreshInterval, stopRefreshIterval);
            ClientHelper.updateIndexSetting(client, index, "refresh_interval", startRefreshInterval);
        }
        return this;
    }

    @Override
    public BulkNodeClient stopBulk(String index) throws IOException {
        if (metric == null) {
            return this;
        }
        if (metric.isBulk(index)) {
            ClientHelper.updateIndexSetting(client, index, "refresh_interval", metric.getStopBulkRefreshIntervals().get(index));
            metric.removeBulk(index);
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
            if (metric != null && metric.indices() != null && !metric.indices().isEmpty()) {
                logger.debug("stopping bulk mode for indices {}...", metric.indices());
                for (String index : ImmutableSet.copyOf(metric.indices())) {
                    stopBulk(index);
                }
            }
            logger.debug("shutting down...");
            client.close();
            logger.debug("shutting down completed");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            isShutdown = true;
        }
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    @Override
    public void suspend() {
        suspended = true;
    }

    @Override
    public void resume() {
        suspended = false;
    }

    @Override
    public BulkNodeClient newIndex(String index) throws IOException {
        return newIndex(index, null, null);
    }

    @Override
    public BulkNodeClient newIndex(String index, String type, InputStream settings, InputStream mappings) throws IOException {
        configHelper.reset();
        configHelper.setting(settings);
        configHelper.mapping(type, mappings);
        return newIndex(index, configHelper.settings(), configHelper.mappings());
    }

    @Override
    public BulkNodeClient newIndex(String index, Settings settings, Map<String, String> mappings) throws IOException {
        if (closed) {
            throw new ElasticsearchIllegalStateException("client is closed");
        }
        if (client == null) {
            logger.warn("no client for create index");
            return this;
        }
        if (index == null) {
            logger.warn("no index name given to create index");
            return this;
        }
        CreateIndexRequestBuilder createIndexRequestBuilder =
                new CreateIndexRequestBuilder(client.admin().indices()).setIndex(index);
        Settings concreteSettings;
        if (settings == null && getSettings() != null) {
            concreteSettings = getSettings();
        } else if (settings != null) {
            concreteSettings = settings;
        } else {
            concreteSettings = null;
        }
        if (concreteSettings != null) {
            logger.info("newIndex: settings = {}", concreteSettings.getAsMap());
            createIndexRequestBuilder.setSettings(concreteSettings);
        }
        if (mappings == null && getMappings() != null) {
            for (String type : getMappings().keySet()) {
                logger.info("newIndex: type = {} mappings = {}", type, getMappings().get(type));
                createIndexRequestBuilder.addMapping(type, getMappings().get(type));
            }
        } else if (mappings != null) {
            for (String type : mappings.keySet()) {
                logger.info("newIndex: type = {} mappings = {}", type, mappings.get(type));
                createIndexRequestBuilder.addMapping(type, mappings.get(type));
            }
        }
        CreateIndexResponse response = createIndexRequestBuilder.execute().actionGet();
        if (response.isAcknowledged()) {
            logger.info("index {} created", index);
        } else {
            throw new IOException("creation of index " + index + " not acknowledged");
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
        DeleteIndexRequestBuilder deleteIndexRequestBuilder =
                new DeleteIndexRequestBuilder(client.admin().indices(), index);
        deleteIndexRequestBuilder.execute().actionGet();
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

    public void setSettings(Settings settings) {
        configHelper.settings(settings);
    }

    public Settings getSettings() {
        return configHelper.settings();
    }

    public ImmutableSettings.Builder getSettingsBuilder() {
        return configHelper.settingsBuilder();
    }

    public void setting(InputStream in) throws IOException {
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

    public void mapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type, in);
    }

    public void mapping(String type, String mapping) throws IOException {
        configHelper.mapping(type, mapping);
    }

    public Map<String, String> getMappings() {
        return configHelper.mappings();
    }

}
