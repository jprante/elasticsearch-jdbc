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
package org.xbib.elasticsearch.plugin.jdbc.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Interface for providing convenient administrative methods for ingesting data into Elasticsearch.
 */
public interface Ingest {

    /**
     * Index document
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return this
     */
    Ingest index(String index, String type, String id, String source);

    /**
     * Delete document
     *
     * @param index the index
     * @param type  the type
     * @param id    the id
     * @return this
     */
    Ingest delete(String index, String type, String id);

    Ingest newClient(Client client) throws IOException;

    Ingest newClient(Settings settings) throws IOException;

    Client client();

    /**
     * Set the maximum number of actions per bulk request
     *
     * @param maxActions maximum number of bulk actions
     * @return this ingest
     */
    Ingest maxActionsPerBulkRequest(int maxActions);

    /**
     * Set the maximum concurent bulk requests
     *
     * @param maxConcurentBulkRequests maximum number of concurrent ingest requests
     * @return this Ingest
     */
    Ingest maxConcurrentBulkRequests(int maxConcurentBulkRequests);

    /**
     * Set the maximum volume for bulk request before flush
     *
     * @param maxVolume maximum volume
     * @return this ingest
     */
    Ingest maxVolumePerBulkRequest(ByteSizeValue maxVolume);

    /**
     * Set the flush interval for automatic flushing outstanding ingest requests
     *
     * @param flushInterval the flush interval, default is 30 seconds
     * @return this ingest
     */
    Ingest flushIngestInterval(TimeValue flushInterval);

    /**
     * The number of shards for index creation
     *
     * @param shards the number of shards
     * @return this
     */
    Ingest shards(int shards);

    /**
     * The number of replica for index creation
     *
     * @param replica the number of replica
     * @return this
     */
    Ingest replica(int replica);

    void setSettings(Settings settings);

    ImmutableSettings.Builder getSettingsBuilder();

    Settings getSettings();

    /**
     * Create settings
     *
     * @param in the input stream with settings
     */
    void setting(InputStream in) throws IOException;

    /**
     * Create a key/value in the settings
     *
     * @param key   the key
     * @param value the value
     */
    void addSetting(String key, String value);

    /**
     * Create a key/value in the settings
     *
     * @param key   the key
     * @param value the value
     */
    void addSetting(String key, Boolean value);

    /**
     * Create a key/value in the settings
     *
     * @param key   the key
     * @param value the value
     */
    void addSetting(String key, Integer value);

    void mapping(String type, InputStream in) throws IOException;

    void mapping(String type, String mapping) throws IOException;

    Map<String, String> getMappings();

    Ingest putMapping(String index);

    Ingest deleteMapping(String index, String type);

    /**
     * Create a new index
     *
     * @return this ingest
     */
    Ingest newIndex(String index) throws IOException;

    Ingest newIndex(String index, String type, InputStream settings, InputStream mappings) throws IOException;

    Ingest newIndex(String index, Settings settings, Map<String, String> mappings) throws IOException;

    /**
     * Delete index
     *
     * @return this ingest
     */
    Ingest deleteIndex(String index);

    /**
     * Start bulk mode
     *
     * @return this ingest
     */
    Ingest startBulk(String index, long startRefreshInterval, long stopRefreshInterval) throws IOException;

    /**
     * Stops bulk mode
     *
     * @return this Ingest
     */
    Ingest stopBulk(String index) throws IOException;

    /**
     * Bulked index request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     *
     * @param indexRequest the index request to add
     * @return this ingest
     */
    Ingest bulkIndex(IndexRequest indexRequest);

    /**
     * Bulked delete request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     *
     * @param deleteRequest the delete request to add
     * @return this ingest
     */
    Ingest bulkDelete(DeleteRequest deleteRequest);

    /**
     * Flush ingest, move all pending documents to the bulk indexer
     *
     * @return this
     */
    Ingest flushIngest();

    /**
     * Wait for all outstanding responses
     *
     * @param maxWait maximum wait time
     * @return this ingest
     * @throws InterruptedException
     */
    Ingest waitForResponses(TimeValue maxWait) throws InterruptedException;

    /**
     * Flush the index
     */
    Ingest flush(String index);

    /**
     * Refresh the index.
     *
     * @return this ingest
     */
    Ingest refresh(String index);

    /**
     * Add replica level.
     *
     * @param level the replica level
     * @return number of shards after updating replica level
     */
    int updateReplicaLevel(String index, int level) throws IOException;

    /**
     * Wait for cluster being healthy.
     *
     * @throws IOException
     */
    Ingest waitForCluster(ClusterHealthStatus status, TimeValue timeValue) throws IOException;

    /**
     * Wait for index recovery (after replica change)
     *
     * @return number of shards found
     */
    int waitForRecovery(String index) throws IOException;

    Metric getMetric();

    boolean hasThrowable();

    /**
     * Return last throwable if exists.
     *
     * @return last throwable
     */
    Throwable getThrowable();

    /**
     * Shutdown the ingesting
     */
    void shutdown();

    boolean isShutdown();

    void suspend();

    void resume();
}
