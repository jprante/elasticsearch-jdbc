/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.jdbc.strategy.simple;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.river.jdbc.RiverMouth;
import org.elasticsearch.river.jdbc.support.HealthMonitorThread;
import org.elasticsearch.river.jdbc.support.RiverContext;
import org.elasticsearch.river.jdbc.support.StructuredObject;

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

    private static final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverMouth.class.getName());

    private static final AtomicInteger outstandingBulkRequests = new AtomicInteger(0);
    private static final ThreadFactory THREAD_FACTORY_HEALTH = EsExecutors.daemonThreadFactory("jdbc-river-health");

    protected RiverContext context;
    protected String index;
    protected String type;
    protected String id;
    protected Client client;
    private int maxBulkActions = 100;
    private int maxConcurrentBulkRequests = 30;
    private boolean versioning = false;
    private boolean acknowledge = false;
    private volatile boolean error;
    private BulkProcessor bulk;
    private HealthMonitorThread healthThread;

    private final BulkProcessor.Listener listener = new BulkProcessor.Listener() {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            long l = outstandingBulkRequests.incrementAndGet();
            logger.info("new bulk [{}] of [{} items], {} outstanding bulk requests",
                    executionId, request.numberOfActions(), l);
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if(acknowledge()){
	        	try {
					context.riverSource().acknowledge(response);
				} catch (IOException e) {
					logger.error("bulk ["+executionId+"] acknowledge error", e);
				}
            }
        	outstandingBulkRequests.decrementAndGet();
            logger.info("bulk [{}] success [{} items] [{}ms]",
                    executionId, response.getItems().length, response.getTook().millis());
            
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            outstandingBulkRequests.decrementAndGet();
            logger.error("bulk [" + executionId + "] error", failure);
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
                .setBulkActions(maxBulkActions-1)
                .setConcurrentRequests(maxConcurrentBulkRequests)
                .setFlushInterval(TimeValue.timeValueSeconds(1))
                .build();

        //Monitor cluster health in separate thread
        healthThread = new HealthMonitorThread(client);
        THREAD_FACTORY_HEALTH.newThread(healthThread).start();

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
    public SimpleRiverMouth versioning(boolean enable) {
        this.versioning = enable;
        return this;
    }

    @Override
    public boolean versioning() {
        return versioning;
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
        if (!checkStatus()) {
            return;
        }

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
        if (object.meta(StructuredObject.VERSION) != null && versioning) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(object.meta(StructuredObject.VERSION)));
        }
        if (object.meta(StructuredObject.ROUTING) != null) {
            request.routing(object.meta(StructuredObject.ROUTING));
        }
        if (object.meta(StructuredObject.PERCOLATE) != null) {
            request.percolate(object.meta(StructuredObject.PERCOLATE));
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
        if (!checkStatus()) {
            return;
        }

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
        if (object.meta(StructuredObject.VERSION) != null && versioning) {
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
        healthThread.stop();
    }

    @Override
    public void createIndexIfNotExists(String settings, String mapping) {
        if (!checkStatus()) {
            return;
        }

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

    /**
     * Checks the cluster health and error flag.
     * @return True if status is ok, false if action should be aborted.
     */
    private boolean checkStatus() {
        while (!healthThread.isHealthy()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                logger.warn("Thread interrupted while waiting to load", e);
                Thread.interrupted();
                return false;
            }
        }
        return !error;
    }

}
