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
package org.elasticsearch.river.jdbc;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.VersionType;

/**
 * Send bulk data to Elasticsearch
 *
 * @author <a href="mailto:joergprante@gmail.com">J&ouml;rg Prante</a>
 */
public class BulkOperation implements MergeListener {

    private String index;
    private String type;
    private String id;
    private Client client;
    private ESLogger logger;
    private int bulkSize = 100;
    private int maxActiveRequests = 30;
    private long millisBeforeContinue = 60000L;
    private int totalTimeouts;
    private static final int MAX_TOTAL_TIMEOUTS = 10;
    private static final AtomicInteger onGoingBulks = new AtomicInteger(0);
    private static final AtomicInteger counter = new AtomicInteger(0);
    private ThreadLocal<BulkRequestBuilder> currentBulk = new ThreadLocal();

    public BulkOperation(Client client, ESLogger logger, String index, String type) {
        this.client = client;
        this.logger = logger;
        this.index = index;
        this.type = type;
        this.id = null;
        this.totalTimeouts = 0;
    }
    
    public String getIndex() {
        return index;
    }
    
    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }
    
    public BulkOperation setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
        return this;
    }

    public BulkOperation setMaxActiveRequests(int maxActiveRequests) {
        this.maxActiveRequests = maxActiveRequests;
        return this;
    }

    public BulkOperation setMillisBeforeContinue(long millis) {
        this.millisBeforeContinue = millis;
        return this;
    }

    @Override
    public void merged(String index, String type, String id, long version, XContentBuilder builder) {
        if (index != null) {
            this.index = index;
        }
        if (type != null) {
            this.type = type;
        }
        if (id != null) {
            this.id = id;
        }
        if (id == null) {
            return; // skip if ID is null
        }
        if (currentBulk.get() == null) {
            currentBulk.set(client.prepareBulk());
        }
        currentBulk.get().add(Requests.indexRequest(getIndex()).type(getType()).id(getId()).versionType(VersionType.EXTERNAL).version(version).source(builder));
        if (currentBulk.get().numberOfActions() >= bulkSize) {
            processBulk();
        }
    }
    
    public void delete(String index, String type, String id, long version) {
        if (index != null) {
            this.index = index;
        }
        if (type != null) {
            this.type = type;
        }
        if (id != null) {
            this.id = id;
        }
        if (id == null) {
            return; // skip
        }
        if (currentBulk.get() == null) {
            currentBulk.set(client.prepareBulk());
        }
        currentBulk.get().add(Requests.deleteRequest(getIndex()).type(getType()).id(getId()));
        if (currentBulk.get().numberOfActions() >= bulkSize) {
            processBulk();
        }        
    }
    
    public void flush() throws IOException {
        if (currentBulk.get() == null) {
            return;
        }
        if (totalTimeouts > MAX_TOTAL_TIMEOUTS) {
            // waiting some minutes is much too long, do not wait any longer            
            throw new IOException("total flush() timeouts exceeded limit of + " + MAX_TOTAL_TIMEOUTS + ", aborting");
        }
        if (currentBulk.get().numberOfActions() > 0) {
            processBulk();
        }
        // wait for all outstanding bulk requests
        while (onGoingBulks.intValue() > 0) {
            logger.info("waiting for {} active bulk requests", onGoingBulks);
            synchronized (onGoingBulks) {
                try {
                    onGoingBulks.wait(millisBeforeContinue);
                } catch (InterruptedException e) {
                    logger.warn("timeout while waiting, continuing after {} ms", millisBeforeContinue);
                    totalTimeouts++;
                }
            }
        }
    }

    private void processBulk() {
        while (onGoingBulks.intValue() >= maxActiveRequests) {
            logger.info("waiting for {} active bulk requests", onGoingBulks);
            synchronized (onGoingBulks) {
                try {
                    onGoingBulks.wait(millisBeforeContinue);
                } catch (InterruptedException e) {
                    logger.warn("timeout while waiting, continuing after {} ms", millisBeforeContinue);
                    totalTimeouts++;
                }
            }
        }
        int currentOnGoingBulks = onGoingBulks.incrementAndGet();
        final int numberOfActions = currentBulk.get().numberOfActions();
        logger.info("submitting new bulk request ({} docs, {} requests currently active)", new Object[]{numberOfActions, currentOnGoingBulks});
        try {
            currentBulk.get().execute(new ActionListener<BulkResponse>() {

                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    if (bulkResponse.hasFailures()) {
                        logger.error("bulk request has failures: {}", bulkResponse.buildFailureMessage());
                    } else {
                        final int totalActions = counter.addAndGet(numberOfActions);
                        logger.info("bulk request success ({} millis, {} docs, total of {} docs)", new Object[]{bulkResponse.tookInMillis(), numberOfActions, totalActions});
                    }
                    onGoingBulks.decrementAndGet();
                    synchronized (onGoingBulks) {
                        onGoingBulks.notifyAll();
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.error("bulk request failed", e);
                }
            });
        } catch (Exception e) {
            logger.error("unhandled exception, failed to execute bulk request", e);
        } finally {
            currentBulk.set(client.prepareBulk());
        }
    }

}
