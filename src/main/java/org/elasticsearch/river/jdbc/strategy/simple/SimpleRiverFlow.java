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

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.jdbc.RiverFlow;
import org.elasticsearch.river.jdbc.RiverMouth;
import org.elasticsearch.river.jdbc.RiverSource;
import org.elasticsearch.river.jdbc.support.RiverContext;
import org.elasticsearch.river.jdbc.support.StructuredObject;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

/**
 * A river flow implementation for the 'simple' strategy.
 * <p/>
 * This river flow runs fetch actions in a loop and waits before the next cycle begins.
 * <p/>
 * A version counter is incremented each time a fetch move is executed.
 * <p/>
 * A checksum is computed over the fetched data. If it differs between runs, it is assumed
 * the data has changed, and a houskeeper is run to clean up the documents which have
 * a smaller version than the river state.
 * <p/>
 * The state of the river flow is saved between runs. So, in case of a restart, the
 * river flow will recover with the last known state of the river.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class SimpleRiverFlow implements RiverFlow {

    private final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverFlow.class.getName());
    protected RiverContext context;
    protected Date startDate;
    protected boolean abort = false;

    @Override
    public String strategy() {
        return "simple";
    }

    @Override
    public SimpleRiverFlow riverContext(RiverContext context) {
        this.context = context;
        return this;
    }

    @Override
    public RiverContext riverContext() {
        return context;
    }

    /**
     * Set a start date
     *
     * @param creationDate the creation date
     */
    @Override
    public SimpleRiverFlow startDate(Date creationDate) {
        this.startDate = creationDate;
        return this;
    }

    /**
     * Return the start date of the river task
     *
     * @return the creation date
     */
    @Override
    public Date startDate() {
        return startDate;
    }

    /**
     * Delay the connector for poll millis, and notify a reason.
     *
     * @param reason the reason for the dealy
     */
    @Override
    public SimpleRiverFlow delay(String reason) {
        TimeValue poll = context.pollingInterval();
        if (poll.millis() > 0L) {
            logger.info("{}, waiting {}", reason, poll);
            try {
                Thread.sleep(poll.millis());
            } catch (InterruptedException e) {
                logger.debug("Thread interrupted while waiting, stopping");
                abort();
            }
        }
        return this;
    }

    /**
     * Triggers flag to abort the connector down at next run.
     */
    @Override
    public void abort() {
        this.abort = true;
    }

    /**
     * The river task loop. Execute move, check if the task must be aborted, continue with next run after a delay.
     */
    @Override
    public void run() {
        while (!abort) {
            move();
            if (abort) {
                return;
            }
            delay("next run");
        }
    }

    /**
     * A single river move.
     */
    @Override
    public void move() {
        try {
            RiverSource source = context.riverSource();
            RiverMouth target = context.riverMouth();
            Client client = context.riverMouth().client();
            Number version;
            String digest;
            GetResponse get = null;
            try {
                // read state from _custom
                client.admin().indices().prepareRefresh(context.riverIndexName()).execute().actionGet();
                get = client.prepareGet(context.riverIndexName(), context.riverName(), ID_INFO_RIVER_INDEX).execute().actionGet();
            } catch (IndexMissingException e) {
                logger.warn("river state missing: {}/{}/{}", context.riverIndexName(), context.riverName(), ID_INFO_RIVER_INDEX);
            }
            if (get != null && get.exists()) {
                Map jdbcState = (Map) get.sourceAsMap().get("jdbc");
                if (jdbcState != null) {
                    version = (Number) jdbcState.get("version");
                    version = version.longValue() + 1; // increase to next version
                    digest = (String) jdbcState.get("digest");
                } else {
                    throw new IOException("can't retrieve previously persisted state from " + context.riverIndexName() + "/" + context.riverName());
                }
            } else {
                version = 1L;
                digest = null;
            }
            // set default job name to current version number
            context.job(Long.toString(version.longValue()));
            String mergeDigest = source.fetch();
            // this end is required before house keeping starts
            target.flush();
            // save state to _custom
            XContentBuilder builder = jsonBuilder();
            builder.startObject().startObject("jdbc");
            if (startDate != null) {
                builder.field("created", startDate);
            }
            builder.field("version", version.longValue());
            builder.field("digest", mergeDigest);
            builder.endObject().endObject();
            if (logger.isDebugEnabled()) {
                logger.debug(builder.string());
            }
            client.prepareBulk().add(indexRequest(context.riverIndexName())
                    .type(context.riverName())
                    .id(ID_INFO_RIVER_INDEX)
                    .source(builder))
                    .execute().actionGet();
            // house keeping if data has changed
            if (digest != null && mergeDigest != null && !mergeDigest.equals(digest)) {
                versionHouseKeeping(version.longValue());
                // perform outstanding versionHouseKeeping bulk requests
                target.flush();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            abort = true;
        }
    }

    /**
     * Do the house keeping for a specific version in the index
     *
     * @param version the version
     * @throws IOException
     */
    protected void versionHouseKeeping(long version) throws IOException {
        logger.info("housekeeping for version " + version);
        Client client = context.riverMouth().client();
        String indexName = context.riverMouth().index();
        String typeName = context.riverMouth().type();
        int bulkSize = context.riverMouth().maxBulkActions();
        client.admin().indices().prepareRefresh(indexName).execute().actionGet();
        SearchResponse response = client.prepareSearch().setIndices(indexName).setTypes(typeName).setSearchType(SearchType.SCAN)
                .setScroll(TimeValue.timeValueMinutes(10)).setSize(bulkSize).setVersion(true).setQuery(matchAllQuery()).execute().actionGet();
        if (response.timedOut()) {
            logger.error("housekeeper scan query timeout");
            return;
        }
        if (response.failedShards() > 0) {
            logger.error("housekeeper failed shards in scan response: {0}", response.failedShards());
            return;
        }
        String scrollId = response.getScrollId();
        if (scrollId == null) {
            logger.error("housekeeper failed, no scroll ID");
            return;
        }
        boolean done = false;
        // scroll
        long deleted = 0L;
        long t0 = System.currentTimeMillis();
        do {
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(TimeValue.timeValueMinutes(10)).execute().actionGet();
            if (response.timedOut()) {
                logger.error("housekeeper scroll query timeout");
                done = true;
            } else if (response.failedShards() > 0) {
                logger.error("housekeeper failed shards in scroll response: {}", response.failedShards());
                done = true;
            } else {
                // terminate scrolling?
                if (response.hits() == null) {
                    done = true;
                } else {
                    for (SearchHit hit : response.getHits().getHits()) {
                        // delete all documents with lower version
                        if (hit.getVersion() < version) {
                            context.riverMouth().delete(new StructuredObject()
                                    .index(hit.getIndex())
                                    .type(hit.getType())
                                    .id(hit.getId()));
                            deleted++;
                        }
                    }
                    scrollId = response.getScrollId();
                }
            }
            if (scrollId != null) {
                done = true;
            }
        } while (!done);
        long t1 = System.currentTimeMillis();
        logger.info("housekeeping done, {} documents deleted, took {} ms", deleted, t1 - t0);
    }
}
