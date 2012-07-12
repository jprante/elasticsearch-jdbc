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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.base.Objects;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class JDBCRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private volatile Thread thread;
    private final SQLService service;
    private final BulkOperation operation;

    // ~ Default Settings
    private final String riverIndexName;
    private String indexName = "jdbc";
    private String typeName = "jdbc";
    private String url;
    private String driver;
    private String user;
    private String password;
    private String sql;
    private List<Object> params;
    private int bulkSize = 100;
    private int maxBulkRequests = 30;
    private int fetchsize = 0;
    private TimeValue bulkTimeout = TimeValue.timeValueMillis(60000);
    private TimeValue poll = TimeValue.timeValueMinutes(60);
    private TimeValue interval = TimeValue.timeValueMinutes(60);
    private boolean rivertable = false;
    private boolean versioning = true;
    private String rounding;
    private int scale = 0;

    private Date creationDate;
    private volatile boolean closed;

    @Inject
    public JDBCRiver(RiverName riverName, RiverSettings settings,
                     @RiverIndexName String riverIndexName, Client client) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;
        // Define settings 
        if (settings.settings().containsKey("jdbc")) {
            Map<String, Object> jdbcSettings = (Map<String, Object>) settings.settings().get("jdbc");
            poll = XContentMapValues.nodeTimeValue(jdbcSettings.get("poll"), TimeValue.timeValueMinutes(60));
            url = XContentMapValues.nodeStringValue(jdbcSettings.get("url"), null);
            driver = XContentMapValues.nodeStringValue(jdbcSettings.get("driver"), null);
            user = XContentMapValues.nodeStringValue(jdbcSettings.get("user"), null);
            password = XContentMapValues.nodeStringValue(jdbcSettings.get("password"), null);
            sql = XContentMapValues.nodeStringValue(jdbcSettings.get("sql"), null);
            fetchsize = XContentMapValues.nodeIntegerValue(jdbcSettings.get("fetchsize"), 0);
            params = XContentMapValues.extractRawValues("params", jdbcSettings);
            rivertable = XContentMapValues.nodeBooleanValue(jdbcSettings.get("rivertable"), false);
            interval = XContentMapValues.nodeTimeValue(jdbcSettings.get("interval"), TimeValue.timeValueMinutes(60));
            versioning = XContentMapValues.nodeBooleanValue(jdbcSettings.get("versioning"), true);
            rounding = XContentMapValues.nodeStringValue(jdbcSettings.get("rounding"), null);
            scale = XContentMapValues.nodeIntegerValue(jdbcSettings.get("scale"), 0);
        }
        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), "jdbc");
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "jdbc");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            maxBulkRequests = XContentMapValues.nodeIntegerValue(indexSettings.get("max_bulk_requests"), 30);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "60s"), TimeValue.timeValueMillis(60000));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(60000);
            }
        }
        service = new SQLService(logger).setPrecision(scale).setRounding(rounding);
        operation = new BulkOperation(client, logger).setIndex(indexName).setType(typeName).setVersioning(versioning)
                .setBulkSize(bulkSize).setMaxActiveRequests(maxBulkRequests)
                .setMillisBeforeContinue(bulkTimeout.millis()).setAcknowledge(riverName.getName(), rivertable ? service : null);
    }

    @Override
    public void start() {
        logger.info("starting JDBC connector: URL [{}], driver [{}], sql [{}], river table [{}], indexing to [{}]/[{}], poll [{}]",
                url, driver, sql, rivertable, indexName, typeName, poll);
        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
            creationDate = new Date();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                creationDate = null;
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }
        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "JDBC connector").newThread(rivertable ? new JDBCRiverTableConnector() : new JDBCConnector());
        thread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing JDBC river");
        thread.interrupt();
        closed = true;
    }

    private class JDBCConnector implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    Number version;
                    String digest;
                    // read state from _custom
                    client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
                    GetResponse get = client.prepareGet(riverIndexName, riverName().name(), "_custom").execute().actionGet();
                    if (creationDate != null || !get.exists()) {
                        version = 1L;
                        digest = null;
                    } else {
                        Map<String, Object> jdbcState = (Map<String, Object>) get.sourceAsMap().get("jdbc");
                        if (jdbcState != null) {
                            version = (Number) jdbcState.get("version");
                            version = version.longValue() + 1; // increase to next version
                            digest = (String) jdbcState.get("digest");
                        } else {
                            throw new IOException("can't retrieve previously persisted state from " + riverIndexName + "/" + riverName().name());
                        }
                    }
                    Connection connection = service.getConnection(driver, url, user, password, true);
                    PreparedStatement statement = service.prepareStatement(connection, sql);
                    service.bind(statement, params);
                    ResultSet results = service.execute(statement, fetchsize);
                    Merger merger = new Merger(operation, version.longValue());
                    long rows = 0L;
                    while (service.nextRow(results, merger)) {
                        rows++;
                    }
                    merger.close();
                    service.close(results);
                    service.close(statement);
                    service.close(connection);
                    logger.info("got " + rows + " rows for version " + version.longValue() + ", digest = " + merger.getDigest());
                    // this flush is required before house keeping starts
                    operation.flush();
                    // save state to _custom
                    XContentBuilder builder = jsonBuilder();
                    builder.startObject().startObject("jdbc");
                    if (creationDate != null) {
                        builder.field("created", creationDate);
                    }
                    builder.field("version", version.longValue());
                    builder.field("digest", merger.getDigest());
                    builder.endObject().endObject();
                    client.prepareBulk().add(indexRequest(riverIndexName).type(riverName.name()).id("_custom").source(builder)).execute().actionGet();
                    // house keeping if data has changed
                    if (digest != null && !merger.getDigest().equals(digest)) {
                        housekeeper(version.longValue());
                        // perform outstanding housekeeper bulk requests
                        operation.flush();
                    }
                    delay("next run");
                } catch (Exception e) {
                    logger.error(e.getMessage(), e, (Object) null);
                    closed = true;
                }
                if (closed) {
                    return;
                }
            }
        }

        private void housekeeper(long version) throws IOException {
            logger.info("housekeeping for version " + version);
            client.admin().indices().prepareRefresh(indexName).execute().actionGet();
            SearchResponse response = client.prepareSearch().setIndices(indexName).setTypes(typeName).setSearchType(SearchType.SCAN).setScroll(TimeValue.timeValueMinutes(10)).setSize(bulkSize).setVersion(true).setQuery(matchAllQuery()).execute().actionGet();
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
                                operation.delete(hit.getIndex(), hit.getType(), hit.getId());
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
            logger.info("housekeeper ready, {} documents deleted, took {} ms", deleted, t1 - t0);
        }
    }

    private class JDBCRiverTableConnector implements Runnable {

        private String[] optypes = new String[]{"create", "index", "delete"};

        @Override
        public void run() {
            while (true) {
                for (String optype : optypes) {
                    try {
                        Connection connection = service.getConnection(driver, url, user, password, false);
                        PreparedStatement statement = service.prepareRiverTableStatement(connection, riverName.getName(), optype, interval.millis());
                        ResultSet results = service.execute(statement, fetchsize);
                        Merger merger = new Merger(operation);
                        long rows = 0L;
                        while (service.nextRiverTableRow(results, merger)) {
                            rows++;
                        }
                        merger.close();
                        service.close(results);
                        service.close(statement);
                        logger.info(optype + ": got " + rows + " rows");
                        // this flush is required before next run
                        operation.flush();
                        service.close(connection);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e, (Object) null);
                        closed = true;
                    }
                    if (closed) {
                        return;
                    }
                }
                delay("next run");
            }
        }
    }


    private void delay(String reason) {
        if (poll.millis() > 0L) {
            logger.info("{}, waiting {}, URL [{}] driver [{}] sql [{}] river table [{}]",
                    reason, poll, url, driver, sql, rivertable);
            try {
                Thread.sleep(poll.millis());
            } catch (InterruptedException e1) {
            }
        }
    }

    // ~ equals, hashCode, toString methods

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JDBCRiver jdbcRiver = (JDBCRiver) o;
        return Objects.equal(riverIndexName, jdbcRiver.riverIndexName)
                && Objects.equal(indexName, jdbcRiver.indexName)
                && Objects.equal(typeName, jdbcRiver.typeName)
                && Objects.equal(url, jdbcRiver.url)
                && Objects.equal(driver, jdbcRiver.driver)
                && Objects.equal(user, jdbcRiver.user)
                && Objects.equal(password, jdbcRiver.password)
                && Objects.equal(sql, jdbcRiver.sql)
                && Objects.equal(params, jdbcRiver.params)
                && Objects.equal(bulkSize, jdbcRiver.bulkSize)
                && Objects.equal(maxBulkRequests, jdbcRiver.maxBulkRequests)
                && Objects.equal(fetchsize, jdbcRiver.fetchsize)
                && Objects.equal(bulkTimeout, jdbcRiver.bulkTimeout)
                && Objects.equal(poll, jdbcRiver.poll)
                && Objects.equal(interval, jdbcRiver.interval)
                && Objects.equal(versioning, jdbcRiver.versioning)
                && Objects.equal(rounding, jdbcRiver.rounding)
                && Objects.equal(scale, jdbcRiver.scale)
                && Objects.equal(rivertable, jdbcRiver.rivertable);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(riverIndexName, indexName, typeName, url, driver, user, password, sql, params, bulkSize, maxBulkRequests, fetchsize,
                bulkTimeout, poll, interval, versioning, rounding, scale, rivertable);
    }


    @Override
    public String toString() {
        final String pwd = password != null ? password.replaceAll(".", "\\*") : null;
        return Objects.toStringHelper(this)
                .add("riverIndexName", riverIndexName)
                .add("indexName", indexName)
                .add("typeName", typeName)
                .add("url", url)
                .add("driver", driver)
                .add("user", user)
                .add("password", pwd)
                .add("sql", sql)
                .add("params", params)
                .add("bulkSize", bulkSize)
                .add("maxBulkRequests", maxBulkRequests)
                .add("fetchsize", fetchsize)
                .add("bulkTimeout", bulkTimeout)
                .add("poll", poll)
                .add("interval", interval)
                .add("versioning", versioning)
                .add("rounding", rounding)
                .add("scale", scale)
                .add("rivertable", rivertable)
                .toString();
    }

}
