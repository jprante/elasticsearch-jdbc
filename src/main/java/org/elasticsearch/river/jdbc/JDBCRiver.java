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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import static org.elasticsearch.client.Requests.indexRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.SearchHit;

public class JDBCRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final String riverIndexName;
    private final String indexName;
    private final String typeName;
    private final SQLService service;
    private final BulkOperation operation;
    private final int bulkSize;
    private final int maxBulkRequests;
    private final TimeValue bulkTimeout;
    private final TimeValue poll;
    private final TimeValue interval;
    private final String url;
    private final String driver;
    private final String user;
    private final String password;
    private final String sql;
    private final String aliasDateField; // use to order request by date and select recent results
    private final int fetchsize;
    private final List<Object> params;
    private final boolean rivertable;
    private final boolean versioning;
    private final String rounding;
    private final int scale;
    private volatile Thread thread;
    private volatile boolean closed;
    private Date creationDate;
    private String strategy;
    protected Runnable riverStrategy;
    protected boolean delay = true; // if thread is launch infinite times. use to test thread only one time

    @Inject
    public JDBCRiver(RiverName riverName, RiverSettings settings,
                     @RiverIndexName String riverIndexName, Client client) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;
        if (settings.settings().containsKey("jdbc")) {
            Map<String, Object> jdbcSettings = (Map<String, Object>) settings.settings().get("jdbc");
            poll = XContentMapValues.nodeTimeValue(jdbcSettings.get("poll"), TimeValue.timeValueMinutes(60));
            url = XContentMapValues.nodeStringValue(jdbcSettings.get("url"), null);
            driver = XContentMapValues.nodeStringValue(jdbcSettings.get("driver"), null);
            user = XContentMapValues.nodeStringValue(jdbcSettings.get("user"), null);
            password = XContentMapValues.nodeStringValue(jdbcSettings.get("password"), null);
            sql = XContentMapValues.nodeStringValue(jdbcSettings.get("sql"), null);
            aliasDateField = XContentMapValues.nodeStringValue(jdbcSettings.get("aliasDateField"), null);
            strategy = XContentMapValues.nodeStringValue(jdbcSettings.get("strategy"), null);
            fetchsize = XContentMapValues.nodeIntegerValue(jdbcSettings.get("fetchsize"), 0);
            params = XContentMapValues.extractRawValues("params", jdbcSettings);
            rivertable = XContentMapValues.nodeBooleanValue(jdbcSettings.get("rivertable"), false);
            interval = XContentMapValues.nodeTimeValue(jdbcSettings.get("interval"), TimeValue.timeValueMinutes(60));
            versioning = XContentMapValues.nodeBooleanValue(jdbcSettings.get("versioning"), true);
            rounding = XContentMapValues.nodeStringValue(jdbcSettings.get("rounding"), null);
            scale = XContentMapValues.nodeIntegerValue(jdbcSettings.get("scale"), 0);
        } else {
            poll = TimeValue.timeValueMinutes(60);
            url = null;
            driver = null;
            user = null;
            password = null;
            sql = null;
            aliasDateField = null;
            strategy = null;
            fetchsize = 0;
            params = null;
            rivertable = false;
            interval = TimeValue.timeValueMinutes(60);
            versioning = true;
            rounding = null;
            scale = 0;
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
        } else {
            indexName = "jdbc";
            typeName = "jdbc";
            bulkSize = 100;
            maxBulkRequests = 30;
            bulkTimeout = TimeValue.timeValueMillis(60000);
        }
        service = new SQLService(logger);
        operation = new BulkOperation(client, logger).setIndex(indexName).setType(typeName)
                .setBulkSize(bulkSize).setMaxActiveRequests(maxBulkRequests)
                .setMillisBeforeContinue(bulkTimeout.millis()).setMillisBeforeContinue(bulkTimeout.millis()).setAcknowledge(riverName.getName(), rivertable ? service : null);

        // Strategy
        riverStrategy = new JDBCConnector();
        if(strategy!=null && strategy.equals("timebasis")){
            riverStrategy = new JDBCConnectorTimebasis();
        }
        if(strategy!=null && strategy.equals("rivertable")){
            riverStrategy = new JDBCRiverTableConnector();
        }

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
        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "JDBC connector").newThread(riverStrategy);
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

    private class JDBCConnectorTimebasis implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    /* Test of differents parameters. The field _id is mandatory (indicate the id in the index) */
                    /* Mandatory : aliasDateField must be in select sql request */
                    if(!sql.contains("_id") || aliasDateField == null || !sql.contains(aliasDateField)){
                        // Error
                        throw new Exception("Field name is mandatory in select clause");
                    }

                    // 2nd version with subselect : sql * from (select a,b,c from aa,bb,cc ...) where date >= aliasDateField order by aliasDateField asc, _id asc
                    String requestSQL = "select * from (" + sql + ") ";

                    String lastModificationDate = getPreviousDateModification();

                    /* Add modification date filter */
                    if(lastModificationDate!=null){
                        requestSQL+= " where \"" + aliasDateField + "\" >=?";
                    }
                    /* Add the order instruction : id and modification date */
                    requestSQL += " order by \"" + aliasDateField + "\" asc, \"_id\" asc";
                    logger.info("Requete SQL : " + requestSQL);
                    String indexOperation = sql.contains("_operation") ? null : "index";

                    Connection connection = service.getConnection(driver, url, user, password, true);
                    PreparedStatement statement = service.prepareStatement(connection, requestSQL);
                    service.bind(statement, params);
                    // Si le lastDateModification!=null, on bind la date
                    if(lastModificationDate!=null){
                        List<Object> list = new ArrayList<Object>();
                        list.add(Timestamp.valueOf(lastModificationDate));
                        service.bind(statement, list);
                    }

                    lastModificationDate = service.treat(statement, fetchsize,indexOperation, aliasDateField,operation);
                    service.close(statement);
                    service.close(connection);

                    saveContexteInIndex(lastModificationDate);

                    if(delay){
                        delay("next run");
                    }
                    else{
                        closed = true;
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e, (Object) null);
                    closed = true;
                }
                if (closed) {
                    return;
                }
            }
        }

        /**
         * The modification date of the last index document
         * @return
         */
        private String getPreviousDateModification()throws Exception{
            client.admin().indices().prepareRefresh(riverIndexName).execute().actionGet();
            GetResponse get = client.prepareGet(riverIndexName, riverName().name(), "_custom").execute().actionGet();
            if (!get.exists()) {
                return null;
            } else {
                Map<String, Object> jdbcState = (Map<String, Object>) get.sourceAsMap().get("jdbc");
                if (jdbcState != null) {
                    return (String)jdbcState.get("lastDateModification");
                } else {
                    throw new IOException("can't retrieve previously persisted state from " + riverIndexName + "/" + riverName().name());
                }
            }
        }

        private void saveContexteInIndex(String lastDateModification)throws Exception{
            XContentBuilder builder = jsonBuilder();
            builder.startObject().startObject("jdbc");
            if (lastDateModification != null) {
                builder.field("lastDateModification", lastDateModification);
            }
            builder.endObject().endObject();
            client.prepareIndex(riverIndexName,riverName.name(),"_custom").setSource(builder).execute().actionGet();
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
}
