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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.jdbc.support.LocaleUtil;
import org.elasticsearch.river.jdbc.support.RiverContext;
import org.elasticsearch.river.jdbc.support.RiverServiceLoader;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The JDBC river
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class JDBCRiver extends AbstractRiverComponent implements River {

    public final static String NAME = "jdbc-river";
    public final static String TYPE = "jdbc";

    private final String strategy;
    private final TimeValue poll;
    private final String url;
    private final String driver;
    private final String user;
    private final String password;
    private final String rounding;
    private final int scale;
    private final String sql;
    private final List<? super Object> sqlparams;
    private final String acksql;
    private final List<? super Object> acksqlparams;
    private final boolean autocommit;
    private final int fetchsize;
    private final int maxrows;
    private final int maxretries;
    private final TimeValue maxretrywait;
    private final String locale;
    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    private final int maxBulkRequests;
    private final String indexSettings;
    private final String typeMapping;
    private final boolean versioning;
    private final boolean digesting;
    private final boolean acknowledgeBulk;
    private final RiverSource riverSource;
    private final RiverMouth riverMouth;
    private final RiverContext riverContext;
    private final RiverFlow riverFlow;
    private volatile Thread thread;
    private volatile boolean closed;

    @Inject
    public JDBCRiver(RiverName riverName, RiverSettings riverSettings,
                     @RiverIndexName String riverIndexName,
                     Client client) {
        super(riverName, riverSettings);
        // riverIndexName = _river

        Map<String, Object> sourceSettings =
                riverSettings.settings().containsKey(TYPE)
                        ? (Map<String, Object>) riverSettings.settings().get(TYPE)
                        : new HashMap<String, Object>();
        // default is a single run
        strategy = XContentMapValues.nodeStringValue(sourceSettings.get("strategy"), "oneshot");
        url = XContentMapValues.nodeStringValue(sourceSettings.get("url"), null);
        driver = XContentMapValues.nodeStringValue(sourceSettings.get("driver"), null);
        user = XContentMapValues.nodeStringValue(sourceSettings.get("user"), null);
        password = XContentMapValues.nodeStringValue(sourceSettings.get("password"), null);
        poll = XContentMapValues.nodeTimeValue(sourceSettings.get("poll"), TimeValue.timeValueMinutes(60));
        sql = XContentMapValues.nodeStringValue(sourceSettings.get("sql"), null);
        sqlparams = XContentMapValues.extractRawValues("sqlparams", sourceSettings);
        rounding = XContentMapValues.nodeStringValue(sourceSettings.get("rounding"), null);
        scale = XContentMapValues.nodeIntegerValue(sourceSettings.get("scale"), 0);
        autocommit = XContentMapValues.nodeBooleanValue(sourceSettings.get("autocommit"), Boolean.FALSE);
        fetchsize = url.startsWith("jdbc:mysql") ? Integer.MIN_VALUE :
                XContentMapValues.nodeIntegerValue(sourceSettings.get("fetchsize"), 10);
        maxrows = XContentMapValues.nodeIntegerValue(sourceSettings.get("max_rows"), 0);
        maxretries = XContentMapValues.nodeIntegerValue(sourceSettings.get("max_retries"), 3);
        maxretrywait = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(sourceSettings.get("max_retries_wait"), "10s"), TimeValue.timeValueMillis(30000));
        locale = XContentMapValues.nodeStringValue(sourceSettings.get("locale"), LocaleUtil.fromLocale(Locale.getDefault()));
        digesting = XContentMapValues.nodeBooleanValue(sourceSettings.get("digesting"), Boolean.TRUE);
        acksql = XContentMapValues.nodeStringValue(sourceSettings.get("acksql"), null);
        acksqlparams = XContentMapValues.extractRawValues("acksqlparams", sourceSettings);

        Map<String, Object> targetSettings =
                riverSettings.settings().containsKey("index")
                        ? (Map<String, Object>) riverSettings.settings().get("index")
                        : new HashMap<String, Object>();
        indexName = XContentMapValues.nodeStringValue(targetSettings.get("index"), TYPE);
        typeName = XContentMapValues.nodeStringValue(targetSettings.get("type"), TYPE);
        bulkSize = XContentMapValues.nodeIntegerValue(targetSettings.get("bulk_size"), 100);
        maxBulkRequests = XContentMapValues.nodeIntegerValue(targetSettings.get("max_bulk_requests"), 30);
        indexSettings = XContentMapValues.nodeStringValue(targetSettings.get("index_settings"), null);
        typeMapping = XContentMapValues.nodeStringValue(targetSettings.get("type_mapping"), null);
        versioning = XContentMapValues.nodeBooleanValue(sourceSettings.get("versioning"), Boolean.FALSE);
        acknowledgeBulk = XContentMapValues.nodeBooleanValue(sourceSettings.get("acknowledge"), Boolean.FALSE);

        riverSource = RiverServiceLoader.findRiverSource(strategy);
        logger.debug("found river source {} for strategy {}", riverSource.getClass().getName(), strategy);
        riverSource.driver(driver)
                .url(url)
                .user(user)
                .password(password)
                .rounding(rounding)
                .precision(scale);

        riverMouth = RiverServiceLoader.findRiverMouth(strategy);
        logger.debug("found river target {} for strategy {}", riverMouth.getClass().getName(), strategy);
        riverMouth.index(indexName)
                .type(typeName)
                .maxBulkActions(bulkSize)
                .maxConcurrentBulkRequests(maxBulkRequests)
                .acknowledge(acknowledgeBulk)
                .versioning(versioning)
                .client(client);

        // scripting ...

        riverContext = new RiverContext()
                .riverName(riverName.getName())
                .riverIndexName(riverIndexName)
                .riverSettings(riverSettings.settings())
                .riverSource(riverSource)
                .riverTarget(riverMouth)
                .pollInterval(poll)
                .pollStatement(sql)
                .pollStatementParams(sqlparams)
                .pollAckStatement(acksql)
                .pollAckStatementParams(acksqlparams)
                .autocommit(autocommit)
                .maxRows(maxrows)
                .fetchSize(fetchsize)
                .retries(maxretries)
                .maxRetryWait(maxretrywait)
                .locale(locale)
                .digesting(digesting)
                .contextualize();

        riverFlow = RiverServiceLoader.findRiverFlow(strategy);
        // prepare task for run
        riverFlow.riverContext(riverContext);

        logger.debug("found river task {} for strategy {}", riverFlow.getClass().getName(), strategy);
    }

    @Override
    public void start() {
        logger.info("starting JDBC river: URL [{}], driver [{}], strategy [{}], index [{}]/[{}]",
                url, driver, strategy, indexName, typeName);
        try {
            riverFlow.startDate(new Date());
            riverMouth.createIndexIfNotExists(indexSettings, typeMapping);
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                riverFlow.startDate(null);
                // that's fine, continue.
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
            } else {
                logger.warn("failed to create index [{}], disabling JDBC river...", e, indexName);
                return;
            }
        }
        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "JDBC river [" + riverName.name() + '/' + strategy + ']')
                .newThread(riverFlow);
        thread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing JDBC river [" + riverName.name() + '/' + strategy + ']');
        if (thread != null) {
            thread.interrupt();
        }
        if (riverFlow != null) {
            riverFlow.abort();
        }
        if (riverSource != null) {
            riverSource.closeReading();
            riverSource.closeWriting();
        }
        if (riverMouth != null) {
            riverMouth.close();
        }
        closed = true; // abort only once
    }

    /**
     * Induce a river run once, but in a synchronous manner.
     */
    public void once() {
        if (riverFlow != null) {
            riverFlow.move();
        }
    }

    /**
     * Induce a river run once, but in an asynchronous manner.
     */
    public void induce() {
        RiverFlow riverTask = RiverServiceLoader.findRiverFlow(strategy);
        // prepare task for run
        riverTask.riverContext(riverContext);
        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "JDBC river (fired) [" + riverName.name() + '/' + strategy + ')')
                .newThread(riverTask);
        riverTask.abort();
        thread.start(); // once
    }

}
