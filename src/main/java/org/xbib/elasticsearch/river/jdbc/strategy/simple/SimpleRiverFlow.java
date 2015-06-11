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
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverName;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateRequestBuilder;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateResponse;
import org.xbib.elasticsearch.action.plugin.jdbc.state.post.PostRiverStateRequestBuilder;
import org.xbib.elasticsearch.action.plugin.jdbc.state.post.PostRiverStateResponse;
import org.xbib.elasticsearch.action.plugin.jdbc.state.put.PutRiverStateRequestBuilder;
import org.xbib.elasticsearch.action.plugin.jdbc.state.put.PutRiverStateResponse;
import org.xbib.elasticsearch.plugin.jdbc.client.IngestFactory;
import org.xbib.elasticsearch.plugin.jdbc.client.Metric;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.plugin.jdbc.util.DurationFormatUtil;
import org.xbib.elasticsearch.plugin.jdbc.util.LocaleUtil;
import org.xbib.elasticsearch.plugin.jdbc.util.RiverServiceLoader;
import org.xbib.elasticsearch.plugin.jdbc.util.SQLCommand;
import org.xbib.elasticsearch.plugin.jdbc.util.VolumeFormatUtil;
import org.xbib.elasticsearch.river.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.TimeZone;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Simple river flow implementation
 */
public class SimpleRiverFlow<RC extends RiverContext> implements RiverFlow<RC> {

    private final static ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.SimpleRiverFlow");

    private final static ESLogger metricsLogger = ESLoggerFactory.getLogger("river.jdbc.RiverMetrics");

    private RiverName riverName;

    private RC riverContext;

    private Settings settings;

    private IngestFactory ingestFactory;

    private Client client;

    private Queue<RiverContext> queue;

    private MeterMetric meterMetric;

    @Override
    public String strategy() {
        return "simple";
    }

    @Override
    public SimpleRiverFlow<RC> newInstance() {
        return new SimpleRiverFlow<RC>();
    }

    @Override
    public RC newRiverContext() {
        this.riverContext = (RC) new SimpleRiverContext();
        return riverContext;
    }

    @Override
    public RiverFlow setRiverName(RiverName riverName) {
        this.riverName = riverName;
        return this;
    }

    @Override
    public RiverName getRiverName() {
        return riverName;
    }

    @Override
    public RiverFlow setSettings(Settings settings) {
        this.settings = settings;
        return this;
    }

    @Override
    public Settings getSettings() {
        return settings;
    }

    @Override
    public boolean isMetricThreadEnabled() {
        return settings.getAsBoolean("metrics", false);
    }

    @Override
    public boolean isSuspensionThreadEnabled() {
        return settings.getAsBoolean("suspension", false);
    }

    @Override
    public RiverFlow setIngestFactory(IngestFactory ingestFactory) {
        this.ingestFactory = ingestFactory;
        return this;
    }

    public RiverFlow setClient(Client client) {
        this.client = client;
        return this;
    }

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public void execute(RC riverContext) throws Exception {
        logger.debug("execute: {}/{} with context {}", riverName.getName(), riverName.getType(), riverContext);
        try {
            beforeFetch(riverContext);
            fetch(riverContext);
        } finally {
            afterFetch(riverContext);
        }
    }

    /**
     * Before a river task (or river run) starts, this method is called.
     */
    protected void beforeFetch(RC riverContext) throws Exception {
        logger.debug("before fetch: getting river state for {}/{}", riverName.getName(), riverName.getType());
        GetRiverStateRequestBuilder riverStateRequestBuilder = new GetRiverStateRequestBuilder(client.admin().cluster())
                .setRiverName(riverName.getName())
                .setRiverType(riverName.getType());
        GetRiverStateResponse riverStateResponse = riverStateRequestBuilder.execute().actionGet();
        RiverState riverState = riverStateResponse.getRiverState();
        // if river state was not defined yet, define it now
        if (riverState == null) {
            logger.debug("river state not found, creating new state");
            riverState = new RiverState()
                    .setName(riverName.getName())
                    .setType(riverName.getType())
                    .setStarted(new DateTime());
            PutRiverStateRequestBuilder putRiverStateRequestBuilder = new PutRiverStateRequestBuilder(client.admin().cluster())
                    .setRiverName(riverName.getName())
                    .setRiverType(riverName.getType())
                    .setRiverState(riverState);
            PutRiverStateResponse putRiverStateResponse = putRiverStateRequestBuilder.execute().actionGet();
            if (putRiverStateResponse.isAcknowledged()) {
                logger.debug("before fetch: put initial state {}", riverState);
            } else {
                logger.error("befor fetch: initial state not acknowledged", riverState);
            }
        }
        RiverSource riverSource = createRiverSource(riverContext.getDefinition());
        RiverMouth riverMouth = createRiverMouth(riverContext.getDefinition());
        riverContext = fillRiverContext(riverContext, riverState, riverSource, riverMouth);
        logger.debug("before fetch: created source = {}, mouth = {}, context = {}",
                riverSource, riverMouth, riverContext);
        Integer counter = riverState.getCounter() + 1;
        DateTime currentTime = riverContext.getRiverState().getCurrentActiveBegin();
        riverContext.setRiverState(riverState.setCounter(counter)
                .setLastActive(currentTime != null ? currentTime : new DateTime(0), null)
                .setCurrentActive(new DateTime()));
        PostRiverStateRequestBuilder postRiverStateRequestBuilder = new PostRiverStateRequestBuilder(client.admin().cluster())
                .setRiverName(riverName.getName())
                .setRiverType(riverName.getType())
                .setRiverState(riverContext.getRiverState());
        PostRiverStateResponse postRiverStateResponse = postRiverStateRequestBuilder.execute().actionGet();
        if (!postRiverStateResponse.isAcknowledged()) {
            logger.warn("post river state not acknowledged: {}/{}", riverName.getName(), riverName.getType());
        }
        logger.debug("before fetch: state posted = {}", riverState);
        // call river source "before fetch"
        try {
            riverContext.getRiverSource().beforeFetch();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        // call river mouth "before fetch"
        try {
            riverContext.getRiverMouth().beforeFetch();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * After river context and state setup, when data should be fetched from river source, this method is called.
     * The default is to invoke the fetch() method of the river source.
     *
     * @throws Exception
     */
    protected void fetch(RiverContext riverContext) throws Exception {
        riverContext.getRiverSource().fetch();
    }

    @Override
    public RiverFlow setMetric(MeterMetric meterMetric) {
        this.meterMetric = meterMetric;
        return this;
    }

    @Override
    public MeterMetric getMetric() {
        return meterMetric;
    }


    @Override
    public RiverFlow setQueue(Queue<RiverContext> queue) {
        this.queue = queue;
        return this;
    }

    @Override
    public Queue<RiverContext> getQueue() {
        return queue;
    }

    /**
     * After the river task has completed a single run, this method is called.
     */
    protected void afterFetch(RiverContext riverContext) throws Exception {
        if (riverContext == null) {
            return;
        }
        if (riverContext.getRiverMouth() == null) {
            logger.warn("no river mouth");
            return;
        }
        // set activity
        RiverState riverState = riverContext.getRiverState()
                .setLastActive(riverContext.getRiverState().getCurrentActiveBegin(), new DateTime());
        PostRiverStateRequestBuilder postRiverStateRequestBuilder = new PostRiverStateRequestBuilder(client.admin().cluster())
                .setRiverName(riverName.getName())
                .setRiverType(riverName.getType())
                .setRiverState(riverState);
        PostRiverStateResponse postRiverStateResponse = postRiverStateRequestBuilder.execute().actionGet();
        if (!postRiverStateResponse.isAcknowledged()) {
            logger.warn("post river state not acknowledged: {}/{}", riverName.getName(), riverName.getType());
        }
        logger.debug("after fetch: state posted = {}", riverState);
        try {
            riverContext.getRiverMouth().afterFetch();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        try {
            riverContext.getRiverSource().afterFetch();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    protected RiverSource createRiverSource(Map<String, Object> params) {
        RiverSource riverSource = RiverServiceLoader.newRiverSource(strategy());
        logger.debug("found river source class {}, params = {}", riverSource, strategy(), params);
        String url = XContentMapValues.nodeStringValue(params.get("url"), null);
        String user = XContentMapValues.nodeStringValue(params.get("user"), null);
        String password = XContentMapValues.nodeStringValue(params.get("password"), null);
        String locale = XContentMapValues.nodeStringValue(params.get("locale"), LocaleUtil.fromLocale(Locale.getDefault()));
        String timezone = XContentMapValues.nodeStringValue(params.get("timezone"), TimeZone.getDefault().getID());
        riverSource.setUrl(url)
                .setUser(user)
                .setPassword(password)
                .setLocale(LocaleUtil.toLocale(locale))
                .setTimeZone(TimeZone.getTimeZone(timezone));
        return riverSource;
    }

    protected RiverMouth createRiverMouth(Map<String, Object> params) throws IOException {
        RiverMouth riverMouth = RiverServiceLoader.newRiverMouth(strategy());
        logger.debug("found river mouth class {}, params = {}", riverMouth, strategy(), params);
        String index = XContentMapValues.nodeStringValue(params.get("index"), "jdbc");
        String type = XContentMapValues.nodeStringValue(params.get("type"), "jdbc");
        riverMouth.setIndex(index)
                .setType(type)
                .setIngestFactory(ingestFactory);
        if (params.get("index_settings") != null) {
            Map<String, String> loadedSettings = new JsonSettingsLoader()
                    .load(jsonBuilder().map((Map<String, Object>) params.get("index_settings")).string());
            riverMouth.setIndexSettings(settingsBuilder().put(loadedSettings).build());
        }
        if (params.get("type_mapping") != null) {
            XContentBuilder builder = jsonBuilder().map((Map<String, Object>) params.get("type_mapping"));
            riverMouth.setTypeMapping(Collections.singletonMap(type, builder.string()));
        }
        return riverMouth;
    }

    protected RC fillRiverContext(RC riverContext, RiverState state,
                                  RiverSource riverSource,
                                  RiverMouth riverMouth) throws IOException {
        Map<String, Object> params = riverContext.getDefinition();
        List<SQLCommand> sql = SQLCommand.parse(params);
        String rounding = XContentMapValues.nodeStringValue(params.get("rounding"), null);
        int scale = XContentMapValues.nodeIntegerValue(params.get("scale"), 2);
        boolean autocommit = XContentMapValues.nodeBooleanValue(params.get("autocommit"), false);
        int fetchsize = 10;
        String fetchSizeStr = XContentMapValues.nodeStringValue(params.get("fetchsize"), null);
        if ("min".equals(fetchSizeStr)) {
            fetchsize = Integer.MIN_VALUE; // for MySQL streaming mode
        } else if (fetchSizeStr != null) {
            try {
                fetchsize = Integer.parseInt(fetchSizeStr);
            } catch (Exception e) {
                // ignore unparseable
            }
        } else {
            // if MySQL, enable streaming mode hack by default
            String url = XContentMapValues.nodeStringValue(params.get("url"), null);
            if (url != null && url.startsWith("jdbc:mysql")) {
                fetchsize = Integer.MIN_VALUE; // for MySQL streaming mode
            }
        }
        int maxrows = XContentMapValues.nodeIntegerValue(params.get("max_rows"), 0);
        int maxretries = XContentMapValues.nodeIntegerValue(params.get("max_retries"), 3);
        TimeValue maxretrywait = XContentMapValues.nodeTimeValue(params.get("max_retries_wait"), TimeValue.timeValueSeconds(30));
        String resultSetType = XContentMapValues.nodeStringValue(params.get("resultset_type"), "TYPE_FORWARD_ONLY");
        String resultSetConcurrency = XContentMapValues.nodeStringValue(params.get("resultset_concurrency"), "CONCUR_UPDATABLE");
        boolean shouldIgnoreNull = XContentMapValues.nodeBooleanValue(params.get("ignore_null_values"), false);
        boolean shouldPrepareDatabaseMetadata = XContentMapValues.nodeBooleanValue(params.get("prepare_database_metadata"), false);
        boolean shouldPrepareResultSetMetadata = XContentMapValues.nodeBooleanValue(params.get("prepare_resultset_metadata"), false);
        Map<String, Object> columnNameMap = (Map<String, Object>) params.get("column_name_map");
        int queryTimeout = XContentMapValues.nodeIntegerValue(params.get("query_timeout"), 1800);
        Map<String, Object> connectionProperties = (Map<String, Object>) params.get("connection_properties");
        boolean shouldTreatBinaryAsString = XContentMapValues.nodeBooleanValue(params.get("treat_binary_as_string"), false);

        riverContext.setRiverState(state)
                .setRiverSource(riverSource)
                .setRiverMouth(riverMouth)
                .setMetric(meterMetric)
                .setRounding(rounding)
                .setScale(scale)
                .setStatements(sql)
                .setAutoCommit(autocommit)
                .setMaxRows(maxrows)
                .setFetchSize(fetchsize)
                .setRetries(maxretries)
                .setMaxRetryWait(maxretrywait)
                .setResultSetType(resultSetType)
                .setResultSetConcurrency(resultSetConcurrency)
                .shouldIgnoreNull(shouldIgnoreNull)
                .shouldPrepareDatabaseMetadata(shouldPrepareDatabaseMetadata)
                .shouldPrepareResultSetMetadata(shouldPrepareResultSetMetadata)
                .setColumnNameMap(columnNameMap)
                .setQueryTimeout(queryTimeout)
                .setConnectionProperties(connectionProperties)
                .shouldTreatBinaryAsString(shouldTreatBinaryAsString);
        riverSource.setRiverContext(riverContext);
        riverMouth.setRiverContext(riverContext);
        return riverContext;
    }

    @Override
    public void logMetrics(RC riverContext, String cause) {
        MeterMetric metric = getMetric();
        if (metric == null) {
            return;
        }
        if (riverContext == null || riverContext.getRiverMouth() == null) {
            return;
        }
        Metric mouthMetric = riverContext.getRiverMouth().getMetric();
        if (mouthMetric == null) {
            return;
        }
        long ticks = metric.count();
        double mean = metric.meanRate();
        double oneminute = metric.oneMinuteRate();
        double fiveminute = metric.fiveMinuteRate();
        double fifteenminute = metric.fifteenMinuteRate();
        long bytes = mouthMetric.getTotalIngestSizeInBytes().count();
        long elapsed = mouthMetric.elapsed() / 1000000;
        String elapsedhuman = DurationFormatUtil.formatDurationWords(elapsed, true, true);
        //double dps = ticks * 1000 / elapsed;
        double avg = bytes / (ticks + 1); // avoid div by zero
        double mbps = (bytes * 1000.0 / elapsed) / (1024.0 * 1024.0);
        NumberFormat formatter = NumberFormat.getNumberInstance();
        metricsLogger.info("{}: river {}/{} metrics: {} rows, {} mean, ({} {} {}), ingest metrics: elapsed {}, {} bytes, {} avg, {} MB/s",
                cause,
                riverName.getType(),
                riverName.getName(),
                ticks,
                mean,
                oneminute,
                fiveminute,
                fifteenminute,
                elapsedhuman,
                VolumeFormatUtil.convertFileSize(bytes),
                VolumeFormatUtil.convertFileSize(avg),
                formatter.format(mbps)
        );
    }

    public void shutdown() throws Exception {
        if (riverContext != null) {
            riverContext.shutdown();
        }
    }

}
