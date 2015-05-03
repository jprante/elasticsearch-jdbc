/*
 * Copyright (C) 2015 Jörg Prante
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
package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.xbib.elasticsearch.common.util.LocaleUtil;
import org.xbib.elasticsearch.common.util.SourceMetric;
import org.xbib.elasticsearch.common.util.StrategyLoader;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.Sink;
import org.xbib.elasticsearch.common.util.SQLCommand;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.IngestFactory;
import org.xbib.elasticsearch.support.client.Metric;
import org.xbib.elasticsearch.support.client.transport.BulkTransportClient;
import org.xbib.tools.MetricsLogger;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * The context consists of the parameters that span source and sink settings.
 * It represents the state, for supporting the task execution, and scripting.
 */
public class StandardContext<S extends JDBCSource> implements Context<S, Sink> {

    private final static Logger logger = LogManager.getLogger("feeder.jdbc.context.standard");

    private int counter;

    private Settings settings;

    private IngestFactory ingestFactory;

    private S source;

    private Sink sink;

    private State state = State.IDLE;

    private DateTime dateOfThrowable;

    private Throwable throwable;

    private final static List<Future> futures = new LinkedList<>();

    private final static SourceMetric sourceMetric = new SourceMetric().start();

    private final static Metric sinkMetric = new Metric().start();

    @Override
    public String strategy() {
        return "standard";
    }

    @Override
    public StandardContext newInstance() {
        return new StandardContext();
    }

    @Override
    public StandardContext setCounter(int counter) {
        this.counter = counter;
        return this;
    }

    @Override
    public int getCounter() {
        return counter;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public StandardContext setSettings(Settings settings) {
        this.settings = settings;
        if (settings.getAsBoolean("metrics.enabled",false) && futures.isEmpty()) {
            Thread thread = new MetricsThread();
            ScheduledThreadPoolExecutor scheduledthreadPoolExecutor =
                    new ScheduledThreadPoolExecutor(1);
            futures.add(scheduledthreadPoolExecutor.scheduleAtFixedRate(thread, 0L,
                    settings.getAsTime("metrics.interval", TimeValue.timeValueSeconds(30)).seconds(), TimeUnit.SECONDS));
            logger.debug("metrics thread started");
        }
        return this;
    }

    @Override
    public Settings getSettings() {
        return settings;
    }

    @Override
    public StandardContext setIngestFactory(IngestFactory ingestFactory) {
        this.ingestFactory = ingestFactory;
        return this;
    }

    public IngestFactory getIngestFactory() {
        return ingestFactory;
    }

    @Override
    public StandardContext setSource(S source) {
        this.source = source;
        source.setMetric(sourceMetric);
        return this;
    }

    @Override
    public S getSource() {
        return source;
    }

    @Override
    public StandardContext setSink(Sink sink) {
        this.sink = sink;
        sink.setMetric(sinkMetric);
        return this;
    }

    @Override
    public Sink getSink() {
        return sink;
    }

    public StandardContext setThrowable(Throwable throwable) {
        this.throwable = throwable;
        this.dateOfThrowable = new DateTime();
        return this;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public DateTime getDateOfThrowable() {
        return dateOfThrowable;
    }

    @Override
    public void execute() throws Exception {
        try {
            state = State.BEFORE_FETCH;
            beforeFetch();
            state = State.FETCH;
            fetch();
        } finally {
            state = State.AFTER_FETCH;
            afterFetch();
            state = State.IDLE;
            counter++;
        }
    }

    @Override
    public void beforeFetch() throws Exception {
        logger.debug("before fetch");
        if (ingestFactory == null) {
            this.ingestFactory = createIngestFactory(settings);
        }
        Sink sink = createSink();
        S source = createSource();
        prepareContext(source, sink);
        logger.debug("before fetch: ingestfactory = {} source = {}, sink = {}",
                ingestFactory, source, sink);
        getSink().beforeFetch();
        getSource().beforeFetch();
    }

    @Override
    public void fetch() throws Exception {
        logger.debug("fetch");
        try {
            getSource().fetch();
        } catch (Throwable e) {
            setThrowable(e);
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void afterFetch() throws Exception {
        logger.debug("after fetch");
        try {
            getSource().afterFetch();
        } catch (Throwable e) {
            setThrowable(e);
            logger.error(e.getMessage(), e);
        }
        try {
            getSink().afterFetch();
        } catch (Throwable e) {
            setThrowable(e);
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void shutdown() throws IOException {
        for (Future future : futures) {
            future.cancel(true);
        }
        getSource().shutdown();
        getSink().shutdown();
    }

    @SuppressWarnings("unchecked")
    protected S createSource() {
        S source = (S) StrategyLoader.newSource(strategy());
        logger.info("found source class {}", source);
        String url = settings.get("url");
        String user = settings.get("user");
        String password = settings.get("password");
        String locale = settings.get("locale", LocaleUtil.fromLocale(Locale.getDefault()));
        String timezone = settings.get("timezone", TimeZone.getDefault().getID());
        source.setUrl(url)
                .setUser(user)
                .setPassword(password)
                .setLocale(LocaleUtil.toLocale(locale))
                .setTimeZone(TimeZone.getTimeZone(timezone));
        return source;
    }

    protected Sink createSink() throws IOException {
        Sink sink = StrategyLoader.newSink(strategy());
        logger.info("found sink class {}", sink);
        String index = settings.get("index", "jdbc");
        String type = settings.get("type", "jdbc");
        sink.setIndex(index).setType(type);
        if (settings.getAsStructuredMap().containsKey("index_settings")) {
            Settings loadedSettings = settings.getAsSettings("index_settings");
            sink.setIndexSettings(loadedSettings);
        }
        if (settings.getAsStructuredMap().containsKey("type_mapping")) {
            XContentBuilder builder = jsonBuilder()
                    .map(settings.getAsSettings("type_mapping").getAsStructuredMap());
            sink.setTypeMapping(Collections.singletonMap(type, builder.string()));
        }
        return sink;
    }

    protected void prepareContext(S source, Sink sink) throws IOException {
        Map<String, Object> params = settings.getAsStructuredMap();
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
        source.setRounding(rounding)
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
        setSource(source);
        setSink(sink);
        source.setContext(this);
        sink.setContext(this);
    }

    private IngestFactory createIngestFactory(final Settings settings) {
        return new IngestFactory() {
            @Override
            public Ingest create() throws IOException {
                Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
                Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2);
                ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m"));
                TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
                BulkTransportClient ingest = new BulkTransportClient();
                ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder()
                        .put("cluster.name", settings.get("elasticsearch.cluster", "elasticsearch"))
                        .putArray("host", settings.getAsArray("elasticsearch.host"))
                        .put("port", settings.getAsInt("elasticsearch.port", 9300))
                        .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                        .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                        .put("name", "feeder") // prevents lookup of names.txt, we don't have it, and marks this node as "feeder"
                        .put("client.transport.ignore_cluster_name", false) // ignore cluster name setting
                        .put("client.transport.ping_timeout", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) //  ping timeout
                        .put("client.transport.nodes_sampler_interval", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))); // for sniff sampling
                // optional found.no transport plugin
                if (settings.get("transport.type") != null) {
                    settingsBuilder.put("transport.type", settings.get("transport.type"));
                }
                // copy found.no transport settings
                Settings foundTransportSettings = settings.getAsSettings("transport.found");
                if (foundTransportSettings != null) {
                    ImmutableMap<String,String> foundTransportSettingsMap = foundTransportSettings.getAsMap();
                    for (Map.Entry<String,String> entry : foundTransportSettingsMap.entrySet()) {
                        settingsBuilder.put("transport.found." + entry.getKey(), entry.getValue());
                    }
                }
                ingest.maxActionsPerBulkRequest(maxbulkactions)
                        .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                        .maxVolumePerBulkRequest(maxvolume)
                        .flushIngestInterval(flushinterval)
                        .newClient(settingsBuilder.build());
                return ingest;
            }
        };
    }

    private final static MetricsLogger metricsLogger = new MetricsLogger();

    public void log() {
        try {
            if (source != null) {
                metricsLogger.writeMetrics(settings, source.getMetric());
            }
            if (sink != null) {
                metricsLogger.writeMetrics(settings, sink.getMetric());
            }
        } catch (Exception e) {
            //
        }
    }

    class MetricsThread extends Thread {
        public void run() {
            log();
        }
    }
}
