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
package org.xbib.elasticsearch.jdbc.strategy.standard;

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
import org.xbib.elasticsearch.action.jdbc.task.get.GetTaskRequestBuilder;
import org.xbib.elasticsearch.action.jdbc.task.get.GetTaskResponse;
import org.xbib.elasticsearch.action.jdbc.task.post.PostTaskRequestBuilder;
import org.xbib.elasticsearch.action.jdbc.task.post.PostTaskResponse;
import org.xbib.elasticsearch.action.jdbc.task.put.PutStateRequestBuilder;
import org.xbib.elasticsearch.action.jdbc.task.put.PutStateResponse;
import org.xbib.elasticsearch.common.client.IngestFactory;
import org.xbib.elasticsearch.common.client.Metric;
import org.xbib.elasticsearch.common.state.State;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.Flow;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.Mouth;
import org.xbib.elasticsearch.common.util.DurationFormatUtil;
import org.xbib.elasticsearch.common.util.LocaleUtil;
import org.xbib.elasticsearch.common.util.SQLCommand;
import org.xbib.elasticsearch.common.util.StrategyLoader;
import org.xbib.elasticsearch.common.util.VolumeFormatUtil;

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
 * Standard flow implementation
 */
public class StandardFlow<C extends Context> implements Flow<C> {

    private final static ESLogger logger = ESLoggerFactory.getLogger("jdbc");

    private final static ESLogger metricsLogger = ESLoggerFactory.getLogger("jdbc.metrics");

    private String name;

    private Settings settings;

    private IngestFactory ingestFactory;

    private Client client;

    private Queue<Context> queue;

    private MeterMetric meterMetric;

    @Override
    public String strategy() {
        return "simple";
    }

    @Override
    public StandardFlow<C> newInstance() {
        return new StandardFlow<C>();
    }

    @Override
    public C newContext() {
        return (C) new StandardContext();
    }

    @Override
    public Flow setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Flow setSettings(Settings settings) {
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
    public Flow setIngestFactory(IngestFactory ingestFactory) {
        this.ingestFactory = ingestFactory;
        return this;
    }

    public Flow setClient(Client client) {
        this.client = client;
        return this;
    }

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public void execute(C context) throws Exception {
        logger.debug("execute: {} with context {}", name, context);
        try {
            beforeFetch(context);
            fetch(context);
        } finally {
            afterFetch(context);
        }
    }

    /**
     * Before a task starts, this method is called
     * @param context the context
     * @throws java.lang.Exception if this method fails
     */
    protected void beforeFetch(C context) throws Exception {
        logger.debug("before fetch: getting state for {}", name);
        GetTaskRequestBuilder stateRequestBuilder = new GetTaskRequestBuilder(client.admin().cluster())
                .setName(name);
        GetTaskResponse stateResponse = stateRequestBuilder.execute().actionGet();
        State state = stateResponse.getState();
        // if state was not defined yet, define it now
        if (state == null) {
            logger.debug("state not found, creating new state");
            state = new State()
                    .setName(name)
                    .setStarted(new DateTime());
            PutStateRequestBuilder putStateRequestBuilder = new PutStateRequestBuilder(client.admin().cluster())
                    .setName(name)
                    .setState(state);
            PutStateResponse putStateResponse = putStateRequestBuilder.execute().actionGet();
            if (putStateResponse.isAcknowledged()) {
                logger.debug("before fetch: put initial state {}", state);
            } else {
                logger.error("befor fetch: initial state not acknowledged", state);
            }
        }
        JDBCSource source = createSource(context.getDefinition());
        Mouth mouth = createMouth(context.getDefinition());
        context = fillContext(context, state, source, mouth);
        logger.debug("before fetch: created source = {}, mouth = {}, context = {}",
                source, mouth, context);
        Integer counter = state.getCounter() + 1;
        context.setState(state.setCounter(counter).setLastActive(new DateTime(), null));
        PostTaskRequestBuilder postStateRequestBuilder = new PostTaskRequestBuilder(client.admin().cluster())
                .setName(name)
                .setState(context.getState());
        PostTaskResponse postTaskResponse = postStateRequestBuilder.execute().actionGet();
        logger.debug("before fetch: state posted = {}", state);
        // call source "before fetch"
        try {
            context.getSource().beforeFetch();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        // call mouth "before fetch"
        try {
            context.getMouth().beforeFetch();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * After context and state setup, when data should be fetched from source, this method is called.
     * The default is to invoke the fetch() method of the source.
     *@param context the context
     * @throws Exception if fetch fails
     */
    protected void fetch(Context context) throws Exception {
        context.getSource().fetch();
    }

    @Override
    public Flow setMetric(MeterMetric meterMetric) {
        this.meterMetric = meterMetric;
        return this;
    }

    @Override
    public MeterMetric getMetric() {
        return meterMetric;
    }


    @Override
    public Flow setQueue(Queue<Context> queue) {
        this.queue = queue;
        return this;
    }

    @Override
    public Queue<Context> getQueue() {
        return queue;
    }

    /**
     * After the task has completed a single run, this method is called.
     * @param context the context
     * @throws java.lang.Exception if this method fails
     */
    protected void afterFetch(Context context) throws Exception {
        if (context == null) {
            return;
        }
        if (context.getMouth() == null) {
            logger.warn("no mouth");
            return;
        }
        // set activity
        State state = context.getState()
                .setLastActive(context.getState().getLastActiveBegin(), new DateTime());
        PostTaskRequestBuilder postStateRequestBuilder = new PostTaskRequestBuilder(client.admin().cluster())
                .setName(name)
                .setState(state);
        PostTaskResponse postTaskResponse = postStateRequestBuilder.execute().actionGet();
        logger.debug("after fetch: state posted = {}", state);
        try {
            context.getMouth().afterFetch();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        try {
            context.getSource().afterFetch();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    protected JDBCSource createSource(Map<String, Object> params) {
        JDBCSource source = (JDBCSource) StrategyLoader.newSource(strategy());
        logger.debug("found source class {}, params = {}", source, strategy(), params);
        String url = XContentMapValues.nodeStringValue(params.get("url"), null);
        String user = XContentMapValues.nodeStringValue(params.get("user"), null);
        String password = XContentMapValues.nodeStringValue(params.get("password"), null);
        String locale = XContentMapValues.nodeStringValue(params.get("locale"), LocaleUtil.fromLocale(Locale.getDefault()));
        String timezone = XContentMapValues.nodeStringValue(params.get("timezone"), TimeZone.getDefault().getID());
        source.setUrl(url)
                .setUser(user)
                .setPassword(password)
                .setLocale(LocaleUtil.toLocale(locale))
                .setTimeZone(TimeZone.getTimeZone(timezone));
        return source;
    }

    protected Mouth createMouth(Map<String, Object> params) throws IOException {
        Mouth mouth = StrategyLoader.newMouth(strategy());
        logger.debug("found mouth class {}, params = {}", mouth, strategy(), params);
        String index = XContentMapValues.nodeStringValue(params.get("index"), "jdbc");
        String type = XContentMapValues.nodeStringValue(params.get("type"), "jdbc");
        mouth.setIndex(index)
                .setType(type)
                .setIngestFactory(ingestFactory);
        if (params.get("index_settings") != null) {
            Map<String, String> loadedSettings = new JsonSettingsLoader()
                    .load(jsonBuilder().map((Map<String, Object>) params.get("index_settings")).string());
            mouth.setIndexSettings(settingsBuilder().put(loadedSettings).build());
        }
        if (params.get("type_mapping") != null) {
            XContentBuilder builder = jsonBuilder().map((Map<String, Object>) params.get("type_mapping"));
            mouth.setTypeMapping(Collections.singletonMap(type, builder.string()));
        }
        return mouth;
    }

    protected C fillContext(C context, State state, JDBCSource JDBCSource, Mouth mouth) throws IOException {
        Map<String, Object> params = context.getDefinition();
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

        context.setState(state)
                .setSource(JDBCSource)
                .setMouth(mouth)
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
        JDBCSource.setContext(context);
        mouth.setContext(context);
        return context;
    }

    @Override
    public void logMetrics(Context context, String cause) {
        MeterMetric metric = getMetric();
        if (metric == null) {
            return;
        }
        if (context == null || context.getMouth() == null) {
            return;
        }
        Metric mouthMetric = context.getMouth().getMetric();
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
        metricsLogger.info("{}: {} metrics: {} rows, {} mean, ({} {} {}), ingest metrics: elapsed {}, {} bytes, {} avg, {} MB/s",
                cause,
                name,
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

}
