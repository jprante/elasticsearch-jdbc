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
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.joda.time.DateTime;
import org.xbib.elasticsearch.common.metrics.MetricsLogger;
import org.xbib.elasticsearch.common.util.LocaleUtil;
import org.xbib.elasticsearch.common.util.StrategyLoader;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.Sink;
import org.xbib.elasticsearch.common.util.SQLCommand;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
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

    private final static Logger logger = LogManager.getLogger("importer.jdbc.context.standard");

    private Settings settings;

    private S source;

    private Sink sink;

    private State state = State.IDLE;

    private DateTime dateOfThrowable;

    private Throwable throwable;

    private final static List<Future> futures = new LinkedList<>();

    @Override
    public String strategy() {
        return "standard";
    }

    @Override
    public StandardContext newInstance() {
        return new StandardContext();
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
            logger.info("metrics thread started");
        }
        return this;
    }

    @Override
    public Settings getSettings() {
        return settings;
    }

    @Override
    public StandardContext setSource(S source) {
        logger.debug("set Source");
        this.source = source;
        Map<String,String> map = settings.getAsMap();
        if (map.containsKey("metrics.lastexecutionstart")) {
            DateTime lastexecutionstart = DateTime.parse(settings.get("metrics.lastexecutionstart"));
            source.getMetric().setLastExecutionStart(lastexecutionstart);
            logger.debug("lastexecutionstart {}", lastexecutionstart);
        }
        if (map.containsKey("metrics.lastexecutionend")) {
            DateTime lastexecutionend = DateTime.parse(settings.get("metrics.lastexecutionend"));
            source.getMetric().setLastExecutionEnd(lastexecutionend);
            logger.debug("lastexecutionend {}", lastexecutionend);
        }
        if (map.containsKey("metrics.counter")) {
            int counter = Integer.parseInt(settings.get("metrics.counter"));
            if (counter > 0) {
                source.getMetric().setCounter(counter);
            }
        }
        return this;
    }

    @Override
    public S getSource() {
        return source;
    }

    @Override
    public StandardContext setSink(Sink sink) {
        this.sink = sink;
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
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void beforeFetch() throws Exception {
        logger.debug("before fetch");
        Sink sink = createSink();
        S source = createSource();
        prepareContext(source, sink);
        sink.setContext(this);
        source.setContext(this);
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
            logger.error("at fetch: " + e.getMessage(), e);
        }
    }

    @Override
    public void afterFetch() throws Exception {
        logger.debug("after fetch");
        writeState();
        try {
            getSource().afterFetch();
        } catch (Throwable e) {
            setThrowable(e);
            logger.error("after fetch: " + e.getMessage(), e);
        }
        try {
            getSink().afterFetch();
        } catch (Throwable e) {
            setThrowable(e);
            logger.error("after fetch: " + e.getMessage(), e);
        }
    }

    @Override
    public void shutdown() {
        logger.info("shutdown in progress");
        for (Future future : futures) {
            future.cancel(true);
        }
        if (source != null) {
            try {
                source.shutdown();
            } catch (Exception e) {
                logger.error("source shutdown: " + e.getMessage(), e);
            }
        }
        if (sink != null) {
            try {
                sink.shutdown();
            } catch (Exception e) {
                logger.error("sink shutdown: " + e.getMessage(), e);
            }
        }
        logger.info("shutdown completed");
        writeState();
    }

    protected void writeState() {
        String statefile = settings.get("statefile");
        if (statefile == null || source == null || source.getMetric() == null) {
            return;
        }
        try {
            File file = new File(statefile);
            if (file.getParentFile() != null && !file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            if (!file.exists() || file.canWrite()) {
                Writer writer = new FileWriter(statefile);
                FormatDateTimeFormatter formatter = Joda.forPattern("dateOptionalTime");
                Settings.Builder settingsBuilder = Settings.settingsBuilder()
                        .put(settings)
                        .put("metrics.lastexecutionstart", formatter.printer().print(source.getMetric().getLastExecutionStart()))
                        .put("metrics.lastexecutionend", formatter.printer().print(source.getMetric().getLastExecutionEnd()))
                        .put("metrics.counter", source.getMetric().getCounter());
                XContentBuilder builder = jsonBuilder().prettyPrint()
                        .startObject()
                        .field("type", "jdbc")
                        .field("jdbc")
                        .map(settingsBuilder.build().getAsStructuredMap())
                        .endObject();
                writer.write(builder.string());
                writer.close();
                if (file.length() > 0) {
                    logger.info("state persisted to {}", statefile);
                } else {
                    logger.error("state file truncated!");
                }
            } else {
                logger.warn("can't write to {}", statefile);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
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
        return sink;
    }

    @SuppressWarnings("unchecked")
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
        boolean shouldDetectGeo = XContentMapValues.nodeBooleanValue(params.get("detect_geo"), true);
        boolean shouldDetectJson = XContentMapValues.nodeBooleanValue(params.get("detect_json"), true);
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
                .shouldDetectGeo(shouldDetectGeo)
                .shouldDetectJson(shouldDetectJson)
                .shouldPrepareDatabaseMetadata(shouldPrepareDatabaseMetadata)
                .shouldPrepareResultSetMetadata(shouldPrepareResultSetMetadata)
                .setColumnNameMap(columnNameMap)
                .setQueryTimeout(queryTimeout)
                .setConnectionProperties(connectionProperties)
                .shouldTreatBinaryAsString(shouldTreatBinaryAsString);
        setSource(source);
        setSink(sink);
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
            // ignore log errors
        }
    }

    class MetricsThread extends Thread {
        public void run() {
            log();
        }
    }
}
