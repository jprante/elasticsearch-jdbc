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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.xbib.elasticsearch.common.util.LocaleUtil;
import org.xbib.elasticsearch.common.util.StrategyLoader;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.Sink;
import org.xbib.elasticsearch.common.util.SQLCommand;
import org.xbib.elasticsearch.support.client.IngestFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * The context consists of the parameters that span source and mouth settings.
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
        return this;
    }

    @Override
    public S getSource() {
        return source;
    }

    public StandardContext setSink(Sink sink) {
        this.sink = sink;
        return this;
    }

    public Sink getSink() {
        return sink;
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
    public void beforeFetch() throws Exception {
        logger.info("before fetch");
        S source = createSource();
        Sink sink = createMouth();
        prepareContext(source, sink);
        logger.info("before fetch: created source = {}, mouth = {}", source, sink);
        getSink().beforeFetch();
        getSource().beforeFetch();
    }

    @Override
    public void fetch() throws Exception {
        logger.info("fetch");
        getSource().fetch();
    }

    @Override
    public void afterFetch() throws Exception {
        logger.info("after fetch");
        try {
            getSource().afterFetch();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        try {
            getSink().afterFetch();
        } catch (Exception e) {
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

    protected Sink createMouth() throws IOException {
        Sink sink = StrategyLoader.newMouth(strategy());
        logger.info("found mouth class {}", sink);
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
}
