
package org.xbib.elasticsearch.plugin.feeder.jdbc;

import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.xbib.elasticsearch.action.river.state.RiverState;
import org.xbib.elasticsearch.plugin.feeder.AbstractFeeder;
import org.xbib.elasticsearch.plugin.feeder.Feeder;
import org.xbib.elasticsearch.plugin.jdbc.LocaleUtil;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.plugin.jdbc.RiverServiceLoader;
import org.xbib.elasticsearch.plugin.jdbc.SQLCommand;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.support.client.State;
import org.xbib.elasticsearch.support.client.bulk.BulkTransportClient;
import org.xbib.elasticsearch.support.client.ingest.IngestTransportClient;
import org.xbib.pipeline.Pipeline;
import org.xbib.pipeline.PipelineProvider;
import org.xbib.pipeline.PipelineRequest;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class JDBCFeeder<T, R extends PipelineRequest, P extends Pipeline<T, R>>
        extends AbstractFeeder<T, R, P> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(JDBCFeeder.class.getSimpleName());

    /**
     * River context used between runs
     */
    protected RiverContext riverContext;

    /**
     * River name, default is "feeder". If feeder runs ins river mode, this can be overwritten
     */
    private String name = "feeder";

    /**
     *  A default index that may be optionally declared. This index is created beforehand and configured
     *  to bulk mode before the feeder starts.
     */
    private String defaultIndex;

    public JDBCFeeder() {
    }

    public JDBCFeeder(JDBCFeeder feeder) {
        super(feeder);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public PipelineProvider<P> pipelineProvider() {
        return new PipelineProvider<P>() {
            @Override
            public P get() {
                return  (P) new JDBCFeeder(JDBCFeeder.this);
            }
        };
    }

    @Override
    public String getType() {
        return "jdbc";
    }

    public Feeder<T, R, P> setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    /**
     * Prepare a feed run. Open a client to Elasticsearch and create a queue of input specifications.
     * This method is executed in a single thread before feed threads are created.
     * @return this feeder
     * @throws IOException
     */
    public Feeder<T, R, P> beforeRun() throws IOException {
        if (spec == null) {
            throw new IllegalArgumentException("no spec?");
        }
        if (settings == null) {
            throw new IllegalArgumentException("no settings?");
        }
        if (ingest == null) {
            Integer maxbulkactions = settings.getAsInt("maxbulkactions", 1000);
            Integer maxconcurrentbulkrequests = settings.getAsInt("maxconcurrentbulkrequests",
                    Runtime.getRuntime().availableProcessors());
            ByteSizeValue maxvolume = settings.getAsBytesSize("maxbulkvolume", ByteSizeValue.parseBytesSizeValue("10m"));
            TimeValue maxrequestwait = settings.getAsTime("maxrequestwait", TimeValue.timeValueSeconds(60));
            ingest = "ingest".equals(settings.get("client")) ?
                    new IngestTransportClient()
                    : new BulkTransportClient();
            ingest.maxActionsPerBulkRequest(maxbulkactions)
                    .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                    .maxVolumePerBulkRequest(maxvolume)
                    .maxRequestWait(maxrequestwait);
            ingest.newClient(URI.create(settings.get("elasticsearch")));
        }
        // create queue
        super.beforeRun();
        return this;
    }

    /**
     * Each feed thread executes this method to do the main work.
     * This method is executed in paralle by all feed threads.
     * @param map the specification f the task to perform
     * @throws Exception
     */
    @Override
    public void executeTask(Map<String, Object> map) throws Exception {
        if (isInterrupted()) {
            logger.warn("interrupted");
            return;
        }
        createRiverContext(getType(), getName(), map);
        startBulk();
        if (riverState == null) {
            riverState = new RiverState();
        }
        riverState.load(ingest.client());
        riverState.setCustom(riverContext.asMap());
        // increment state counter
        Long counter = riverState.getCounter() + 1;
        this.riverState = riverState.setCounter(counter)
                .setEnabled(true)
                .setActive(true)
                .setTimestamp(new Date());
        riverState.save(ingest.client());
        if (logger.isDebugEnabled()) {
            logger.debug("state saved before fetch");
        }
        // set the job number to the state counter
        riverContext.job(Long.toString(counter));
        if (logger.isDebugEnabled()) {
            logger.debug("trying to fetch ...");
        }
        riverContext.getRiverSource().fetch();
        if (logger.isDebugEnabled()) {
            logger.debug("fetched, flushing");
        }
        riverContext.getRiverMouth().flush();
        if (logger.isDebugEnabled()) {
            logger.debug("flushed");
        }
        // we don't know if this is the last run. Stop bulk for now, make indexed documents visible for search
        stopBulk();

    }

    protected void createRiverContext(String riverType, String riverName, Map<String, Object> mySettings) throws IOException {
        String strategy = XContentMapValues.nodeStringValue(mySettings.get("strategy"), "simple");

        String url = XContentMapValues.nodeStringValue(mySettings.get("url"), null);
        String user = XContentMapValues.nodeStringValue(mySettings.get("user"), null);
        String password = XContentMapValues.nodeStringValue(mySettings.get("password"), null);
        List<SQLCommand> sql = SQLCommand.parse(mySettings);
        String rounding = XContentMapValues.nodeStringValue(mySettings.get("rounding"), null);
        int scale = XContentMapValues.nodeIntegerValue(mySettings.get("scale"), 2);
        boolean autocommit = XContentMapValues.nodeBooleanValue(mySettings.get("autocommit"), false);
        int fetchsize = url != null && url.startsWith("jdbc:mysql") ? Integer.MIN_VALUE :
                XContentMapValues.nodeIntegerValue(mySettings.get("fetchsize"), 10);
        int maxrows = XContentMapValues.nodeIntegerValue(mySettings.get("max_rows"), 0);
        int maxretries = XContentMapValues.nodeIntegerValue(mySettings.get("max_retries"), 3);
        TimeValue maxretrywait = XContentMapValues.nodeTimeValue(mySettings.get("max_retries_wait"),
                        TimeValue.timeValueSeconds(30));
        String locale = XContentMapValues.nodeStringValue(mySettings.get("locale"),
                LocaleUtil.fromLocale(Locale.getDefault()));
        String resultSetType = XContentMapValues.nodeStringValue(mySettings.get("resultset_type"),
                "TYPE_FORWARD_ONLY");
        String resultSetConcurrency = XContentMapValues.nodeStringValue(mySettings.get("resultset_concurrency"),
                "CONCUR_UPDATABLE");
        boolean shouldIgnoreNull = XContentMapValues.nodeBooleanValue(mySettings.get("ignore_null_values"), false);
        String timezone = XContentMapValues.nodeStringValue(mySettings.get("timezone"), TimeZone.getDefault().getID());
        boolean shouldPrepareDatabaseMetadata = XContentMapValues.nodeBooleanValue(mySettings.get("prepare_database_metadata"), false);
        boolean shouldPrepareResultSetMetadata = XContentMapValues.nodeBooleanValue(mySettings.get("prepare_resultset_metadata"), false);

        RiverSource riverSource = RiverServiceLoader.findRiverSource(strategy);
        logger.debug("found river source class {} for strategy {}", riverSource.getClass().getName(), strategy);
        RiverMouth riverMouth = RiverServiceLoader.findRiverMouth(strategy);
        logger.debug("found river mouth class {} for strategy {}", riverMouth.getClass().getName(), strategy);
        RiverFlow riverFlow = RiverServiceLoader.findRiverFlow(strategy);
        logger.debug("found river flow class {} for strategy {}", riverFlow.getClass().getName(), strategy);

        defaultIndex = XContentMapValues.nodeStringValue(mySettings.get("index"), "jdbc");
        String defaultType = XContentMapValues.nodeStringValue(mySettings.get("type"), "jdbc");
        boolean timeWindowed = XContentMapValues.nodeBooleanValue(mySettings.get("index_timewindow"), false);

        logger.info("river default index/type {}/{} (timewindowed={})", defaultIndex, defaultType, timeWindowed);

        if (mySettings.containsKey("index_settings")) {
            ingest.setSettings(settingsBuilder().put(new JsonSettingsLoader()
                    .load(jsonBuilder().map((Map<String, Object>) mySettings.get("index_settings")).string()))
                    .build());
        }
        if (mySettings.containsKey("type_mapping")) {
            ingest.addMapping(defaultType,
                    jsonBuilder().map((Map<String, Object>) mySettings.get("type_mapping")).string());
        }

        riverSource.setUrl(url)
                .setUser(user)
                .setPassword(password)
                .setTimeZone(TimeZone.getTimeZone(timezone));
        riverMouth.setIndex(defaultIndex)
                .setType(defaultType)
                .setIngest(ingest)
                .setTimeWindowed(timeWindowed);
        riverFlow.setFeeder(this);
        this.riverContext = new RiverContext()
                .setRiverName(riverName)
                .setRiverSettings(mySettings)
                .setRiverSource(riverSource)
                .setRiverMouth(riverMouth)
                .setRiverFlow(riverFlow)
                .setLocale(locale)
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
                .contextualize();
        logger.trace("JDBC feeder ready to start, context is {}", riverContext);
    }

    /**
     * Close a single cycle. First, open connections in the river source are closed,
     * then river mouth is closed. After this, set river state to disabled and save river state.
     *
     * @throws IOException if close failed
     */
    @Override
    public void close() throws IOException {
        super.close();
        if (riverContext != null) {
            riverContext.getRiverSource().closeReading();
            logger.info("reading connection closed");
            riverContext.getRiverSource().closeWriting();
            logger.info("writing connection closed");
            try {
                logger.info("river mouth closing");
                riverContext.getRiverMouth().close();
                logger.info("river mouth closed");
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            this.riverState = riverState
                    .setEnabled(false)
                    .setActive(false)
                    .setTimestamp(new Date());
            riverState.save(ingest.client());
            logger.info("river state saved");
        }
    }

    private void startBulk() {
        try {
            if (!ingest.client().admin().indices().prepareExists(defaultIndex).execute().actionGet().isExists()) {
                logger.info("creating index {} and enabling bulk mode", defaultIndex);
                ingest.newIndex(defaultIndex);
            }
            ingest.startBulk(defaultIndex);
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }
    }

    private void stopBulk() {
        State state = ingest.getState();
        if (state.indices() != null && !state.indices().isEmpty()) {
            for (String index : ImmutableSet.copyOf(state.indices())) {
                logger.info("stopping bulk mode for index {} and refreshing...", index);
                try {
                    ingest.stopBulk(index);
                    ingest.refresh(index);
                } catch (IOException e) {
                    logger.error(e.getMessage(),e);
                }
            }
        }
    }

}
