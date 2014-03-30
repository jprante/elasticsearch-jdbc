
package org.xbib.elasticsearch.river.jdbc;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import org.xbib.elasticsearch.river.jdbc.support.LocaleUtil;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.RiverServiceLoader;
import org.xbib.elasticsearch.river.jdbc.support.SQLCommand;

import static org.elasticsearch.common.collect.Maps.newHashMap;

/**
 * The JDBC river
 */
public class JDBCRiver extends AbstractRiverComponent implements River {

    private final ESLogger logger = ESLoggerFactory.getLogger(JDBCRiver.class.getName());

    public final static String NAME = "river-jdbc";

    public final static String TYPE = "jdbc";

    private final String strategy;

    private final String url;

    private final String indexName;

    private final String typeName;

    private final RiverSource riverSource;

    private final RiverMouth riverMouth;

    private final RiverFlow riverFlow;

    private final RiverContext riverContext;

    private volatile boolean closed;

    @Inject
    public JDBCRiver(RiverName riverName,
                     RiverSettings riverSettings,
                     Client client) {
        super(riverName, riverSettings);
        logger.debug("JDBC river initializing...");

        Map<String, Object> mySettings = newHashMap();
        if (riverSettings.settings().containsKey(TYPE)) {
            mySettings = (Map<String, Object>) riverSettings.settings().get(TYPE);
        }
        strategy = XContentMapValues.nodeStringValue(mySettings.get("strategy"), "simple");
        String schedule = mySettings.containsKey("schedule") ?
            XContentMapValues.nodeStringValue(mySettings.get("schedule"), null) :
            null;
        Integer poolsize = XContentMapValues.nodeIntegerValue(mySettings.get("poolsize"), 1);
        // disable interval
        TimeValue interval = XContentMapValues.nodeTimeValue(mySettings.get("interval"), TimeValue.timeValueMinutes(-1));

        // JDBC
        url = XContentMapValues.nodeStringValue(mySettings.get("url"), null);
        String user = XContentMapValues.nodeStringValue(mySettings.get("user"), null);
        String password = XContentMapValues.nodeStringValue(mySettings.get("password"), null);
        List<SQLCommand> sql = SQLCommand.parse(mySettings);
        String rounding = XContentMapValues.nodeStringValue(mySettings.get("rounding"), null);
        int scale = XContentMapValues.nodeIntegerValue(mySettings.get("scale"), 2);
        boolean autocommit = XContentMapValues.nodeBooleanValue(mySettings.get("autocommit"), Boolean.FALSE);

        int fetchsize = url.startsWith("jdbc:mysql") ? Integer.MIN_VALUE :
                XContentMapValues.nodeIntegerValue(mySettings.get("fetchsize"), 10);
        int maxrows = XContentMapValues.nodeIntegerValue(mySettings.get("max_rows"), 0);
        int maxretries = XContentMapValues.nodeIntegerValue(mySettings.get("max_retries"), 3);
        TimeValue maxretrywait =
                XContentMapValues.nodeTimeValue(mySettings.get("max_retries_wait"),
                        TimeValue.timeValueSeconds(30));
        String locale = XContentMapValues.nodeStringValue(mySettings.get("locale"),
                LocaleUtil.fromLocale(Locale.getDefault()));
        String resultSetType = XContentMapValues.nodeStringValue(mySettings.get("resultset_type"),
                "TYPE_FORWARD_ONLY");
        String resultSetConcurrency = XContentMapValues.nodeStringValue(mySettings.get("resultset_concurrency"),
                "CONCUR_UPDATABLE");

        // defaults for column strategy
        String columnCreatedAt = XContentMapValues.nodeStringValue(mySettings.get("created_at"), "created_at");
        String columnUpdatedAt = XContentMapValues.nodeStringValue(mySettings.get("updated_at"), "updated_at");
        String columnDeletedAt = XContentMapValues.nodeStringValue(mySettings.get("deleted_at"), null);
        boolean columnEscape = XContentMapValues.nodeBooleanValue(mySettings.get("column_escape"), true);

        // set up bulk indexer. If no _index or _type pseudo columns are set in SQL statements, this index is used.
        indexName = XContentMapValues.nodeStringValue(mySettings.get("index"), TYPE);
        typeName = XContentMapValues.nodeStringValue(mySettings.get("type"), TYPE);
        int bulkSize = XContentMapValues.nodeIntegerValue(mySettings.get("bulk_size"), 100);
        int maxBulkRequests = XContentMapValues.nodeIntegerValue(mySettings.get("max_bulk_requests"), 30);
        // flush interval for bulk indexer
        TimeValue flushInterval = XContentMapValues.nodeTimeValue(mySettings.get("bulk_flush_interval"),
                TimeValue.timeValueSeconds(5));
        // get two maps from the river settings to improve index creation
        Map<String,Object> indexSettings = mySettings.containsKey("index_settings") ?
                XContentMapValues.nodeMapValue(mySettings.get("index_settings"), null) : null;
        Map<String,Object> typeMapping = mySettings.containsKey("type_mapping") ?
                XContentMapValues.nodeMapValue(mySettings.get("type_mapping"), null) : null;

        riverSource = RiverServiceLoader.findRiverSource(strategy);
        logger.debug("found river source class {} for strategy {}", riverSource.getClass().getName(), strategy);

        riverMouth = RiverServiceLoader.findRiverMouth(strategy);
        logger.debug("found river mouth class {} for strategy {}", riverMouth.getClass().getName(), strategy);

        riverFlow = RiverServiceLoader.findRiverFlow(strategy);
        logger.debug("found river flow class {} for strategy {}", riverFlow.getClass().getName(), strategy);

        riverSource.url(url)
                .user(user)
                .password(password);

        riverMouth.setSettings(indexSettings)
                .setMapping(typeMapping)
                .setIndex(indexName)
                .setType(typeName)
                .setMaxBulkActions(bulkSize)
                .setMaxConcurrentBulkRequests(maxBulkRequests)
                .setFlushInterval(flushInterval)
                .client(client);

        riverContext = new RiverContext()
                .riverName(riverName.getName())
                .riverSettings(riverSettings.settings())
                .riverSource(riverSource)
                .riverMouth(riverMouth)
                .riverFlow(riverFlow)
                .setLocale(locale)
                .setRounding(rounding)
                .setScale(scale)
                .setSchedule(schedule)
                .setPoolSize(poolsize)
                .setInterval(interval)
                .setStatements(sql)
                .setAutoCommit(autocommit)
                .setMaxRows(maxrows)
                .setFetchSize(fetchsize)
                .setRetries(maxretries)
                .setMaxRetryWait(maxretrywait)
                .setResultSetType(resultSetType)
                .setResultSetConcurrency(resultSetConcurrency)
                .columnCreatedAt(columnCreatedAt)
                .columnUpdatedAt(columnUpdatedAt)
                .columnDeletedAt(columnDeletedAt)
                .columnEscape(columnEscape)
                .contextualize();

        logger.debug("JDBC river initialized, ready to start");

    }

    @Override
    public void start() {
        logger.info("starting JDBC river: URL [{}], strategy [{}], index/type [{}/{}]",
                url, strategy, indexName, typeName);
        Thread thread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                "jdbc-river-[" + riverName.name() + '/' + strategy + ']')
                .newThread(riverFlow);
        riverFlow.schedule(thread);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        logger.info("closing JDBC river: URL [{}], strategy [{}], index/type [{}/{}]",
                url, strategy, indexName, typeName);
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
    }

    /**
     * Induce a river run once, but in a synchronous manner. Mainly used for tests.
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
        RiverFlow riverFlow = RiverServiceLoader.findRiverFlow(strategy);
        // prepare task for run
        riverFlow.riverContext(riverContext);
        Thread thread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                "JDBC river: [" + riverName.name() + '/' + strategy + ')')
                .newThread(riverFlow);
        riverFlow.once(thread);
    }

    public RiverFlow riverFlow() {
        return riverFlow;
    }

}
