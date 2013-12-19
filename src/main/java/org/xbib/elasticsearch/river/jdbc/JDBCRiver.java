
package org.xbib.elasticsearch.river.jdbc;

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
import org.xbib.elasticsearch.river.jdbc.support.LocaleUtil;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.RiverServiceLoader;

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

    private final String url;

    private final String driver;

    private final String indexName;

    private final String typeName;

    private final String indexSettings;

    private final String typeMapping;

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

        Map<String, Object> mySettings =
                riverSettings.settings().containsKey(TYPE)
                        ? (Map<String, Object>) riverSettings.settings().get(TYPE)
                        : new HashMap<String, Object>();
        // default is a single run
        strategy = XContentMapValues.nodeStringValue(mySettings.get("strategy"), "oneshot");
        url = XContentMapValues.nodeStringValue(mySettings.get("url"), null);
        driver = XContentMapValues.nodeStringValue(mySettings.get("driver"), null);
        String user = XContentMapValues.nodeStringValue(mySettings.get("user"), null);
        String password = XContentMapValues.nodeStringValue(mySettings.get("password"), null);
        TimeValue poll = XContentMapValues.nodeTimeValue(mySettings.get("poll"), TimeValue.timeValueMinutes(60));
        String sql = XContentMapValues.nodeStringValue(mySettings.get("sql"), null);
        List<? super Object> sqlparams = XContentMapValues.extractRawValues("sqlparams", mySettings);
        boolean callable = XContentMapValues.nodeBooleanValue(mySettings.get("callable"), Boolean.FALSE);
        String rounding = XContentMapValues.nodeStringValue(mySettings.get("rounding"), null);
        int scale = XContentMapValues.nodeIntegerValue(mySettings.get("scale"), 2);
        boolean autocommit = XContentMapValues.nodeBooleanValue(mySettings.get("autocommit"), Boolean.FALSE);
        String columnCreatedAt;
        columnCreatedAt = XContentMapValues.nodeStringValue(mySettings.get("column_created_at"), "created_at");
        String columnUpdatedAt = XContentMapValues.nodeStringValue(mySettings.get("column_updated_at"), "updated_at");
        String columnDeletedAt = XContentMapValues.nodeStringValue(mySettings.get("column_deleted_at"), null);
        boolean columnEscape = XContentMapValues.nodeBooleanValue(mySettings.get("column_escape"), true);
        int fetchsize = url.startsWith("jdbc:mysql") ? Integer.MIN_VALUE :
                XContentMapValues.nodeIntegerValue(mySettings.get("fetchsize"), 10);
        int maxrows = XContentMapValues.nodeIntegerValue(mySettings.get("max_rows"), 0);
        int maxretries = XContentMapValues.nodeIntegerValue(mySettings.get("max_retries"), 3);
        TimeValue maxretrywait = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(mySettings.get("max_retries_wait"), "10s"), TimeValue.timeValueMillis(30000));
        String locale = XContentMapValues.nodeStringValue(mySettings.get("locale"), LocaleUtil.fromLocale(Locale.getDefault()));
        boolean digesting = XContentMapValues.nodeBooleanValue(mySettings.get("digesting"), Boolean.TRUE);
        String acksql = XContentMapValues.nodeStringValue(mySettings.get("acksql"), null);
        List<? super Object> acksqlparams = XContentMapValues.extractRawValues("acksqlparams", mySettings);
        String presql = XContentMapValues.nodeStringValue(mySettings.get("presql"), null);
        List<? super Object> presqlparams = XContentMapValues.extractRawValues("presqlparams", mySettings);
        indexName = XContentMapValues.nodeStringValue(mySettings.get("index"), TYPE);
        typeName = XContentMapValues.nodeStringValue(mySettings.get("type"), TYPE);
        int bulkSize = XContentMapValues.nodeIntegerValue(mySettings.get("bulk_size"), 100);
        int maxBulkRequests = XContentMapValues.nodeIntegerValue(mySettings.get("max_bulk_requests"), 30);
        indexSettings = XContentMapValues.nodeStringValue(mySettings.get("index_settings"), null);
        typeMapping = XContentMapValues.nodeStringValue(mySettings.get("type_mapping"), null);
        boolean acknowledgeBulk = XContentMapValues.nodeBooleanValue(mySettings.get("acknowledge"), Boolean.FALSE);

        riverSource = RiverServiceLoader.findRiverSource(strategy);
        logger.debug("found river source {} for strategy {}", riverSource.getClass().getName(), strategy);
        riverSource.driver(driver)
                .url(url)
                .user(user)
                .password(password)
                .rounding(rounding)
                .precision(scale);

        riverMouth = RiverServiceLoader.findRiverMouth(strategy);
        logger.debug("found river mouth {} for strategy {}", riverMouth.getClass().getName(), strategy);
        riverMouth.index(indexName)
                .type(typeName)
                .maxBulkActions(bulkSize)
                .maxConcurrentBulkRequests(maxBulkRequests)
                .acknowledge(acknowledgeBulk)
                .client(client);

        riverContext = new RiverContext()
                .riverName(riverName.getName())
                .riverIndexName(riverIndexName)
                .riverSettings(riverSettings.settings())
                .riverSource(riverSource)
                .riverMouth(riverMouth)
                .pollInterval(poll)
                .pollStatement(sql)
                .pollStatementParams(sqlparams)
                .pollPreStatement(presql)
                .pollPreStatementParams(presqlparams)
                .callable(callable)
                .pollAckStatement(acksql)
                .pollAckStatementParams(acksqlparams)
                .autocommit(autocommit)
                .columnCreatedAt(columnCreatedAt)
                .columnUpdatedAt(columnUpdatedAt)
                .columnDeletedAt(columnDeletedAt)
                .columnEscape(columnEscape)
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

        logger.debug("found river flow {} for strategy {}", riverFlow.getClass().getName(), strategy);
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
