package org.xbib.elasticsearch.jdbc.strategy.column;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.xbib.elasticsearch.support.AbstractNodeTestHelper;
import org.xbib.elasticsearch.support.client.Ingest;
import org.xbib.elasticsearch.support.client.IngestFactory;
import org.xbib.elasticsearch.support.client.transport.BulkTransportClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.TimeZone;

public abstract class AbstractColumnStrategyTest extends AbstractNodeTestHelper {

    protected static ColumnSource source;

    protected static ColumnContext context;

    public abstract ColumnSource newSource();

    public abstract ColumnContext newContext();

    @BeforeMethod
    @Parameters({"starturl", "user", "password", "create"})
    public void beforeMethod(String starturl, String user, String password, @Optional String resourceName)
            throws Exception {
        startNodes();

        logger.info("nodes started");

        waitForYellow("1");
        source = newSource();
        source.setUrl(starturl)
                .setUser(user)
                .setPassword(password)
                .setLocale(Locale.getDefault())
                .setTimeZone(TimeZone.getDefault());
        logger.info("create table {}", resourceName);
        if (resourceName == null || "".equals(resourceName)) {
            return;
        }
        Connection connection = source.getConnectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        sqlScript(connection, resourceName);
        source.closeWriting();
    }

    @AfterMethod
    @Parameters({"stopurl", "user", "password", "delete"})
    public void afterMethod(String stopurl, String user, String password, @Optional String resourceName)
            throws Exception {

        logger.info("remove table {}", resourceName);
        if (resourceName == null || "".equals(resourceName)) {
            return;
        }
        // before dropping tables, open read connection must be closed to avoid hangs in mysql/postgresql
        logger.debug("closing reads...");
        source.closeReading();

        logger.debug("connecting for close...");
        Connection connection = source.getConnectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        logger.debug("cleaning...");
        // clean up tables
        sqlScript(connection, resourceName);
        logger.debug("closing writes...");
        source.closeWriting();

        // we can drop database by a magic 'stop' URL
        source = newSource();
        source.setUrl(stopurl)
                .setUser(user)
                .setPassword(password)
                .setLocale(Locale.getDefault())
                .setTimeZone(TimeZone.getDefault());
        try {
            logger.info("connecting to stop URL...");
            // activate stop URL
            source.getConnectionForWriting();
        } catch (Exception e) {
            // exception is expected, ignore
        }
        // close open write connection
        source.closeWriting();
        logger.info("stopped");

        // delete test index
        try {
            client("1").admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            logger.info("index {} deleted", index);
        } catch (IndexMissingException e) {
            logger.warn(e.getMessage());
        }
        stopNodes();
    }

    protected void create(String resource) throws Exception {
        waitForYellow("1");
        InputStream in = getClass().getResourceAsStream(resource);
        logger.info("creating context");
        Settings settings = ImmutableSettings.settingsBuilder()
                .loadFromStream("test", in)
                .build().getAsSettings("jdbc");
        context = newContext();
        context.setSettings(settings)
                .setIngestFactory(createIngestFactory(settings));
    }

    protected Settings createSettings(String resource)
            throws IOException {
        InputStream in = getClass().getResourceAsStream(resource);
        Settings settings = ImmutableSettings.settingsBuilder()
                .loadFromStream("test", in)
                .build().getAsSettings("jdbc");
        in.close();
        return settings;
    }

    private void sqlScript(Connection connection, String resourceName) throws Exception {
        InputStream in = getClass().getResourceAsStream(resourceName);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        String sql;
        while ((sql = br.readLine()) != null) {

            try {
                logger.trace("executing {}", sql);
                Statement p = connection.createStatement();
                p.execute(sql);
                p.close();
            } catch (SQLException e) {
                // ignore
                logger.error(sql + " failed. Reason: " + e.getMessage());
            } finally {
                connection.commit();
            }
        }
        br.close();
    }

    protected IngestFactory createIngestFactory(final Settings settings) {
        return new IngestFactory() {
            @Override
            public Ingest create() throws IOException {
                Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 10000);
                Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2);
                ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m"));
                TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
                BulkTransportClient ingest = new BulkTransportClient();
                Settings clientSettings = ImmutableSettings.settingsBuilder()
                        .put("cluster.name", settings.get("elasticsearch.cluster", getClusterName()))
                        .putArray("host", getHosts())
                        .put("port", settings.getAsInt("elasticsearch.port", 9300))
                        .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                        .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                        .put("name", "importer") // prevents lookup of names.txt, we don't have it
                        .put("client.transport.ignore_cluster_name", false) // ignore cluster name setting
                        .put("client.transport.ping_timeout", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) //  ping timeout
                        .put("client.transport.nodes_sampler_interval", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) // for sniff sampling
                        .build();
                ingest.maxActionsPerBulkRequest(maxbulkactions)
                        .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                        .maxVolumePerBulkRequest(maxvolume)
                        .flushIngestInterval(flushinterval)
                        .newClient(clientSettings);
                return ingest;
            }
        };
    }
}
