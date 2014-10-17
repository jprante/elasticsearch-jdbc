package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.RiverSettings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateRequest;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateResponse;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.support.helper.AbstractNodeTestHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractColumnRiverTest extends AbstractNodeTestHelper {

    private final static int SECONDS_TO_WAIT = 15;

    protected static ColumnRiverSource source;

    protected static ColumnRiverContext context;

    public abstract ColumnRiverSource newRiverSource();

    public abstract ColumnRiverContext newRiverContext();

    @BeforeMethod
    @Parameters({"starturl", "user", "password", "create"})
    public void beforeMethod(String starturl, String user, String password, @Optional String resourceName)
            throws Exception {
        startNodes();

        logger.info("nodes started");

        waitForYellow("1");
        try {
            // create river index
            client("1").admin().indices().create(new CreateIndexRequest("_river")).actionGet();
            logger.info("river index created");
        } catch (IndexAlreadyExistsException e) {
            logger.warn(e.getMessage());
        }
        source = newRiverSource();
        source.setUrl(starturl)
                .setUser(user)
                .setPassword(password)
                .setLocale(Locale.getDefault())
                .setTimeZone(TimeZone.getDefault());
        context = newRiverContext();
        context.setRiverSource(source);
        source.setRiverContext(context);
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

        // some driver can drop database by a magic 'stop' URL
        source = newRiverSource();
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
        try {
            client("1").admin().indices().deleteMapping(new DeleteMappingRequest()
                    .indices(new String[]{"_river"}).types("my_jdbc_river")).actionGet();
            logger.info("river my_jdbc_river deleted");
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
        stopNodes();
    }

    protected void performRiver(String resource) throws Exception {
        createRiver(resource);
        waitForRiver();
        waitForActiveRiver();
        waitForInactiveRiver();
    }

    protected void createRiver(String resource) throws Exception {
        waitForYellow("1");
        byte[] b = Streams.copyToByteArray(getClass().getResourceAsStream(resource));
        Map<String, Object> map = XContentHelper.convertToMap(b, false).v2();
        XContentBuilder builder = jsonBuilder().map(map);
        logger.info("river = {}", builder.string());
        IndexRequest indexRequest = Requests.indexRequest("_river").type("my_jdbc_river").id("_meta")
                .source(builder.string());
        client("1").index(indexRequest).actionGet();
        client("1").admin().indices().prepareRefresh("_river").execute().actionGet();
        logger.info("river is created");
    }

    public void waitForRiver() throws Exception {
        waitForRiver(client("1"), "my_jdbc_river", "jdbc", SECONDS_TO_WAIT);
        logger.info("river is up");
    }

    public void waitForActiveRiver() throws Exception {
        waitForActiveRiver(client("1"), "my_jdbc_river", "jdbc", SECONDS_TO_WAIT);
        logger.info("river is active");
    }

    public void waitForInactiveRiver() throws Exception {
        waitForInactiveRiver(client("1"), "my_jdbc_river", "jdbc", SECONDS_TO_WAIT);
        logger.info("river is inactive");
    }

    protected RiverSettings riverSettings(String resource)
            throws IOException {
        InputStream in = getClass().getResourceAsStream(resource);
        return new RiverSettings(ImmutableSettings.settingsBuilder().build(),
                XContentHelper.convertToMap(Streams.copyToByteArray(in), false).v2());
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

    public static RiverState waitForRiver(Client client, String riverName, String riverType, int seconds)
            throws InterruptedException, IOException {
        GetRiverStateRequest riverStateRequest = new GetRiverStateRequest()
                .setRiverName(riverName)
                .setRiverType(riverType);
        GetRiverStateResponse riverStateResponse = client.admin().cluster()
                .execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
        logger.info("waitForRiver {}/{}", riverName, riverType);
        while (seconds-- > 0 && riverStateResponse.exists(riverName, riverType)) {
            Thread.sleep(1000L);
            try {
                riverStateResponse = client.admin().cluster().execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
                logger.info("waitForRiver state={}", riverStateResponse.getRiverState());
            } catch (IndexMissingException e) {
                logger.warn("index missing");
            }
        }
        if (seconds < 0) {
            throw new IOException("timeout waiting for river");
        }
        return riverStateResponse.getRiverState();
    }

    public static RiverState waitForActiveRiver(Client client, String riverName, String riverType, int seconds) throws InterruptedException, IOException {
        long now = System.currentTimeMillis();
        GetRiverStateRequest riverStateRequest = new GetRiverStateRequest()
                .setRiverName(riverName)
                .setRiverType(riverType);
        GetRiverStateResponse riverStateResponse = client.admin().cluster()
                .execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
        RiverState riverState = riverStateResponse.getRiverState();
        long t0 = riverState != null ? riverState.getLastActiveBegin().getMillis() : 0L;
        logger.info("waitForActiveRiver: now={} t0={} t0<now={} state={}",
                now, t0, t0 < now, riverState);
        while (seconds-- > 0 && t0 == 0 && t0 < now) {
            Thread.sleep(1000L);
            try {
                riverStateResponse = client.admin().cluster().execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
                riverState = riverStateResponse.getRiverState();
                t0 = riverState != null ? riverState.getLastActiveBegin().getMillis() : 0L;
            } catch (IndexMissingException e) {
                //
            }
            logger.info("waitForActiveRiver: now={} t0={} t0<now={} state={}",
                    now, t0, t0 < now, riverState);
        }
        if (seconds < 0) {
            throw new IOException("timeout waiting for active river");
        }
        return riverState;
    }

    public static RiverState waitForInactiveRiver(Client client, String riverName, String riverType, int seconds) throws InterruptedException, IOException {
        long now = System.currentTimeMillis();
        GetRiverStateRequest riverStateRequest = new GetRiverStateRequest()
                .setRiverName(riverName)
                .setRiverType(riverType);
        GetRiverStateResponse riverStateResponse = client.admin().cluster()
                .execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
        RiverState riverState = riverStateResponse.getRiverState();
        long t0 = riverState != null ? riverState.getLastActiveBegin().getMillis() : 0L;
        long t1 = riverState != null ? riverState.getLastActiveEnd().getMillis() : 0L;
        logger.info("waitForInactiveRiver: now={} t0<now={} t1-t0<=0={} state={}",
                now, t0 < now, t1 - t0 <= 0L, riverState);
        while (seconds-- > 0 && t0 < now && t1 - t0 <= 0L) {
            Thread.sleep(1000L);
            try {
                riverStateResponse = client.admin().cluster().execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
                riverState = riverStateResponse.getRiverState();
                t0 = riverState != null ? riverState.getLastActiveBegin().getMillis() : 0L;
                t1 = riverState != null ? riverState.getLastActiveEnd().getMillis() : 0L;
            } catch (IndexMissingException e) {
                //
            }
            logger.info("waitForInactiveRiver: now={} t0<now={} t1-t0<=0={} state={}",
                    now, t0 < now, t1 - t0 <= 0L, riverState);
        }
        if (seconds < 0) {
            throw new IOException("timeout waiting for inactive river");
        }
        return riverState;
    }
}
