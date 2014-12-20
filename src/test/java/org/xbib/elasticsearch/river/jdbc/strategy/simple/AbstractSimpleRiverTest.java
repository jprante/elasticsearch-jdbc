package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateAction;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateRequest;
import org.xbib.elasticsearch.action.plugin.jdbc.state.get.GetRiverStateResponse;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.plugin.jdbc.util.LocaleUtil;
import org.xbib.elasticsearch.river.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.support.helper.AbstractNodeTestHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractSimpleRiverTest extends AbstractNodeTestHelper {

    private final static int SECONDS_TO_WAIT = 15;

    protected static RiverSource source;

    protected static RiverContext context;

    public abstract RiverSource newRiverSource();

    public abstract RiverContext newRiverContext();

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
        source = newRiverSource()
                .setUrl(starturl)
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
        source = newRiverSource()
                .setUrl(stopurl)
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
        logger.info("river creation request sent");
    }

    public void waitForRiver() throws Exception {
        waitForRiver(client("1"), "my_jdbc_river", "jdbc", SECONDS_TO_WAIT);
        logger.info("river is created");
    }

    public void waitForActiveRiver() throws Exception {
        waitForActiveRiver(client("1"), "my_jdbc_river", "jdbc", SECONDS_TO_WAIT);
        logger.info("river is active");
    }

    public void waitForInactiveRiver() throws Exception {
        waitForInactiveRiver(client("1"), "my_jdbc_river", "jdbc", SECONDS_TO_WAIT);
        logger.info("river is inactive");
    }

    protected void createRandomProducts(String sql, int size)
            throws SQLException {
        Connection connection = source.getConnectionForWriting();
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            add(connection, sql, UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        source.closeWriting();
    }

    protected void createTimestampedLogs(String sql, int size, String locale, String timezone)
            throws SQLException {
        Connection connection = source.getConnectionForWriting();
        Locale l = LocaleUtil.toLocale(locale);
        TimeZone t = TimeZone.getTimeZone(timezone);
        source.setTimeZone(t).setLocale(l);
        Calendar cal = Calendar.getInstance(t, l);
        // half of log in the past, half of it in the future
        cal.add(Calendar.HOUR, -(size / 2));
        for (int i = 0; i < size; i++) {
            Timestamp modified = new Timestamp(cal.getTimeInMillis());
            String message = "Hello world";
            add(connection, sql, modified, message);
            cal.add(Calendar.HOUR, 1);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        source.closeWriting();
    }

    private void add(Connection connection, String sql, final String name, final long amount, final double price)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList<Object>() {{
            add(name);
            add(amount);
            add(price);
        }};
        source.bind(stmt, params);
        stmt.execute();
    }

    private void add(Connection connection, String sql, final Timestamp ts, final String message)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList<Object>() {{
            add(ts);
            add(message);
        }};
        source.bind(stmt, params);
        stmt.execute();
    }

    protected void createRandomProductsJob(String sql, int size)
            throws SQLException {
        Connection connection = source.getConnectionForWriting();
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            add(connection, sql, 1L, UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        source.closeWriting();
    }

    private void add(Connection connection, String sql, final long job, final String name, final long amount, final double price)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList<Object>() {
            {
                add(job);
                add(name);
                add(amount);
                add(price);
            }
        };
        source.bind(stmt, params);
        stmt.execute();
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
        logger.debug("waitForRiver {}/{}", riverName, riverType);
        boolean exists = riverStateResponse.exists(riverName, riverType);
        seconds = 2 * seconds;
        while (seconds-- > 0 && !exists) {
            Thread.sleep(500L);
            try {
                riverStateResponse = client.admin().cluster().execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
                exists = riverStateResponse.exists(riverName, riverType);
                logger.debug("waitForRiver exists={} state={}", exists, riverStateResponse.getRiverState());
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
        long t0 = riverState != null && riverState.getLastActiveBegin() != null ?
                riverState.getLastActiveBegin().getMillis() : 0L;
        logger.debug("waitForActiveRiver: now={} t0={} t0<now={} state={}", now, t0, t0 < now, riverState);
        seconds = 2 * seconds;
        while (seconds-- > 0 && t0 == 0 && t0 < now) {
            Thread.sleep(500L);
            try {
                riverStateResponse = client.admin().cluster().execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
                riverState = riverStateResponse.getRiverState();
                t0 = riverState != null ? riverState.getLastActiveBegin().getMillis() : 0L;
            } catch (IndexMissingException e) {
                logger.warn("index missing");
            }
            logger.debug("waitForActiveRiver: now={} t0={} t0<now={} state={}", now, t0, t0 < now, riverState);
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
        long t0 = riverState != null && riverState.getLastActiveBegin() != null ?
                riverState.getLastActiveBegin().getMillis() : 0L;
        long t1 = riverState != null  && riverState.getLastActiveEnd() != null ?
                riverState.getLastActiveEnd().getMillis() : 0L;
        logger.debug("waitForInactiveRiver: now={} t0<now={} t1-t0<=0={} state={}", now, t0 < now, t1 - t0 <= 0L, riverState);
        seconds = 2 * seconds;
        while (seconds-- > 0 && t0 < now && t1 - t0 <= 0L) {
            Thread.sleep(500L);
            try {
                riverStateResponse = client.admin().cluster().execute(GetRiverStateAction.INSTANCE, riverStateRequest).actionGet();
                riverState = riverStateResponse.getRiverState();
                t0 = riverState != null && riverState.getLastActiveBegin() != null ?
                        riverState.getLastActiveBegin().getMillis() : 0L;
                t1 = riverState != null && riverState.getLastActiveEnd() != null ?
                        riverState.getLastActiveEnd().getMillis() : 0L;
            } catch (IndexMissingException e) {
                logger.warn("index missing");
            }
            logger.debug("waitForInactiveRiver: now={} t0<now={} t1-t0<=0={} state={}", now, t0 < now, t1 - t0 <= 0L, riverState);
        }
        if (seconds < 0) {
            throw new IOException("timeout waiting for inactive river");
        }
        return riverState;
    }
}
