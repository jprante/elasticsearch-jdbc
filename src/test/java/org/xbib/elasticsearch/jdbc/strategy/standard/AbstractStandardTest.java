package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.IndexMissingException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.xbib.elasticsearch.action.jdbc.task.get.GetTaskAction;
import org.xbib.elasticsearch.action.jdbc.task.get.GetTaskRequest;
import org.xbib.elasticsearch.action.jdbc.task.get.GetTaskResponse;
import org.xbib.elasticsearch.common.state.State;
import org.xbib.elasticsearch.common.util.LocaleUtil;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.support.AbstractNodeTestHelper;

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

public abstract class AbstractStandardTest extends AbstractNodeTestHelper {

    private final static int SECONDS_TO_WAIT = 15;

    protected static JDBCSource JDBCSource;

    protected static Context context;

    public abstract JDBCSource newSource();

    public abstract Context newContext();

    @BeforeMethod
    @Parameters({"starturl", "user", "password", "create"})
    public void beforeMethod(String starturl, String user, String password, @Optional String resourceName)
            throws Exception {
        startNodes();
        logger.info("nodes started");
        waitForYellow("1");
        /*try {
            // create index
            client("1").admin().indices().create(new CreateIndexRequest("_river")).actionGet();
            logger.info("index created");
        } catch (IndexAlreadyExistsException e) {
            logger.warn(e.getMessage());
        }*/
        JDBCSource = newSource()
                .setUrl(starturl)
                .setUser(user)
                .setPassword(password)
                .setLocale(Locale.getDefault())
                .setTimeZone(TimeZone.getDefault());
        context = newContext();
        context.setSource(JDBCSource);
        JDBCSource.setContext(context);
        logger.info("create table {}", resourceName);
        if (resourceName == null || "".equals(resourceName)) {
            return;
        }
        Connection connection = JDBCSource.getConnectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        sqlScript(connection, resourceName);
        JDBCSource.closeWriting();
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
        JDBCSource.closeReading();

        logger.debug("connecting for close...");
        Connection connection = JDBCSource.getConnectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        logger.debug("cleaning...");
        // clean up tables
        sqlScript(connection, resourceName);
        logger.debug("closing writes...");
        JDBCSource.closeWriting();

        // some driver can drop database by a magic 'stop' URL
        JDBCSource = newSource()
                .setUrl(stopurl)
                .setUser(user)
                .setPassword(password)
                .setLocale(Locale.getDefault())
                .setTimeZone(TimeZone.getDefault());
        try {
            logger.info("connecting to stop URL...");
            // activate stop URL
            JDBCSource.getConnectionForWriting();
        } catch (Exception e) {
            // exception is expected, ignore
        }
        // close open write connection
        JDBCSource.closeWriting();
        logger.info("stopped");

        // delete test index
        try {
            client("1").admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            logger.info("index {} deleted", index);
        } catch (IndexMissingException e) {
            logger.warn(e.getMessage());
        }
        /*try {
            client("1").admin().indices().deleteMapping(new DeleteMappingRequest()
                    .indices(new String[]{"_river"}).types("my_jdbc_river")).actionGet();
            logger.info("river my_jdbc_river deleted");
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }*/
        stopNodes();
    }

    protected void perform(String resource) throws Exception {
        create(resource);
        waitFor();
        waitForActive();
        waitForInactive();
    }

    protected void create(String resource) throws Exception {
        waitForYellow("1");
        byte[] b = Streams.copyToByteArray(getClass().getResourceAsStream(resource));
        Map<String, Object> map = XContentHelper.convertToMap(b, false).v2();
        XContentBuilder builder = jsonBuilder().map(map);
        logger.info("task = {}", builder.string());
        IndexRequest indexRequest = Requests.indexRequest("_river").type("my_task").id("_meta")
                .source(builder.string());
        client("1").index(indexRequest).actionGet();
        client("1").admin().indices().prepareRefresh("_river").execute().actionGet();
        logger.info("creation request sent");
    }

    public void waitFor() throws Exception {
        waitFor(client("1"), "my_task", SECONDS_TO_WAIT);
        logger.info("task is created");
    }

    public void waitForActive() throws Exception {
        waitForActive(client("1"), "my_task", SECONDS_TO_WAIT);
        logger.info("task is active");
    }

    public void waitForInactive() throws Exception {
        waitForInactive(client("1"), "my_task", SECONDS_TO_WAIT);
        logger.info("task is inactive");
    }

    protected void createRandomProducts(String sql, int size)
            throws SQLException {
        Connection connection = JDBCSource.getConnectionForWriting();
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            add(connection, sql, UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        JDBCSource.closeWriting();
    }

    protected void createTimestampedLogs(String sql, int size, String locale, String timezone)
            throws SQLException {
        Connection connection = JDBCSource.getConnectionForWriting();
        Locale l = LocaleUtil.toLocale(locale);
        TimeZone t = TimeZone.getTimeZone(timezone);
        JDBCSource.setTimeZone(t).setLocale(l);
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
        JDBCSource.closeWriting();
    }

    private void add(Connection connection, String sql, final String name, final long amount, final double price)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList<Object>() {{
            add(name);
            add(amount);
            add(price);
        }};
        JDBCSource.bind(stmt, params);
        stmt.execute();
    }

    private void add(Connection connection, String sql, final Timestamp ts, final String message)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList<Object>() {{
            add(ts);
            add(message);
        }};
        JDBCSource.bind(stmt, params);
        stmt.execute();
    }

    protected void createRandomProductsJob(String sql, int size)
            throws SQLException {
        Connection connection = JDBCSource.getConnectionForWriting();
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            add(connection, sql, 1L, UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        JDBCSource.closeWriting();
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
        JDBCSource.bind(stmt, params);
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

    public static State waitFor(Client client, String name, int seconds)
            throws InterruptedException, IOException {
        GetTaskRequest stateRequest = new GetTaskRequest()
                .setName(name);
        GetTaskResponse stateResponse = client.admin().cluster()
                .execute(GetTaskAction.INSTANCE, stateRequest).actionGet();
        logger.debug("waitFor {}", name);
        boolean exists = stateResponse.exists(name);
        seconds = 2 * seconds;
        while (seconds-- > 0 && !exists) {
            Thread.sleep(500L);
            try {
                stateResponse = client.admin().cluster().execute(GetTaskAction.INSTANCE, stateRequest).actionGet();
                exists = stateResponse.exists(name);
                logger.debug("waitFor exists={} state={}", exists, stateResponse.getState());
            } catch (IndexMissingException e) {
                logger.warn("index missing");
            }
        }
        if (seconds < 0) {
            throw new IOException("timeout waiting for");
        }
        return stateResponse.getState();
    }

    public static State waitForActive(Client client, String name, int seconds) throws InterruptedException, IOException {
        long now = System.currentTimeMillis();
        GetTaskRequest stateRequest = new GetTaskRequest()
                .setName(name);
        GetTaskResponse stateResponse = client.admin().cluster()
                .execute(GetTaskAction.INSTANCE, stateRequest).actionGet();
        State state = stateResponse.getState();
        long t0 = state != null && state.getLastActiveBegin() != null ?
                state.getLastActiveBegin().getMillis() : 0L;
        logger.debug("waitForActive: now={} t0={} t0<now={} state={}", now, t0, t0 < now, state);
        seconds = 2 * seconds;
        while (seconds-- > 0 && t0 == 0 && t0 < now) {
            Thread.sleep(500L);
            try {
                stateResponse = client.admin().cluster().execute(GetTaskAction.INSTANCE, stateRequest).actionGet();
                state = stateResponse.getState();
                t0 = state != null ? state.getLastActiveBegin().getMillis() : 0L;
            } catch (IndexMissingException e) {
                logger.warn("index missing");
            }
            logger.debug("waitForActive: now={} t0={} t0<now={} state={}", now, t0, t0 < now, state);
        }
        if (seconds < 0) {
            throw new IOException("timeout waiting for active");
        }
        return state;
    }

    public static State waitForInactive(Client client, String name, int seconds) throws InterruptedException, IOException {
        long now = System.currentTimeMillis();
        GetTaskRequest stateRequest = new GetTaskRequest()
                .setName(name);
        GetTaskResponse stateResponse = client.admin().cluster()
                .execute(GetTaskAction.INSTANCE, stateRequest).actionGet();
        State state = stateResponse.getState();
        long t0 = state != null && state.getLastActiveBegin() != null ?
                state.getLastActiveBegin().getMillis() : 0L;
        long t1 = state != null  && state.getLastActiveEnd() != null ?
                state.getLastActiveEnd().getMillis() : 0L;
        logger.debug("waitForInactive: now={} t0<now={} t1-t0<=0={} state={}", now, t0 < now, t1 - t0 <= 0L, state);
        seconds = 2 * seconds;
        while (seconds-- > 0 && t0 < now && t1 - t0 <= 0L) {
            Thread.sleep(500L);
            try {
                stateResponse = client.admin().cluster().execute(GetTaskAction.INSTANCE, stateRequest).actionGet();
                state = stateResponse.getState();
                t0 = state != null && state.getLastActiveBegin() != null ?
                        state.getLastActiveBegin().getMillis() : 0L;
                t1 = state != null && state.getLastActiveEnd() != null ?
                        state.getLastActiveEnd().getMillis() : 0L;
            } catch (IndexMissingException e) {
                logger.warn("index missing");
            }
            logger.debug("waitForInactive: now={} t0<now={} t1-t0<=0={} state={}", now, t0 < now, t1 - t0 <= 0L, state);
        }
        if (seconds < 0) {
            throw new IOException("timeout waiting for inactive");
        }
        return state;
    }
}
