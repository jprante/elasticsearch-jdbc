package org.xbib.importer.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.content.settings.Settings;
import org.xbib.importer.Importer;
import org.xbib.importer.elasticsearch.MockSink;
import org.xbib.importer.elasticsearch.NodeTestBase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 */
public class SinkTests extends NodeTestBase {

    private static final Logger logger = LogManager.getLogger(SinkTests.class.getName());

    private static JDBCSource source;

    @BeforeMethod
    @Parameters({"starturl", "user", "password", "create"})
    public void beforeMethod(String starturl, String user, String password, @Optional String resourceName)
            throws Exception {
        startNodes();
        source = new JDBCSource(starturl, user, password, new MockSink());;
        logger.info("create table {}", resourceName);
        if (resourceName == null || "".equals(resourceName)) {
            return;
        }
        Connection connection = source.openConnectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        sqlScript(connection, resourceName);
        source.closeWriteConnection();
    }

    @AfterMethod
    @Parameters({"stopurl", "user", "password", "delete"})
    public void afterMethod(String stopurl, String user, String password, @Optional String resourceName)
            throws Exception {
        logger.info("remove table {}", resourceName);
        if (resourceName == null || "".equals(resourceName)) {
            return;
        }
        logger.debug("cleaning...");
        // clean up tables
        Connection connection = source.openConnectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        sqlScript(connection, resourceName);
        // before dropping tables, open read connection must be closed to avoid hangs in mysql/postgresql
        logger.debug("closing reads...");
        source.closeReadConnection();
        // we can drop database by a magic 'stop' URL
        source = new JDBCSource(stopurl, user, password, new MockSink());;
        try {
            logger.info("connecting to stop URL...");
            // activate stop URL
            source.openConnectionForWriting();
        } catch (Exception e) {
            // exception is expected, ignore
        }
        source.closeWriteConnection();
        logger.info("stopped");
        // delete test index
        try {
            client("1").admin().indices().prepareDelete(index).execute().actionGet();
            logger.info("index {} deleted", index);
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
        stopNodes();
    }

    /**
     * Start the task and execute a simple star query
     *
     * @throws Exception if test fails
     */
    //@Test
    //@Parameters({"task1", "sql1"})
    public void testTaskOne(String resource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        perform(resource);
    }

    /**
     * Product table (star query)
     *
     * @param resource the resource
     * @throws Exception if test fails
     */
    //@Test
    //@Parameters({"task2", "sql1"})
    public void testTaskTwo(String resource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        perform(resource);
        assertHits("1", 104);
    }

    /**
     * Product table
     *
     * @param resource the resource
     * @param sql           the SQL statement
     * @throws SQLException if test fails
     * @throws IOException if test fails
     */
    @Test
    @Parameters({"task3", "sql1"})
    public void testTaskThree(String resource, String sql) throws SQLException, IOException {
        createRandomProducts(sql, 100);
        perform(resource);
        assertHits("1", 104);
        assertTimestampSort("1", "timestamp", 104);
    }

    protected void perform(String resource) throws IOException {
        try (Importer importer = createImporter(resource)) {
            importer.open();
        }
    }

    protected Importer createImporter(String resource) throws IOException{
        try (InputStream in = getClass().getResourceAsStream(resource)) {
            Settings settings = Settings.settingsBuilder()
                    .put("output.elasticsearch.cluster", getClusterName())
                    .putArray("output.elasticsearch.host", getHost())
                    .loadFromStream("test", in)
                    .build();
            return new Importer(settings, "jdbc", 1);
        }
    }

    protected void createRandomProducts(String sql, int size) throws SQLException {
        Connection connection = source.openConnectionForWriting();
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            add(connection, sql, UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        source.closeWriteConnection();
    }

    private void add(Connection connection, String sql, final String name, final long amount, final double price)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList<Object>();
        params.add(name);
        params.add(amount);
        params.add(price);
        source.bind(stmt, params);
        stmt.execute();
    }

    private void sqlScript(Connection connection, String resourceName) throws IOException, SQLException {
        InputStream in = getClass().getResourceAsStream(resourceName);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        String sql;
        while ((sql = br.readLine()) != null) {
            try {
                logger.debug("executing {}", sql);
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
}
