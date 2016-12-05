package org.xbib.importer.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.importer.Document;
import org.xbib.importer.elasticsearch.MockSink;
import org.xbib.importer.Sink;
import org.xbib.importer.elasticsearch.ElasticsearchDocumentBuilder;
import org.xbib.importer.util.LocaleUtil;
import org.xbib.importer.util.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 */
public class SourceTests {

    private static final Logger logger = LogManager.getLogger("test.source");

    private static JDBCSource source;

    @BeforeMethod
    @Parameters({"starturl", "user", "password", "create"})
    public void beforeMethod(String starturl, String user, String password, @Optional String resourceName)
            throws SQLException, IOException {
        logger.info("before method");
        source = new JDBCSource(starturl, user, password, new MockSink());
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
        logger.debug("remove table {}", resourceName);
        if (resourceName == null || "".equals(resourceName)) {
            return;
        }
        // before dropping tables, open read connection must be closed to avoid hangs in mysql/postgresql
        logger.debug("closing reads...");
        source.closeReadConnection();
        logger.debug("connecting for close...");
        Connection connection = source.openConnectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        logger.debug("cleaning...");
        // clean up tables
        sqlScript(connection, resourceName);
        logger.debug("closing writes...");
        source.closeWriteConnection();
        // we can drop database by a magic 'stop' URL
        source = new JDBCSource(stopurl, user, password, new MockSink());
        try {
            logger.info("connecting to stop URL...");
            // activate stop URL
            source.openConnectionForWriting();
        } catch (Exception e) {
            // exception is expected, ignore
        }
        // close open write connection
        source.closeWriteConnection();
    }

    @Test
    public void testSimpleConnectionClose() throws Exception {
        Connection connection = source.openConnectionForReading();
        assertFalse(connection.isClosed());
        source.closeReadConnection();
        assertTrue(connection.isClosed());
        source.openConnectionForReading();
    }

    @Test
    @Parameters({"sql1"})
    public void testSimpleSQL(String sql) throws Exception {
        PreparedStatement statement = source.prepareQuery(sql);
        ResultSet results = source.executeQuery(statement);
        for (int i = 0; i < 5; i++) {
            assertTrue(results.next());
        }
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql2", "n"})
    public void testSimpleStarQuery(String sql, @Optional Integer n) throws Exception {
        List<Object> params = new LinkedList<Object>();
        Sink output = new MockSink() {
            @Override
            public void index(Document document, boolean create) throws IOException {
                logger.debug("document={}", document);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        ElasticsearchDocumentBuilder tabularDataStream = new ElasticsearchDocumentBuilder(output);
        long rows = 0L;
        source.beforeRows(results, tabularDataStream);
        while (source.nextRow(results, tabularDataStream)) {
            rows++;
        }
        assertEquals(rows, n == null ? 5 : n);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql3"})
    public void testSimpleNullInteger(String sql) throws Exception {
        List<Object> params = new LinkedList<Object>();
        Sink sink = new MockSink() {
            @Override
            public void index(Document document, boolean create) throws IOException {
                if (document == null || document.getSource() == null) {
                    throw new IllegalArgumentException("object missing");
                }
                Values o = (Values) document.getSource().get("amount");
                if (o == null) {
                    o = (Values) document.getSource().get("AMOUNT"); // hsqldb is uppercase
                }
                if (o != null && !o.isNull()) {
                    throw new IllegalArgumentException("amount not null??? " + o.getClass().getName());
                }
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        ElasticsearchDocumentBuilder tabularDataStream = new ElasticsearchDocumentBuilder(sink);
        long rows = 0L;
        source.beforeRows(results, tabularDataStream);
        if (source.nextRow(results, tabularDataStream)) {
            // only one row
            rows++;
        }
        assertEquals(rows, 1);
        source.close(results);
        source.close(statement);
    }

    /**
     * Test JDBC Array to structured object array
     *
     * @param sql the array select statement
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"sql4", "res1", "res2"})
    public void testSimpleArray(@Optional String sql, @Optional String res1, @Optional String res2) throws Exception {
        if (sql == null) {
            return;
        }
        List<Object> params = new LinkedList<>();
        final List<Document> result = new LinkedList<>();
        Sink sink = new MockSink() {
            @Override
            public void index(Document object, boolean create) throws IOException {
                if (object == null || object.getSource() == null) {
                    throw new IllegalArgumentException("object missing");
                }
                result.add(object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        ElasticsearchDocumentBuilder tabularDataStream = new ElasticsearchDocumentBuilder(sink);
        long rows = 0L;
        source.beforeRows(results, tabularDataStream);
        while (source.nextRow(results, tabularDataStream)) {
            rows++;
        }
        assertEquals(rows, 2);
        source.close(results);
        source.close(statement);
        Iterator<Document> it = result.iterator();
        assertEquals(it.next().getSource().toString(), res1);
        assertEquals(it.next().getSource().toString(), res2);
    }

    protected void createRandomProducts(String sql, int size)
            throws SQLException {
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

    protected void createTimestampedLogs(String sql, int size, String locale, String timezone)
            throws SQLException {
        Connection connection = source.openConnectionForWriting();

        TimeZone t = TimeZone.getTimeZone(timezone);
        Locale l = LocaleUtil.toLocale(locale);
        //source.setTimeZone(t);
        //source.setLocale(l);
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
        source.closeWriteConnection();
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
        Connection connection = source.openConnectionForWriting();
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            long job = 0L;
            add(connection, sql, job, UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        source.closeWriteConnection();
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