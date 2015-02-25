package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.common.keyvalue.KeyValueStreamListener;
import org.xbib.elasticsearch.jdbc.strategy.mock.MockMouth;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.common.util.Values;
import org.xbib.elasticsearch.jdbc.strategy.Mouth;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.support.StringKeyValueStreamListener;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class StandardSourceTests extends AbstractStandardTest {

    private static final ESLogger logger = ESLoggerFactory.getLogger(StandardSourceTests.class.getName());

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    @Test
    public void testSimpleConnectionClose() throws Exception {
        Connection connection = JDBCSource.getConnectionForReading();
        assertFalse(connection.isClosed());
        JDBCSource.closeReading();
        assertTrue(connection.isClosed());
        JDBCSource.getConnectionForReading();
    }

    @Test
    @Parameters({"sql1"})
    public void testSimpleSQL(String sql) throws Exception {
        PreparedStatement statement = JDBCSource.prepareQuery(sql);
        ResultSet results = JDBCSource.executeQuery(statement);
        for (int i = 0; i < 5; i++) {
            assertTrue(results.next());
        }
        JDBCSource.close(results);
        JDBCSource.close(statement);
    }

    @Test
    @Parameters({"sql2", "n"})
    public void testSimpleStarQuery(String sql, @Optional Integer n) throws Exception {
        List<Object> params = new LinkedList<Object>();
        Mouth output = new MockMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("object={}", object);
            }
        };
        PreparedStatement statement = JDBCSource.prepareQuery(sql);
        JDBCSource.bind(statement, params);
        ResultSet results = JDBCSource.executeQuery(statement);
        KeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(output);
        long rows = 0L;
        JDBCSource.beforeRows(results, listener);
        while (JDBCSource.nextRow(results, listener)) {
            rows++;
        }
        JDBCSource.afterRows(results, listener);
        assertEquals(rows, n == null ? 5 : n);
        JDBCSource.close(results);
        JDBCSource.close(statement);
    }

    @Test
    @Parameters({"sql3"})
    public void testSimpleNullInteger(String sql) throws Exception {
        List<Object> params = new LinkedList<Object>();
        Mouth mouth = new MockMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                if (object == null || object.source() == null) {
                    throw new IllegalArgumentException("object missing");
                }
                Values o = (Values) object.source().get("amount");
                if (o == null) {
                    o = (Values) object.source().get("AMOUNT"); // hsqldb is uppercase
                }
                if (!o.isNull()) {
                    throw new IllegalArgumentException("amount not null??? " + o.getClass().getName());
                }
            }
        };
        PreparedStatement statement = JDBCSource.prepareQuery(sql);
        JDBCSource.bind(statement, params);
        ResultSet results = JDBCSource.executeQuery(statement);
        KeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(mouth);
        long rows = 0L;
        JDBCSource.beforeRows(results, listener);
        if (JDBCSource.nextRow(results, listener)) {
            // only one row
            rows++;
        }
        JDBCSource.afterRows(results, listener);
        assertEquals(rows, 1);
        JDBCSource.close(results);
        JDBCSource.close(statement);
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
        List<Object> params = new LinkedList<Object>();
        final List<IndexableObject> result = new LinkedList<IndexableObject>();
        Mouth mouth = new MockMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                if (object == null || object.source() == null) {
                    throw new IllegalArgumentException("object missing");
                }
                result.add(object);
            }
        };
        PreparedStatement statement = JDBCSource.prepareQuery(sql);
        JDBCSource.bind(statement, params);
        ResultSet results = JDBCSource.executeQuery(statement);
        KeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(mouth);
        long rows = 0L;
        JDBCSource.beforeRows(results, listener);
        while (JDBCSource.nextRow(results, listener)) {
            rows++;
        }
        JDBCSource.afterRows(results, listener);
        assertEquals(rows, 2);
        JDBCSource.close(results);
        JDBCSource.close(statement);
        Iterator<IndexableObject> it = result.iterator();
        assertEquals(it.next().source().toString(), res1);
        assertEquals(it.next().source().toString(), res2);
    }

}