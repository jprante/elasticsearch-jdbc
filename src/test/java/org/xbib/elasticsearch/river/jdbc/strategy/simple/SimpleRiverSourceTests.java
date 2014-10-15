package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.keyvalue.KeyValueStreamListener;
import org.xbib.elasticsearch.plugin.jdbc.util.IndexableObject;
import org.xbib.elasticsearch.plugin.jdbc.util.Values;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.StringKeyValueStreamListener;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SimpleRiverSourceTests extends AbstractSimpleRiverTest {

    private static final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverSourceTests.class.getName());

    @Override
    public RiverSource newRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public SimpleRiverContext newRiverContext() {
        return new SimpleRiverContext();
    }

    @Test
    public void testSimpleConnectionClose() throws Exception {
        Connection connection = source.getConnectionForReading();
        assertFalse(connection.isClosed());
        source.closeReading();
        assertTrue(connection.isClosed());
        source.getConnectionForReading();
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
        RiverMouth output = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        KeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(output);
        long rows = 0L;
        source.beforeRows(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
        source.afterRows(results, listener);
        assertEquals(rows, n == null ? 5 : n);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql3"})
    public void testSimpleNullInteger(String sql) throws Exception {
        List<Object> params = new LinkedList<Object>();
        RiverMouth mouth = new MockRiverMouth() {
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
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        KeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(mouth);
        long rows = 0L;
        source.beforeRows(results, listener);
        if (source.nextRow(results, listener)) {
            // only one row
            rows++;
        }
        source.afterRows(results, listener);
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
        List<Object> params = new LinkedList<Object>();
        final List<IndexableObject> result = new LinkedList<IndexableObject>();
        RiverMouth mouth = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                if (object == null || object.source() == null) {
                    throw new IllegalArgumentException("object missing");
                }
                result.add(object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        KeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(mouth);
        long rows = 0L;
        source.beforeRows(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
        source.afterRows(results, listener);
        assertEquals(rows, 2);
        source.close(results);
        source.close(statement);
        Iterator<IndexableObject> it = result.iterator();
        assertEquals(it.next().source().toString(), res1);
        assertEquals(it.next().source().toString(), res2);
    }

}