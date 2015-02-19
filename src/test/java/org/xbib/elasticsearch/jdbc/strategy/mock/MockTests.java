package org.xbib.elasticsearch.jdbc.strategy.mock;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.keyvalue.KeyValueStreamListener;
import org.xbib.elasticsearch.jdbc.strategy.standard.AbstractStandardTest;
import org.xbib.elasticsearch.jdbc.util.IndexableObject;
import org.xbib.elasticsearch.jdbc.strategy.Mouth;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardContext;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardSource;
import org.xbib.elasticsearch.jdbc.support.StringKeyValueStreamListener;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

public class MockTests extends AbstractStandardTest {

    private static final ESLogger logger = ESLoggerFactory.getLogger(MockTests.class.getName());

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    @Test
    @Parameters({"sql1"})
    public void testMockBill(String sql) throws Exception {
        List<Object> params = new LinkedList<Object>();
        Mouth output = new MockMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("sql1 object={}", object);
            }
        };
        PreparedStatement statement = JDBCSource.prepareQuery(sql);
        JDBCSource.bind(statement, params);
        ResultSet results = JDBCSource.executeQuery(statement);
        StringKeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(output);
        long rows = 0L;
        JDBCSource.beforeRows(results, listener);
        while (JDBCSource.nextRow(results, listener)) {
            rows++;
        }
        JDBCSource.afterRows(results, listener);
        assertEquals(rows, 5);
        JDBCSource.close(results);
        JDBCSource.close(statement);
    }

    @Test
    @Parameters({"sql2"})
    public void testMockDepartments(String sql) throws Exception {
        List<Object> params = new LinkedList<Object>();
        Mouth output = new MockMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("sql2 object={}", object);
            }
        };
        PreparedStatement statement = JDBCSource.prepareQuery(sql);
        JDBCSource.bind(statement, params);
        ResultSet results = JDBCSource.executeQuery(statement);
        StringKeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(output);
        JDBCSource.beforeRows(results, listener);
        long rows = 0L;
        while (JDBCSource.nextRow(results, listener)) {
            rows++;
        }
        JDBCSource.afterRows(results, listener);
        assertEquals(rows, 11);
        JDBCSource.close(results);
        JDBCSource.close(statement);
    }

    @Test
    @Parameters({"sql3"})
    public void testMockHighBills(String sql) throws Exception {
        List<Object> params = new LinkedList<Object>();
        params.add(2.00);
        Mouth output = new MockMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("sql3={}", object);
            }
        };
        PreparedStatement statement = JDBCSource.prepareQuery(sql);
        JDBCSource.bind(statement, params);
        ResultSet results = JDBCSource.executeQuery(statement);
        KeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(output);
        JDBCSource.beforeRows(results, listener);
        long rows = 0L;
        while (JDBCSource.nextRow(results, listener)) {
            rows++;
        }
        JDBCSource.afterRows(results, listener);
        assertEquals(rows, 2);
        JDBCSource.close(results);
        JDBCSource.close(statement);
    }

    @Test
    @Parameters({"sql4"})
    public void testMockTimePeriod(String sql) throws Exception {
        List<Object> params = new LinkedList<Object>();
        params.add("2012-06-10 00:00:00");
        Mouth output = new MockMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("object={}", object);
            }
        };
        PreparedStatement statement = JDBCSource.prepareQuery(sql);
        JDBCSource.bind(statement, params);
        ResultSet results = JDBCSource.executeQuery(statement);
        StringKeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(output);
        JDBCSource.beforeRows(results, listener);
        long rows = 0L;
        while (JDBCSource.nextRow(results, listener)) {
            rows++;
        }
        JDBCSource.afterRows(results, listener);
        assertEquals(rows, 3);
        JDBCSource.close(results);
        JDBCSource.close(statement);
    }

    @Test
    @Parameters({"sql5"})
    public void testMockIndexId(String sql) throws Exception {
        MockMouth mock = new MockMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                super.index(object, create);
                logger.debug("products={}", object);
            }
        };
        //mock.setIndex("products").setType("products");
        PreparedStatement statement = JDBCSource.prepareQuery(sql);
        ResultSet results = JDBCSource.executeQuery(statement);
        StringKeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(mock);
        JDBCSource.beforeRows(results, listener);
        while (JDBCSource.nextRow(results, listener)) {
            // ignore
        }
        JDBCSource.afterRows(results, listener);
        assertEquals(mock.getCounter(), 3);
        JDBCSource.close(results);
        JDBCSource.close(statement);
    }


}
