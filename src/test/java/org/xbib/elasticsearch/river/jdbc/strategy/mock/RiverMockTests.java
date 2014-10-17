package org.xbib.elasticsearch.river.jdbc.strategy.mock;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.keyvalue.KeyValueStreamListener;
import org.xbib.elasticsearch.plugin.jdbc.util.IndexableObject;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.AbstractSimpleRiverTest;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverContext;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;
import org.xbib.elasticsearch.river.jdbc.support.StringKeyValueStreamListener;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

public class RiverMockTests extends AbstractSimpleRiverTest {

    private static final ESLogger logger = ESLoggerFactory.getLogger(RiverMockTests.class.getName());

    @Override
    public RiverSource newRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public SimpleRiverContext newRiverContext() {
        return new SimpleRiverContext();
    }

    @Test
    @Parameters({"sql1"})
    public void testMockBill(String sql) throws Exception {
        List<Object> params = new LinkedList();
        RiverMouth output = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("sql1 object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        StringKeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(output);
        long rows = 0L;
        source.beforeRows(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
        source.afterRows(results, listener);
        assertEquals(rows, 5);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql2"})
    public void testMockDepartments(String sql) throws Exception {
        List<Object> params = new LinkedList();
        RiverMouth output = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("sql2 object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        StringKeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(output);
        source.beforeRows(results, listener);
        long rows = 0L;
        while (source.nextRow(results, listener)) {
            rows++;
        }
        source.afterRows(results, listener);
        assertEquals(rows, 11);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql3"})
    public void testMockHighBills(String sql) throws Exception {
        List<Object> params = new LinkedList();
        params.add(2.00);
        RiverMouth output = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("sql3={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        KeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(output);
        source.beforeRows(results, listener);
        long rows = 0L;
        while (source.nextRow(results, listener)) {
            rows++;
        }
        source.afterRows(results, listener);
        assertEquals(rows, 2);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql4"})
    public void testMockTimePeriod(String sql) throws Exception {
        List<Object> params = new LinkedList();
        params.add("2012-06-10 00:00:00");
        RiverMouth output = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        StringKeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(output);
        source.beforeRows(results, listener);
        long rows = 0L;
        while (source.nextRow(results, listener)) {
            rows++;
        }
        source.afterRows(results, listener);
        assertEquals(rows, 3);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql5"})
    public void testMockIndexId(String sql) throws Exception {
        MockRiverMouth mock = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                super.index(object, create);
                logger.debug("products={}", object);
            }
        };
        //mock.setIndex("products").setType("products");
        PreparedStatement statement = source.prepareQuery(sql);
        ResultSet results = source.executeQuery(statement);
        StringKeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(mock);
        source.beforeRows(results, listener);
        while (source.nextRow(results, listener)) {
            // ignore
        }
        source.afterRows(results, listener);
        assertEquals(mock.getCounter(), 3);
        source.close(results);
        source.close(statement);
    }


}
