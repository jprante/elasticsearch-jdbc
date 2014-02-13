
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.AbstractRiverTest;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.RiverKeyValueStreamListener;
import org.xbib.elasticsearch.gatherer.IndexableObject;
import org.xbib.io.keyvalue.KeyValueStreamListener;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public class SimpleRiverSourceDataTests extends AbstractRiverTest {

    private static final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverSourceDataTests.class.getName());

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        return new RiverContext();
    }

    @Test
    @Parameters({"sql1"})
    public void testBill(String sql) throws Exception {
        List<Object> params = new ArrayList();
        RiverMouth output = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("sql1 object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        RiverKeyValueStreamListener listener = new RiverKeyValueStreamListener()
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
    public void testDepartments(String sql) throws Exception {
        List<Object> params = newLinkedList();
        RiverMouth output = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("sql2 object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        RiverKeyValueStreamListener listener = new RiverKeyValueStreamListener()
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
    public void testHighBills(String sql) throws Exception {
        List<Object> params = new ArrayList();
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
        KeyValueStreamListener listener = new RiverKeyValueStreamListener()
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
    public void testTimePeriod(String sql) throws Exception {
        List<Object> params = new ArrayList();
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
        RiverKeyValueStreamListener listener = new RiverKeyValueStreamListener()
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
    public void testIndexId(String sql) throws Exception {
        MockRiverMouth mock = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                super.index(object, create);
                logger.debug("products={}", object);
            }
        };
        mock.setIndex("products").setType("products");
        PreparedStatement statement = source.prepareQuery(sql);
        ResultSet results = source.executeQuery(statement);
        RiverKeyValueStreamListener listener = new RiverKeyValueStreamListener()
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
