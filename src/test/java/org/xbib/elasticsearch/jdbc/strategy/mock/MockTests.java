/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.jdbc.strategy.mock;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.common.keyvalue.KeyValueStreamListener;
import org.xbib.elasticsearch.common.util.StringKeyValueStreamListener;
import org.xbib.elasticsearch.jdbc.strategy.standard.AbstractSinkTest;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.jdbc.strategy.Sink;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardContext;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardSource;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

public class MockTests extends AbstractSinkTest {

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
        Sink output = new MockSink() {
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
        List<Object> params = new LinkedList<Object>();
        Sink output = new MockSink() {
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
        List<Object> params = new LinkedList<Object>();
        params.add(2.00);
        Sink output = new MockSink() {
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
        List<Object> params = new LinkedList<Object>();
        params.add("2012-06-10 00:00:00");
        Sink output = new MockSink() {
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
        MockSink mock = new MockSink() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                super.index(object, create);
                logger.debug("products={}", object);
            }
        };
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
