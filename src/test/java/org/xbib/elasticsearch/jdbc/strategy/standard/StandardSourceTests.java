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
package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.common.util.StringKeyValueStreamListener;
import org.xbib.elasticsearch.jdbc.strategy.mock.MockSink;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.common.util.Values;
import org.xbib.elasticsearch.jdbc.strategy.Sink;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class StandardSourceTests extends AbstractSourceTest {

    private static final Logger logger = LogManager.getLogger(StandardSourceTests.class.getName());

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
        Sink sink = new MockSink() {
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
        StringKeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(sink);
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
        Sink sink = new MockSink() {
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
        StringKeyValueStreamListener listener = new StringKeyValueStreamListener()
                .output(sink);
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