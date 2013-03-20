/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.common.Base64;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.jdbc.RiverMouth;
import org.elasticsearch.river.jdbc.RiverSource;
import org.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.elasticsearch.river.jdbc.support.RiverContext;
import org.elasticsearch.river.jdbc.support.StructuredObject;
import org.elasticsearch.river.jdbc.support.ValueListener;
import org.elasticsearch.river.jdbc.support.ValueSet;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class SimpleRiverSourceTests extends AbstractRiverTest {

    private static final ESLogger logger = Loggers.getLogger(SimpleRiverSourceTests.class);

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        RiverContext context = new RiverContext();
        context.digesting(true);
        return context;
    }

    @Test
    public void testConnectionClose() throws Exception {
        Connection connection = source.connectionForReading();
        assertFalse(connection.isClosed());
        source.closeReading();
        assertTrue(connection.isClosed());
        source.connectionForReading();
    }

    @Test
    @Parameters({"sql1"})
    public void testSQL(String sql) throws Exception {
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
    public void testStarQuery(String sql, @Optional Integer n) throws Exception {
        List<? extends Object> params = new ArrayList();
        RiverMouth target = new MockRiverMouth() {
            @Override
            public void index(StructuredObject object) throws IOException {
                logger.debug("object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        ValueListener listener = new SimpleValueListener().target(target);
        long rows = 0L;
        source.beforeFirstRow(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
        listener.reset();
        assertEquals(rows, n == null ? 5 : n);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql2", "n"})
    public void testDigestQuery(String sql, @Optional Integer n) throws Exception {
        List<? extends Object> params = new ArrayList();
        RiverMouth mouth = new MockRiverMouth() {
            @Override
            public void index(StructuredObject object) throws IOException {
                logger.debug("object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        ValueListener listener = new SimpleValueListener()
                .digest(context.digesting())
                .target(mouth);
        long rows = 0L;
        source.beforeFirstRow(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
        listener.reset();
        assertEquals(Base64.encodeBytes(listener.digest().digest()),
                "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=");
        assertEquals(rows, n == null ? 5 : n);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql3"})
    public void testNullInteger(String sql) throws Exception {
        List<? extends Object> params = new ArrayList();
        RiverMouth mouth = new MockRiverMouth() {
            @Override
            public void index(StructuredObject object) throws IOException {
                if (object == null || object.source() == null) {
                    throw new IllegalArgumentException("object missing");
                }
                ValueSet o = (ValueSet)object.source().get("amount");
                if (o == null) {
                    o = (ValueSet)object.source().get("AMOUNT"); // hsqldb is uppercase
                }
                if (!o.isNull()) {
                    throw new IllegalArgumentException("amount not null??? " + o.getClass().getName() );
                }
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        ValueListener listener = new SimpleValueListener()
                .digest(context.digesting())
                .target(mouth);
        long rows = 0L;
        source.beforeFirstRow(results, listener);
        if (source.nextRow(results, listener)) {
            // only one row
            rows++;
        }
        listener.reset();
        assertEquals(rows, 1);
        source.close(results);
        source.close(statement);
    }
}