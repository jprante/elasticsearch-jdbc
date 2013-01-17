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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.jdbc.RiverMouth;
import org.elasticsearch.river.jdbc.RiverSource;
import org.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.elasticsearch.river.jdbc.support.StructuredObject;
import org.elasticsearch.river.jdbc.support.ValueListener;
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

    private final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverSourceTests.class.getName());

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
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

}
