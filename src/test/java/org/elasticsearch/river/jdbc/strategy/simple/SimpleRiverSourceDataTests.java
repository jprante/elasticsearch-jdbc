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
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.jdbc.RiverMouth;
import org.elasticsearch.river.jdbc.RiverSource;
import org.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.elasticsearch.river.jdbc.support.RiverContext;
import org.elasticsearch.river.jdbc.support.StructuredObject;
import org.elasticsearch.river.jdbc.support.ValueListener;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class SimpleRiverSourceDataTests extends AbstractRiverTest {

    private static final ESLogger logger = Loggers.getLogger(SimpleRiverSourceDataTests.class);

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
    @Parameters({"sql1"})
    public void testBill(String sql) throws Exception {
        try {
            List<Object> params = new ArrayList();
            RiverMouth target = new MockRiverMouth() {
                @Override
                public void index(StructuredObject object) throws IOException {
                    logger.debug("sql1 object={}", object);
                }
            };
            PreparedStatement statement = source.prepareQuery(sql);
            source.bind(statement, params);
            ResultSet results = source.executeQuery(statement);
            SimpleValueListener listener = new SimpleValueListener().target(target);
            long rows = 0L;
            source.beforeFirstRow(results, listener);
            while (source.nextRow(results, listener)) {
                rows++;
            }
            listener.reset();
            assertEquals(rows, 5);
            source.close(results);
            source.close(statement);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Test
    @Parameters({"sql2"})
    public void testDepartments(String sql) throws Exception {
        List<Object> params = new ArrayList();
        RiverMouth target = new MockRiverMouth() {
            @Override
            public void index(StructuredObject object) throws IOException {
                logger.debug("sql2 object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        SimpleValueListener listener = new SimpleValueListener().target(target);
        long rows = 0L;
        source.beforeFirstRow(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
        listener.reset();
        assertEquals(rows, 11);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql3"})
    public void testHighBills(String sql) throws Exception {
        List<Object> params = new ArrayList();
        params.add(2.00);
        RiverMouth target = new MockRiverMouth() {
            @Override
            public void index(StructuredObject object) throws IOException {
                logger.debug("sql3={}", object);
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
        assertEquals(rows, 2);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql4"})
    public void testTimePeriod(String sql) throws Exception {
        List<Object> params = new ArrayList();
        params.add("2012-06-10 00:00:00");
        RiverMouth target = new MockRiverMouth() {
            @Override
            public void index(StructuredObject object) throws IOException {
                logger.debug("object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        SimpleValueListener listener = new SimpleValueListener().target(target);
        long rows = 0L;
        source.beforeFirstRow(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
        listener.reset();
        assertEquals(rows, 3);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql5"})
    public void testIndexId(String sql) throws Exception {
        MockRiverMouth target = new MockRiverMouth() {
            @Override
            public void index(StructuredObject object) throws IOException {
                super.index(object);
                logger.debug("products={}", object);
            }
        };
        target.index("products").type("products");
        PreparedStatement statement = source.prepareQuery(sql);
        ResultSet results = source.executeQuery(statement);
        SimpleValueListener listener = new SimpleValueListener().target(target);
        long rows = 0L;
        source.beforeFirstRow(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
        listener.reset();
        assertEquals(target.getCounter(), 3);
        source.close(results);
        source.close(statement);
    }


}
