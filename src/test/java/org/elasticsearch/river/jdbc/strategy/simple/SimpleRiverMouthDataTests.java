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

import org.elasticsearch.client.Client;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.jdbc.JDBCRiver;
import org.elasticsearch.river.jdbc.RiverSource;
import org.elasticsearch.river.jdbc.support.RiverContext;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SimpleRiverMouthDataTests extends AbstractRiverNodeTest {

    private Client client;

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

    /**
     * Product table star select
     *
     * @param riverResource
     * @throws Exception
     */
    @Test
    @Parameters({"river2", "sql1"})
    public void testSimpleRiverRandom(String riverResource, String sql) throws Exception {
        Connection connection = source.connectionForWriting();
        createRandomProducts(connection, sql, 100);
        source.closeWriting();
        startNode("1");
        client = client("1");
        RiverSettings settings = riverSettings(riverResource);
        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, "_river", client);
        river.start();
        Thread.sleep(3000L); // let the good things happen
        assertEquals(client.prepareSearch(INDEX).execute().actionGet().getHits().getTotalHits(), 104);
        river.close();
    }

    /**
     * Product table
     *
     * @param riverResource
     * @throws Exception
     */
    @Test
    @Parameters({"river3", "sql1"})
    public void testSimpleRiverMaxrows(String riverResource, String sql) throws Exception {
        Connection connection = source.connectionForWriting();
        createRandomProducts(connection, sql, 100);
        source.closeWriting();
        startNode("1");
        client = client("1");
        RiverSettings settings = riverSettings(riverResource);
        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, "_river", client);
        river.start();
        Thread.sleep(3000L); // let some good things happen
        river.once();
        Thread.sleep(3000L); // let other good things happen
        assertEquals(client.prepareSearch(INDEX).execute().actionGet().getHits().getTotalHits(), 208);
        river.close();
    }

    private void createRandomProducts(Connection connection, String sql, int size)
            throws SQLException {
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            addData(connection, sql, UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }

    private void addData(Connection connection, String sql, final String name, final long amount, final double price)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList() {{
            add(name);
            add(amount);
            add(price);
        }};
        source.bind(stmt, params);
        stmt.execute();
    }

}
