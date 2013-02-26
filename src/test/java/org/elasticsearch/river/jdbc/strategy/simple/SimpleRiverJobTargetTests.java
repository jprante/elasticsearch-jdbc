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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SimpleRiverJobTargetTests extends AbstractRiverNodeTest {

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

    @Test
    @Parameters({"river1", "sql1", "sql2"})
    public void testSimpleRiverJob(String riverResource, String sql1, String sql2) throws IOException, InterruptedException, SQLException {
        Connection connection = source.connectionForWriting();
        createRandomProductsJob(connection, sql2, 100);
        source.closeWriting();

        connection = source.connectionForReading();
        ResultSet results = connection.createStatement().executeQuery(sql1);
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        int count = results.next() ? results.getInt(1) : -1;
        source.close(results);
        source.closeReading();
        assertEquals(count, 100);

        startNode("1");
        client = client("1");
        assertEquals(client.prepareSearch(INDEX).execute().actionGet().getHits().getTotalHits(), 0);

        RiverSettings settings = riverSettings(riverResource);
        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, "_river", client);
        river.once();

        Thread.sleep(3000L); // let some good things happen

        assertEquals(client.prepareSearch(INDEX).setTypes(TYPE).execute().actionGet().getHits().getTotalHits(), 100);

        connection = source.connectionForReading();
        results = connection.createStatement().executeQuery(sql1);
        count = results.next() ? results.getInt(1) : -1;
        results.close();
        assertEquals(count, 0);

        river.close();
    }

    private void createRandomProductsJob(Connection connection, String sql, int size)
            throws SQLException {
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            add(connection, sql, "1", UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }

    private void add(Connection connection, String sql, final String job, final String name, final long amount, final double price)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList() {
            {
                add(job);
                add(name);
                add(amount);
                add(price);
            }
        };
        source.bind(stmt, params);
        stmt.execute();
    }
}
