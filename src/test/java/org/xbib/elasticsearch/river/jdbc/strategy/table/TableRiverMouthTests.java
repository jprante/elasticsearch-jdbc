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
package org.xbib.elasticsearch.river.jdbc.strategy.table;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.client.Client;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.support.AbstractRiverNodeTest;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class TableRiverMouthTests extends AbstractRiverNodeTest {

    private Client client;

    @Override
    public RiverSource getRiverSource() {
        return new TableRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        RiverContext context = new RiverContext();
        context.digesting(true);
        return context;
    }

    @Test
    @Parameters({"river1", "sql1"})
    public void testTableRiver(String riverResource, String sql) throws SQLException, IOException, InterruptedException {
        Connection connection = source.connectionForWriting();
        createData(connection, sql, 101);
        createDelete(connection, sql, 1);
        source.closeWriting();
        client = client("1");
        RiverSettings settings = riverSettings(riverResource);
        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, "_river", client);
        river.once();
        Thread.sleep(3000L); // let the good things happen
        assertEquals(client.prepareSearch(INDEX).setTypes(TYPE).execute().actionGet().getHits().getTotalHits(), 100);
        river.close();
    }

    @SuppressWarnings("unchecked")
	private void createDelete(Connection connection, String sql, final int id)
            throws SQLException {
        
    	PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList() {
            {
                add(INDEX);
                add(TYPE);
                add(Integer.toString(id));
                add("delete");
                add(null);
                add(1);
                add(1.0);
            }
        };
        source.bind(stmt, params);
        stmt.execute();

        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }
    
    private void createData(Connection connection, String sql, int size)
            throws SQLException {
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            addData(connection, sql, INDEX, TYPE, Integer.toString(i), "index", UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }

    private void addData(Connection connection, String sql, final String index, final String type, final String id, final String operationType, final String name, final long amount, final double price)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList() {
            {
                add(index);
                add(type);
                add(id);
                add(operationType);
                add(name);
                add(amount);
                add(price);
            }
        };
        source.bind(stmt, params);
        stmt.execute();
    }
}
