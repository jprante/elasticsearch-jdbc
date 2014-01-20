
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.client.Client;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.support.AbstractRiverNodeTest;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SimpleRiverDataTests extends AbstractRiverNodeTest {

    private Client client;

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        return new RiverContext();
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
        client = client("1");
        RiverSettings settings = riverSettings(riverResource);
        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, client);
        river.start();
        Thread.sleep(10000L); // let the river run
        assertEquals(client.prepareSearch(INDEX).execute().actionGet().getHits().getTotalHits(), 104);
        river.close();
    }

    /**
     * Product table
     *
     * @param riverResource the river
     * @param sql the SQL statement
     * @throws Exception
     */
    @Test
    @Parameters({"river3", "sql1"})
    public void testSimpleRiverMaxrows(String riverResource, String sql) throws Exception {
        Connection connection = source.connectionForWriting();
        createRandomProducts(connection, sql, 100);
        source.closeWriting();
        client = client("1");
        RiverSettings settings = riverSettings(riverResource);
        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, client);
        river.start();
        Thread.sleep(5000L); // let the river flow...
        river.once();
        Thread.sleep(5000L); // let the river flow...
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
        List<Object> params = new ArrayList<Object>() {{
            add(name);
            add(amount);
            add(price);
        }};
        source.bind(stmt, params);
        stmt.execute();
    }

}
