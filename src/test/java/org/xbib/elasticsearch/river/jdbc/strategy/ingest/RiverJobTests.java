
package org.xbib.elasticsearch.river.jdbc.strategy.ingest;

import org.elasticsearch.client.Client;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;
import org.xbib.elasticsearch.river.jdbc.support.AbstractRiverNodeTest;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class RiverJobTests extends AbstractRiverNodeTest {

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        return new RiverContext();
    }

    @Test
    @Parameters({"river1", "sql1", "sql2"})
    public void testSimpleRiverJob(String riverResource, String sql1, String sql2)
            throws IOException, InterruptedException, SQLException {
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
        Client client = client("1");
        assertEquals(client.prepareSearch(INDEX).execute().actionGet().getHits().getTotalHits(), 0);
        RiverSettings settings = riverSettings(riverResource);
        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, client);
        river.once();
        Thread.sleep(10000L); // let the river flow
        assertEquals(client.prepareSearch(INDEX).setTypes(TYPE).execute().actionGet().getHits().getTotalHits(), 100);
        connection = source.connectionForReading();
        // sql1 = select count(*)
        results = connection.createStatement().executeQuery(sql1);
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
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
        List<Object> params = new ArrayList<Object>() {
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
