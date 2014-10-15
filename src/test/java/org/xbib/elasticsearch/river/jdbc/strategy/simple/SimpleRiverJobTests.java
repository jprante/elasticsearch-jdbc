package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.river.jdbc.RiverSource;

import java.sql.Connection;
import java.sql.ResultSet;

public class SimpleRiverJobTests extends AbstractSimpleRiverTest {

    @Override
    public RiverSource newRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public SimpleRiverContext newRiverContext() {
        return new SimpleRiverContext();
    }

    @Test
    @Parameters({"river1", "sql1", "sql2"})
    public void testSimpleRiverJob(String riverResource, String sql1, String sql2)
            throws Exception {
        createRandomProductsJob(sql2, 100);
        Connection connection = source.getConnectionForReading();
        ResultSet results = connection.createStatement().executeQuery(sql1);
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        int count = results.next() ? results.getInt(1) : -1;
        source.close(results);
        source.closeReading();
        assertEquals(count, 100);
        performRiver(riverResource);
        assertHits("1", 100);
        // count docs in source table, must be null, because river deletes them.
        connection = source.getConnectionForReading();
        // sql1 = select count(*)
        results = connection.createStatement().executeQuery(sql1);
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        count = results.next() ? results.getInt(1) : -1;
        results.close();
        assertEquals(count, 0);
    }

}
