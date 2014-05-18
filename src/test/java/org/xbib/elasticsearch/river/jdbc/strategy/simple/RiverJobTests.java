package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.support.helper.AbstractRiverNodeTest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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
        //assertHits("1", 0);
        createRiver(riverResource);
        waitForInactiveRiver();
        assertHits("1", 100);
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
