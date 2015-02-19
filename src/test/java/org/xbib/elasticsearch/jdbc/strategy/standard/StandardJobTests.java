package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;

import java.sql.Connection;
import java.sql.ResultSet;

public class StandardJobTests extends AbstractStandardTest {

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    @Test
    @Parameters({"task1", "sql1", "sql2"})
    public void testJob(String resource, String sql1, String sql2)
            throws Exception {
        createRandomProductsJob(sql2, 100);
        Connection connection = JDBCSource.getConnectionForReading();
        ResultSet results = connection.createStatement().executeQuery(sql1);
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        int count = results.next() ? results.getInt(1) : -1;
        JDBCSource.close(results);
        JDBCSource.closeReading();
        assertEquals(count, 100);
        perform(resource);
        assertHits("1", 100);
        // count docs in source table, must be null
        connection = JDBCSource.getConnectionForReading();
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
