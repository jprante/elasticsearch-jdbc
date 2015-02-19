package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;

public class StandardScheduleTests extends AbstractStandardTest {

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    /**
     * Product table star select, scheduled for more than two runs
     *
     * @param resource the resource
     * @param sql           the SQL statement
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"task6", "sql1"})
    public void testSimpleSchedule(String resource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        create(resource);
        waitFor();
        waitForActive();
        Thread.sleep(12500L); // run more than twice
        client("1").admin().indices().prepareRefresh(index).execute().actionGet();
        long hits = client("1").prepareSearch(index).execute().actionGet().getHits().getTotalHits();
        assertTrue(hits > 100L);
    }

    /**
     * Test read and write of timestamps in a table. We create 100 timestamps over hour interval,
     * current timestamp $now is in the center.
     * Selecting timestamps from $now, there should be at least 50 rows/hits per run, if $now works.
     *
     * @param resource the JSON resource
     * @param sql           the sql statement to select timestamps
     * @throws Exception
     */
    @Test
    @Parameters({"task7", "sql2"})
    public void testTimestamps(String resource, String sql) throws Exception {
        createTimestampedLogs(sql, 100, "iw_IL", "Asia/Jerusalem"); // TODO make timezone/locale configurable
        create(resource);
        waitFor();
        waitForActive();
        Thread.sleep(12500L); // ensure at least two runs
        client("1").admin().indices().prepareRefresh(index).execute().actionGet();
        long hits = client("1").prepareSearch(index).execute().actionGet().getHits().getTotalHits();
        // just an estimation, at least two runs should deliver 50 hits each.
        assertTrue(hits > 99L);
    }

}
