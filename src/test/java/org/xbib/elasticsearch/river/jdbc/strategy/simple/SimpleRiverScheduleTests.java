package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.river.jdbc.RiverSource;

public class SimpleRiverScheduleTests extends AbstractSimpleRiverTest {

    @Override
    public RiverSource newRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public SimpleRiverContext newRiverContext() {
        return new SimpleRiverContext();
    }

    /**
     * Product table star select, scheduled for more than two runs
     *
     * @param riverResource the river resource
     * @param sql           the SQL statement
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"river6", "sql1"})
    public void testSimpleSchedule(String riverResource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        createRiver(riverResource);
        waitForRiver();
        waitForActiveRiver();
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
     * @param riverResource the river JSON resource
     * @param sql           the sql statement to select timestamps
     * @throws Exception
     */
    @Test
    @Parameters({"river7", "sql2"})
    public void testTimestamps(String riverResource, String sql) throws Exception {
        createTimestampedLogs(sql, 100, "iw_IL", "Asia/Jerusalem"); // TODO make timezone/locale configurable
        createRiver(riverResource);
        waitForRiver();
        waitForActiveRiver();
        Thread.sleep(12500L); // ensure at least two runs
        client("1").admin().indices().prepareRefresh(index).execute().actionGet();
        long hits = client("1").prepareSearch(index).execute().actionGet().getHits().getTotalHits();
        // just an estimation, at least two runs should deliver 50 hits each.
        assertTrue(hits > 99L);
    }

}
