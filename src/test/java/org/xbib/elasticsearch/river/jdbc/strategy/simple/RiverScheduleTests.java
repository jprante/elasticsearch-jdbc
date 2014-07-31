package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.support.helper.AbstractRiverNodeTest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class RiverScheduleTests extends AbstractRiverNodeTest {

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        return new RiverContext();
    }

    /**
     * Product table star select, scheduled for more than two runs
     * @param riverResource the river resource
     * @param sql the SQL statement
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"river6", "sql1"})
    public void testSimpleSchedule(String riverResource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        createRiver(riverResource);
        Thread.sleep(12500L); // run more than twice
        client("1").admin().indices().prepareRefresh(index).execute().actionGet();
        long hits = client("1").prepareSearch(index).execute().actionGet().getHits().getTotalHits();
        assertThat(hits, greaterThan(100L));
    }

    /**
     * Test read and write of timestamps in a table. We create 100 timestamps over hour interval,
     * current timestamp $now is in the center.
     * Selecting timestamps from $now, there should be at least 50 rows/hits per run, if $now works.
     *
     * @param riverResource the river JSON resource
     * @param sql the sql statement to select timestamps
     * @throws Exception
     */
    @Test
    @Parameters({"river7", "sql2"})
    public void testTimestamps(String riverResource, String sql) throws Exception {
        createTimestampedLogs(sql, 100, "iw_IL", "Asia/Jerusalem"); // TODO make timezone/locale configurable
        createRiver(riverResource);
        Thread.sleep(12500L); // ensure at least two runs
        client("1").admin().indices().prepareRefresh(index).execute().actionGet();
        long hits = client("1").prepareSearch(index).execute().actionGet().getHits().getTotalHits();
        // just an estimation, at least two runs should deliver 50 hits each.
        assertThat(hits, greaterThan(99L));
    }

}
