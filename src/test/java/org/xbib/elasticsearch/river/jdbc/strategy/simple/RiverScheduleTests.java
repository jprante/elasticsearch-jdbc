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
     * Product table star select, scheduled for more than one run
     * @param riverResource the river resource
     * @param sql the SQL statement
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"river6", "sql1"})
    public void testSimpleSchedule(String riverResource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        createRiver(riverResource);
        Thread.sleep(12500L);
        client("1").admin().indices().prepareRefresh(index).execute().actionGet();
        assertThat(client("1").prepareSearch(index).execute().actionGet().getHits().getTotalHits(),
                greaterThan(100L));
    }

}
