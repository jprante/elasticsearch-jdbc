package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.support.helper.AbstractRiverNodeTest;

public class RiverDataTests extends AbstractRiverNodeTest {

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        return new RiverContext();
    }

    /**
     * Start the river and execute a simple star query
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"river1", "sql1"})
    public void testSimpleRiverOnce(String riverResource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        createRiver(riverResource);
        waitForRiverEnabled();
        waitForInactiveRiver();
    }

    /**
     * Product table (star query)
     *
     * @param riverResource the river resource
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"river2", "sql1"})
    public void testSimpleRiverRandom(String riverResource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        createRiver(riverResource);
        waitForRiverEnabled();
        waitForInactiveRiver();
        assertHits("1", 104);
        logger.info("success");
    }

    /**
     * Product table
     *
     * @param riverResource the river
     * @param sql           the SQL statement
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"river3", "sql1"})
    public void testSimpleRiverMaxrows(String riverResource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        createRiver(riverResource);
        waitForRiverEnabled();
        waitForInactiveRiver();
        assertHits("1", 104);
        logger.info("success");
    }

}
