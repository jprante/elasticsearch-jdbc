package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.river.jdbc.RiverSource;

public class SimpleRiverDataTests extends AbstractSimpleRiverTest {

    @Override
    public RiverSource newRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public SimpleRiverContext newRiverContext() {
        return new SimpleRiverContext();
    }

    /**
     * Start the river and execute a simple star query
     *
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"river1", "sql1"})
    public void testSimpleRiverOnce(String riverResource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        performRiver(riverResource);
        logger.info("success");
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
        performRiver(riverResource);
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
        performRiver(riverResource);
        assertHits("1", 104);
        assertTimestampSort("1", 104);
        logger.info("success");
    }

}
