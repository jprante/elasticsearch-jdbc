package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.support.helper.AbstractRiverNodeTest;

public class RiverStoredProcedureTests extends AbstractRiverNodeTest {

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        return new RiverContext();
    }

    @Test
    @Parameters({"river8"})
    public void testSimpleStoredProcedure(String riverResource)
            throws Exception {
        createRiver(riverResource);
        waitForInactiveRiver();
        assertHits("1", 5);
        logger.info("got the five hits");
    }

    @Test
    @Parameters({"river9"})
    public void testRegisterStoredProcedure(String riverResource) throws Exception {
        createRiver(riverResource);
        waitForInactiveRiver();
        assertHits("1", 1);
        logger.info("got the hit");
        SearchResponse response = client("1").prepareSearch("my_jdbc_river_index")
                .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        String resp = response.getHits().getHits()[0].getSource().toString();
        logger.info("resp={}", resp);
        assertEquals("{mySupplierName=Acme, Inc.}", resp);
    }

}
