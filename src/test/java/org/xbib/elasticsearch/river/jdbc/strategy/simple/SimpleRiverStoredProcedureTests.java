package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.river.jdbc.RiverSource;

public class SimpleRiverStoredProcedureTests extends AbstractSimpleRiverTest {

    @Override
    public RiverSource newRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public SimpleRiverContext newRiverContext() {
        return new SimpleRiverContext();
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
