package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;

public class StandardStoredProcedureTests extends AbstractStandardTest {

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    @Test
    @Parameters({"river8"})
    public void testSimpleStoredProcedure(String riverResource)
            throws Exception {
        create(riverResource);
        waitForInactive();
        assertHits("1", 5);
        logger.info("got the five hits");
    }

    @Test
    @Parameters({"river9"})
    public void testRegisterStoredProcedure(String riverResource) throws Exception {
        create(riverResource);
        waitForInactive();
        assertHits("1", 1);
        logger.info("got the hit");
        SearchResponse response = client("1").prepareSearch("my_jdbc_river_index")
                .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        String resp = response.getHits().getHits()[0].getSource().toString();
        logger.info("resp={}", resp);
        assertEquals("{mySupplierName=Acme, Inc.}", resp);
    }

}
