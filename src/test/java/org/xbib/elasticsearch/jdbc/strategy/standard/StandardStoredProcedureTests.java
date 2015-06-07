/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;

public class StandardStoredProcedureTests extends AbstractSinkTest {

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    @Test
    @Parameters({"task8"})
    public void testSimpleStoredProcedure(String resource)
            throws Exception {
        perform(resource);
        boolean b = waitFor(source.getContext(), Context.State.IDLE, 5000L);
        logger.info("after wait for: {}", b);
        assertHits("1", 5);
        logger.info("got the five hits");
    }

    @Test
    @Parameters({"task9"})
    public void testRegisterStoredProcedure(String resource) throws Exception {
        perform(resource);
        boolean b = waitFor(source.getContext(), Context.State.IDLE, 5000L);
        logger.info("after wait for: {}", b);
        assertHits("1", 1);
        logger.info("got the hit");
        SearchResponse response = client("1").prepareSearch("my_index")
                .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        String resp = response.getHits().getHits()[0].getSource().toString();
        logger.info("resp={}", resp);
        assertEquals("{mySupplierName=Acme, Inc.}", resp);
    }

}
