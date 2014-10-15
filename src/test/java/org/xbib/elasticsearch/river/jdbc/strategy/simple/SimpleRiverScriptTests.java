package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.river.jdbc.RiverSource;

public class SimpleRiverScriptTests extends AbstractSimpleRiverTest {

    @Override
    public RiverSource newRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public SimpleRiverContext newRiverContext() {
        return new SimpleRiverContext();
    }

    /**
     * Orders table (star query)
     *
     * @param riverResource the river definition
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"river1"})
    public void testSimpleRiverOnce(String riverResource) throws Exception {
        createRiver(riverResource);
        waitForInactiveRiver();
    }

}
