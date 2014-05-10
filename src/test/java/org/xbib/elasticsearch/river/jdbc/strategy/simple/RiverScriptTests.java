package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.support.helper.AbstractRiverNodeTest;

public class RiverScriptTests extends AbstractRiverNodeTest {

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        return new RiverContext();
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
        waitForRiverEnabled();
        waitForInactiveRiver();
    }

}
