package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;

public class StandardScriptTests extends AbstractStandardTest {

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
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
        create(riverResource);
        waitForInactive();
    }

}
