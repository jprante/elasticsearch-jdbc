
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.client.Client;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.support.AbstractRiverNodeTest;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;

public class SimpleRiverTests extends AbstractRiverNodeTest {

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
     *
     * @param riverResource the river resource
     * @throws Exception
     */
    @Test
    @Parameters({"river1"})
    public void testSimpleRiverOnce(String riverResource) throws Exception {
        Client client = client("1");
        RiverSettings settings = riverSettings(riverResource);
        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, client);
        river.start();
        Thread.sleep(10000L); // let the river flow
        river.close();
    }

}
