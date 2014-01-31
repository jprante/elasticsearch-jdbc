
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import java.io.IOException;

import org.elasticsearch.client.Client;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.support.AbstractRiverNodeTest;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;

public class SimpleRiverScriptTests extends AbstractRiverNodeTest {

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        return new RiverContext();
    }

    /**
     * Orders table star select
     *
     * @param riverResource
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Test
    @Parameters({"river1"})
    public void testSimpleRiverOnce(String riverResource) throws IOException, InterruptedException {
        Client client = client("1");
        RiverSettings settings = riverSettings(riverResource);
        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, client);
        river.once();
        Thread.sleep(3000L); // let the good things happen
        river.close();
    }

}
