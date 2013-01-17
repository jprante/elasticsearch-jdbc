package org.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.jdbc.JDBCRiver;
import org.elasticsearch.river.jdbc.RiverSource;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;

public class SimpleRiverScriptTests extends AbstractRiverNodeTest {


    private final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverMouthTests.class.getName());
    private Client client;

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
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
        startNode("1");
        client = client("1");
        RiverSettings settings = riverSettings(riverResource);
        JDBCRiver river = new JDBCRiver(new RiverName(INDEX, TYPE), settings, "_river", client);
        river.once();
        Thread.sleep(3000L); // let the good things happen
        river.close();
    }

}
