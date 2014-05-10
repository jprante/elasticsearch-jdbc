package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.support.helper.AbstractRiverNodeTest;

import java.sql.Connection;
import java.sql.Statement;

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
    @Parameters({"river5", "sql1", "sql2"})
    public void testSimpleStoredProcedure(String riverResource, String sql, String storedProcSQL)
            throws Exception {
        createRandomProducts(sql, 100);
        // create stored procedure
        Connection connection = source.getConnectionForWriting();
        Statement statement = connection.createStatement();
        statement.execute(storedProcSQL);
        statement.close();
        source.closeWriting();
        createRiver(riverResource);
        waitForRiverEnabled();
        waitForInactiveRiver();
    }

}
