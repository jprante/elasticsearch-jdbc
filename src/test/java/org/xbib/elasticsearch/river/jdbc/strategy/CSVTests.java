package org.xbib.elasticsearch.river.jdbc.strategy;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class CSVTests extends Assert {

    private final ESLogger logger = ESLoggerFactory.getLogger(CSVTests.class.getName());

    @Test
    public void testCSV() throws Exception {
        RiverSource source = new SimpleRiverSource()
                .driver("org.xbib.jdbc.csv.CsvDriver")
                .url("jdbc:csv:src/test/resources/csv?separator=;")
                .user("")
                .password("");
        RiverContext context = new RiverContext()
                .riverSource(source)
                .retries(1)
                .maxRetryWait(TimeValue.timeValueSeconds(3));
        context.contextualize();
        Connection connection = source.connectionForReading();
        assertNotNull(connection);

        // fetch some data
        PreparedStatement statement = source.prepareQuery("select * from Gesamtfinanzplan");
        ResultSet results = source.executeQuery(statement);
        for (int i = 0; i < 36; i++) {
            assertTrue(results.next());
            logger.info("{} {} {} {} {} {} {} ",
                    results.getString(1),
                    results.getString(2),
                    results.getString(3),
                    results.getString(4),
                    results.getString(5),
                    results.getString(6),
                    results.getString(7)
            );

        }
        source.close(results);
        source.close(statement);


        source.closeReading();
    }
}
