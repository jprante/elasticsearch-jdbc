package org.xbib.elasticsearch.jdbc.strategy.column;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.mock.MockSink;
import org.xbib.elasticsearch.jdbc.strategy.mock.MockJDBCSource;

public class ColumnStrategyContextTests extends AbstractColumnStrategyTest {

    @Override
    public ColumnSource newSource() {
        return new ColumnSource();
    }

    @Override
    public ColumnContext newContext() {
        return new ColumnContext();
    }

    @Test
    @Parameters({"existedWhereClause"})
    @SuppressWarnings("unchecked")
    public void testWriteLastRunTime(String resource) throws Exception {
        Settings settings = createSettings(resource);
        context = newContext();
        MockJDBCSource source = new MockJDBCSource() {
            @Override
            public void fetch() {
            }
        };
        context.setSettings(settings)
                .setSink(new MockSink())
                .setSource(source)
                .setIngestFactory(createIngestFactory(settings));
        context.execute();
        assertNotNull(context.getLastRunTimestamp());
    }

    @Test
    @Parameters({"existedWhereClause"})
    @SuppressWarnings("unchecked")
    public void testReadLastRunTime(String resource) throws Exception {
        Settings settings = createSettings(resource);
        final DateTime lastRunAt = new DateTime(new DateTime().getMillis() - 600);
        context = newContext();
        MockJDBCSource source = new MockJDBCSource() {
            @Override
            public void fetch() {
                DateTime readlastRunAt = context.getLastRunTimestamp();
                assertNotNull(readlastRunAt);
                assertEquals(readlastRunAt, lastRunAt);

            }
        };
        context.setSettings(settings)
                .setSink(new MockSink())
                .setSource(source)
                .setIngestFactory(createIngestFactory(settings));
        context.setLastRunTimeStamp(lastRunAt);
        context.execute();
    }

}
