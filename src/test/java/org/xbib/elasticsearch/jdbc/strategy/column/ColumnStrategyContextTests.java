package org.xbib.elasticsearch.jdbc.strategy.column;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.mock.MockSink;
import org.xbib.elasticsearch.jdbc.strategy.mock.MockJDBCSource;

import java.io.IOException;

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
    public void testWriteLastRunTime(String resource) throws Exception {
        createContext(new MockJDBCSource() {
            @Override
            public void fetch() {
            }
        });
        Settings settings = createSettings(resource);
        //Map<String, Object> def = (Map<String, Object>) settingsMap.get("jdbc");
        //context.setDefinition(def);
        //ColumnFlow flow = new ColumnFlow();
        context = newContext();
        context.setSettings(settings);
        context.execute();
        assertNotNull(context.getLastRunTimestamp() /*context.getState().getMap().get(ColumnFlow.LAST_RUN_TIME)*/);
    }

    @Test
    @Parameters({"existedWhereClause"})
    public void testReadLastRunTime(String resource) throws Exception {
        Settings settings = createSettings(resource);
        final DateTime lastRunAt = new DateTime(new DateTime().getMillis() - 600);
        createContext(new MockJDBCSource() {
            @Override
            public void fetch() {
                DateTime readlastRunAt = context.getLastRunTimestamp();
                //(DateTime) context.getState().getMap().get(ColumnFlow.LAST_RUN_TIME);
                assertNotNull(readlastRunAt);
                assertEquals(readlastRunAt, lastRunAt);

            }
        });
        /*State state = new State();
        context.setState(state);
        context.getState().getMap().put(ColumnFlow.LAST_RUN_TIME, lastRunAt);*/
        //ColumnContext context = new ColumnContext();
        //Map<String, Object> def = (Map<String, Object>) settingsMap.get("jdbc");
        //context.setDefinition(def);
        //ColumnFlow flow = new ColumnFlow();
        context.setSettings(settings);
        context.setLastRunTimeStamp(lastRunAt);
        context.execute();
    }

    private void createContext(JDBCSource source) throws IOException {
        context = newContext();
        context.columnEscape(true)
                .setSink(new MockSink())
                .setSource(source);
    }
}
