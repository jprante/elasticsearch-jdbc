package org.xbib.elasticsearch.jdbc.strategy.column;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.common.client.Ingest;
import org.xbib.elasticsearch.common.client.IngestFactory;
import org.xbib.elasticsearch.common.client.node.BulkNodeClient;
import org.xbib.elasticsearch.common.state.State;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.mock.MockMouth;
import org.xbib.elasticsearch.jdbc.strategy.mock.MockJDBCSource;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class ColumnStrategyFlowTests extends AbstractColumnStrategyTest {

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
        }, resource);
        Map<String, Object> settingsMap = taskSettings(resource);
        Settings settings = settingsBuilder().put(settingsMap).build();
        ColumnContext context = new ColumnContext();
        Map<String, Object> def = (Map<String, Object>) settingsMap.get("jdbc");
        context.setDefinition(def);
        ColumnFlow flow = new ColumnFlow();
        flow.setName("column")
                .setSettings(settings)
                .setClient(client("1"))
                .setIngestFactory(createIngestFactory(settings, client("1")))
                .execute(context);
        assertNotNull(context.getState().getMap().get(ColumnFlow.LAST_RUN_TIME));
    }

    @Test
    @Parameters({"existedWhereClause"})
    public void testReadLastRunTime(String resource) throws Exception {
        final DateTime lastRunAt = new DateTime(new DateTime().getMillis() - 600);
        createContext(new MockJDBCSource() {
            @Override
            public void fetch() {
                DateTime readlastRunAt = (DateTime) context.getState().getMap().get(ColumnFlow.LAST_RUN_TIME);
                assertNotNull(readlastRunAt);
                assertEquals(readlastRunAt, lastRunAt);

            }
        }, resource);
        State state = new State();
        context.setState(state);
        context.getState().getMap().put(ColumnFlow.LAST_RUN_TIME, lastRunAt);
        Map<String, Object> settingsMap = taskSettings(resource);
        Settings settings = settingsBuilder().put(settingsMap).build();
        ColumnContext context = new ColumnContext();
        Map<String, Object> def = (Map<String, Object>) settingsMap.get("jdbc");
        context.setDefinition(def);
        ColumnFlow flow = new ColumnFlow();
        flow.setName("column")
                .setSettings(settings)
                .setClient(client("1"))
                .setIngestFactory(createIngestFactory(settings, client("1")))
                .execute(context);
    }

    private void createContext(JDBCSource JDBCSource, String resource) throws IOException {
        context.columnEscape(true)
                .setMouth(new MockMouth())
                .setSource(JDBCSource);
    }

    private IngestFactory createIngestFactory(final Settings settings, final Client client) {
        return new IngestFactory() {
            @Override
            public Ingest create() {
                Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 100);
                Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2);
                ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m"));
                TimeValue maxrequestwait = settings.getAsTime("max_request_wait", TimeValue.timeValueSeconds(60));
                TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
                return new BulkNodeClient()
                        .maxActionsPerBulkRequest(maxbulkactions)
                        .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                        .maxRequestWait(maxrequestwait)
                        .maxVolumePerBulkRequest(maxvolume)
                        .flushIngestInterval(flushinterval)
                        .newClient(client);
            }
        };
    }

}
