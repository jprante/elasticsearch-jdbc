package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.RiverName;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.client.Ingest;
import org.xbib.elasticsearch.plugin.jdbc.client.IngestFactory;
import org.xbib.elasticsearch.plugin.jdbc.client.node.BulkNodeClient;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverSource;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class ColumnRiverFlowTests extends AbstractColumnRiverTest {

    @Override
    public ColumnRiverSource newRiverSource() {
        return new ColumnRiverSource();
    }

    @Override
    public ColumnRiverContext newRiverContext() {
        return new ColumnRiverContext();
    }

    @Test
    @Parameters({"river-existedWhereClause"})
    public void testWriteLastRiverRunTime(String riverResource) throws Exception {
        createRiverContext(new MockRiverSource() {
            @Override
            public void fetch() {
            }
        }, riverResource);
        Map<String, Object> settingsMap = riverSettings(riverResource).settings();
        Settings settings = settingsBuilder().put(settingsMap).build();
        ColumnRiverContext riverContext = new ColumnRiverContext();
        Map<String, Object> def = (Map<String, Object>) settingsMap.get("jdbc");
        riverContext.setDefinition(def);
        ColumnRiverFlow flow = new ColumnRiverFlow();
        flow.setRiverName(new RiverName("jdbc", "column"))
                .setSettings(settings)
                .setClient(client("1"))
                .setIngestFactory(createIngestFactory(settings, client("1")))
                .execute(riverContext);
        assertNotNull(riverContext.getRiverState().getMap().get(ColumnRiverFlow.LAST_RUN_TIME));
    }

    @Test
    @Parameters({"river-existedWhereClause"})
    public void testReadLastRiverRunTime(String riverResource) throws Exception {
        final DateTime lastRunAt = new DateTime(new DateTime().getMillis() - 600);
        createRiverContext(new MockRiverSource() {
            @Override
            public void fetch() {
                DateTime readlastRunAt = (DateTime) context.getRiverState().getMap().get(ColumnRiverFlow.LAST_RUN_TIME);
                assertNotNull(readlastRunAt);
                assertEquals(readlastRunAt, lastRunAt);

            }
        }, riverResource);
        RiverState riverState = new RiverState();
        context.setRiverState(riverState);
        context.getRiverState().getMap().put(ColumnRiverFlow.LAST_RUN_TIME, lastRunAt);
        Map<String, Object> settingsMap = riverSettings(riverResource).settings();
        Settings settings = settingsBuilder().put(settingsMap).build();
        ColumnRiverContext riverContext = new ColumnRiverContext();
        Map<String, Object> def = (Map<String, Object>) settingsMap.get("jdbc");
        riverContext.setDefinition(def);
        ColumnRiverFlow flow = new ColumnRiverFlow();
        flow.setRiverName(new RiverName("jdbc", "column"))
                .setSettings(settings)
                .setClient(client("1"))
                .setIngestFactory(createIngestFactory(settings, client("1")))
                .execute(riverContext);
    }

    private void createRiverContext(RiverSource riverSource, String riverResource) throws IOException {
        context.columnEscape(true)
                .setRiverMouth(new MockRiverMouth())
                .setRiverSource(riverSource);
    }

    private IngestFactory createIngestFactory(final Settings settings, final Client client) {
        return new IngestFactory() {
            @Override
            public Ingest create() {
                Integer maxbulkactions = settings.getAsInt("max_bulk_actions", 100);
                Integer maxconcurrentbulkrequests = settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2);
                ByteSizeValue maxvolume = settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m"));
                TimeValue flushinterval = settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5));
                return new BulkNodeClient()
                        .maxActionsPerBulkRequest(maxbulkactions)
                        .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                        .maxVolumePerBulkRequest(maxvolume)
                        .flushIngestInterval(flushinterval)
                        .newClient(client);
            }
        };
    }

}
