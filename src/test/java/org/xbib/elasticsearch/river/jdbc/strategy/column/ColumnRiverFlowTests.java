package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.xbib.elasticsearch.action.river.state.RiverState;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverSource;
import org.xbib.elasticsearch.support.helper.AbstractRiverNodeTest;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ColumnRiverFlowTests extends AbstractRiverNodeTest {

    @Override
    public RiverSource getRiverSource() {
        return new ColumnRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        return new RiverContext();
    }

    @Test()
    @Parameters({"river-existedWhereClause"})
    public void testWriteLastRiverRunTimeToCustomRiverInfo(String riverResource) throws IOException {
        final Client client = client("1");
        setupContext(new MockRiverSource() {
            @Override
            public void fetch() {
            }
        }, riverResource);
        Map<String, Object> spec = (Map<String, Object>) riverSettings(riverResource).settings().get("jdbc");
        Map<String, String> loadedSettings = new JsonSettingsLoader().load(jsonBuilder().map(spec).string());
        Settings settings = settingsBuilder().put(loadedSettings).build();
        RiverFlow flow = new ColumnRiverFlow();
        flow.setRiverContext(context);
        flow.getFeeder()
                .setName(context.getRiverName())
                .setRiverState(new RiverState())
                .setSpec(spec)
                .setSettings(settings)
                .setClient(client)
                .run();
        client.admin().indices().refresh(Requests.refreshRequest("_river")).actionGet();
        assertLastRiverRunTimeExists(client);
    }

    @Test()
    @Parameters({"river-existedWhereClause"})
    public void testReadLastRiverRunTimeFromCustomRiverInfo(String riverResource) throws IOException {
        final Client client = client("1");
        final TimeValue lastRunAt = new TimeValue(600);
        setupContext(new MockRiverSource() {
            @Override
            public void fetch() {
                assertLastRunDateEquals(lastRunAt);
            }
        }, riverResource);
        writeLastRunDateToCustomRiverInfo(client, lastRunAt);
        Map<String, Object> spec = (Map<String, Object>) riverSettings(riverResource).settings().get("jdbc");
        Map<String, String> loadedSettings = new JsonSettingsLoader().load(jsonBuilder().map(spec).string());
        Settings settings = settingsBuilder().put(loadedSettings).build();
        RiverFlow flow = new ColumnRiverFlow();
        flow.setRiverContext(context);
        flow.getFeeder()
                .setRiverState(new RiverState())
                .setSpec(spec)
                .setSettings(settings)
                .setClient(client)
                .run();
    }

    private void setupContext(RiverSource riverSource, String riverResource) throws IOException {
        RiverSettings riverSettings = riverSettings(riverResource);
        context.setRiverName(new RiverName(index, type).getName());
        context.setRiverMouth(new MockRiverMouth());
        context.setRiverSource(riverSource);
        context.setRiverSettings(riverSettings.settings());
        context.columnEscape(true);
    }

    private void assertLastRiverRunTimeExists(Client client) {
        try {
            GetResponse get = client.prepareGet("_river",
                    context.getRiverName(),
                    ColumnRiverFlow.DOCUMENT).execute().actionGet();
            logger.info("get response = {}", get.getSourceAsMap());
            Map map = (Map) get.getSourceAsMap().get("jdbc");
            assertNotNull(map, "jdbc key not exists in custom info");
            assertNotNull(map.get(ColumnRiverFlow.LAST_RUN_TIME), "last run time should not be null");
        } catch (IndexMissingException e) {
            fail("custom info not found");
        }
    }

    private void assertLastRunDateEquals(TimeValue expectedLastRunAt) {
        Map map = (Map) context.getRiverSettings().get("jdbc");
        assertNotNull(map);
        assertEquals(map.get(ColumnRiverFlow.LAST_RUN_TIME), expectedLastRunAt);
    }

    private void writeLastRunDateToCustomRiverInfo(Client client, TimeValue lastRunAt) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("jdbc")
                .field(ColumnRiverFlow.LAST_RUN_TIME, lastRunAt.millis())
                .endObject()
                .endObject();
        client.prepareIndex().setIndex("_river").setType(context.getRiverName()).setId(ColumnRiverFlow.DOCUMENT)
                .setSource(builder.string()).execute().actionGet();
        client.admin().indices().refresh(Requests.refreshRequest("_river")).actionGet();
    }
}
