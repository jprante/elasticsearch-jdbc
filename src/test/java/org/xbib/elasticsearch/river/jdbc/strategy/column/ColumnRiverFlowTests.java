
package org.xbib.elasticsearch.river.jdbc.strategy.column;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverSource;
import org.xbib.elasticsearch.river.jdbc.support.AbstractRiverNodeTest;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;

import static org.xbib.elasticsearch.river.jdbc.RiverFlow.ID_INFO_RIVER_INDEX;
import static org.xbib.elasticsearch.river.jdbc.strategy.column.ColumnRiverFlow.LAST_RUN_TIME;

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
        setupContext(new MockRiverSource(){
            @Override
            public void fetch() {
            }
        }, riverResource, client);
        
        RiverFlow flow = new ColumnRiverFlow();
        flow.riverContext(context);
        
        flow.move();
        
        assertLastRiverRunTimeExists(client);
    }
    
    @Test()
    @Parameters({"river-existedWhereClause"})
    public void testReadLastRiverRunTimeFromCustomRiverInfo(String riverResource) throws IOException {
        final Client client = client("1");
        
        final TimeValue lastRunAt = new TimeValue(600);
        
        setupContext(new MockRiverSource(){
            @Override
            public void fetch() {
                assertLastRunDateEquals(lastRunAt);
            }
        }, riverResource, client);
        
        
        writeLastRunDateToCustomRiverInfo(client, lastRunAt);

        
        RiverFlow flow = new ColumnRiverFlow();
        flow.riverContext(context);
        
        flow.move();
    }
    
    private void setupContext(RiverSource riverSource, String riverResource, final Client client) throws IOException {
        RiverSettings riverSettings = riverSettings(riverResource);      

        context.riverMouth(new MockRiverMouth(){
            @Override
            public Client client() {
                return client;
            }            
        });
        context.riverSource(riverSource);
        context.riverName(new RiverName(INDEX, TYPE).getName());
        context.riverSettings(riverSettings.settings());
        context.columnEscape(true);
    }
    
    private void assertLastRiverRunTimeExists(Client client) {
        try {
            GetResponse get = client.prepareGet("_river", context.riverName(), ID_INFO_RIVER_INDEX).execute().actionGet();
            Map map = (Map) get.getSourceAsMap().get("jdbc");

            assertNotNull(map, "jdbc key not exists in custom info");        
            assertNotNull(map.get(ColumnRiverFlow.LAST_RUN_TIME), "last run time should not be null");
        } catch(IndexMissingException e) {
            fail("custom info not found");
        }
    }
    
    private void assertLastRunDateEquals(TimeValue expectedLastRunAt) {
        Map map = (Map) context.riverSettings().get("jdbc");
        assertNotNull(map);
        assertEquals(map.get(ColumnRiverFlow.LAST_RUN_TIME), expectedLastRunAt);        
    }

    private void writeLastRunDateToCustomRiverInfo(Client client, TimeValue lastRunAt) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder
                .startObject()
                .startObject("jdbc")
                .field(LAST_RUN_TIME, lastRunAt.millis())
                .endObject()
                .endObject();

        client.prepareBulk()
                .add(Requests.indexRequest("_river")
                    .type(context.riverName())
                    .id(ID_INFO_RIVER_INDEX)
                    .source(builder)
                )
                .execute()
                .actionGet();
    }
}
