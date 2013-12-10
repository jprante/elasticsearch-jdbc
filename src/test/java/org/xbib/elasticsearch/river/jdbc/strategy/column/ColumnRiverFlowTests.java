/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.xbib.elasticsearch.river.jdbc.strategy.column.ColumnRiverFlow;
import org.xbib.elasticsearch.river.jdbc.strategy.column.ColumnRiverSource;
import java.io.IOException;
import java.util.Map;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import org.xbib.elasticsearch.river.jdbc.RiverFlow;
import static org.xbib.elasticsearch.river.jdbc.RiverFlow.ID_INFO_RIVER_INDEX;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import static org.xbib.elasticsearch.river.jdbc.strategy.column.ColumnRiverFlow.LAST_RUN_TIME;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverSource;
import org.xbib.elasticsearch.river.jdbc.support.AbstractRiverNodeTest;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class ColumnRiverFlowTests extends AbstractRiverNodeTest {

    @Override
    public RiverSource getRiverSource() {
        return new ColumnRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        RiverContext context = new RiverContext();
        return context;
    }
    
    @Test()
    @Parameters({"river1"})
    public void testWriteLastRiverRunTimeToCustomRiverInfo(String riverResource) throws IOException {

        final Client client = client("1");
        setupContext(new MockRiverSource(){
            @Override
            public String fetch() {
                return null;
            }
        }, riverResource, client);
        
        RiverFlow flow = new ColumnRiverFlow();
        flow.riverContext(context);
        
        flow.move();
        
        assertLastRiverRunTimeExists(client);
    }
    
    @Test()
    @Parameters({"river1"})
    public void testReadLastRiverRunTimeFromCustomRiverInfo(String riverResource) throws IOException {
        final Client client = client("1");
        
        final TimeValue lastRunAt = new TimeValue(600);
        
        setupContext(new MockRiverSource(){
            @Override
            public String fetch() {
                assertLastRunDateEquals(lastRunAt);
                return null;
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
        context.riverIndexName("_river");
        context.riverName(new RiverName(INDEX, TYPE).getName());
        context.riverSettings(riverSettings.settings());
        context.columnEscape(true);
    }
    
    private void assertLastRiverRunTimeExists(Client client) {
        try {
            GetResponse get = client.prepareGet(context.riverIndexName(), context.riverName(), ID_INFO_RIVER_INDEX).execute().actionGet();
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
                .add(Requests.indexRequest(context.riverIndexName())
                    .type(context.riverName())
                    .id(ID_INFO_RIVER_INDEX)
                    .source(builder)
                )
                .execute()
                .actionGet();
    }
}
