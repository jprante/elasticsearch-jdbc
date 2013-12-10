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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexMissingException;
import org.xbib.elasticsearch.river.jdbc.JDBCRiver;
import static org.xbib.elasticsearch.river.jdbc.RiverFlow.ID_INFO_RIVER_INDEX;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverFlow;

/**
 * River flow implementation for the 'column' strategy
 * 
 * @author Piotr Śliwa <piotr.sliwa@zineinc.com>
 */
public class ColumnRiverFlow extends SimpleRiverFlow {

    private final ESLogger logger = ESLoggerFactory.getLogger(ColumnRiverFlow.class.getName());
    
    static final String LAST_RUN_TIME = "last_run_time";
    static final String CURRENT_RUN_STARTED_TIME = "current_run_started_time";    
    
    @Override
    public String strategy() {
        return "column";
    }
    
    @Override
    public void move() {
        try {
            TimeValue lastRunTime = readLastRunTimeFromCustomInfo();
            TimeValue currentTime = new TimeValue(new java.util.Date().getTime());

            writeTimesToJdbcSettings(lastRunTime, currentTime);            
            
            context.riverSource().fetch();
            
            writeCustomInfo(currentTime.millis());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            abort = true;
        }
    }
    
    private TimeValue readLastRunTimeFromCustomInfo() throws IOException {
        try {
            GetResponse response = client().prepareGet(context.riverIndexName(), context.riverName(), ID_INFO_RIVER_INDEX).execute().actionGet();

            if(response != null && response.isExists()) {
                Map jdbcState = (Map) response.getSourceAsMap().get("jdbc");

                if(jdbcState != null) {
                    Number lastRunTime = (Number) jdbcState.get(LAST_RUN_TIME);

                    if(lastRunTime != null) {
                        return new TimeValue(lastRunTime.longValue());
                    }
                } else {
                    throw new IOException("can't retrieve previously persisted state from " + context.riverIndexName() + "/" + context.riverName());
                }
            }
        } catch(IndexMissingException e) {
            logger.warn("river state missing: {}/{}/{}", context.riverIndexName(), context.riverName(), ID_INFO_RIVER_INDEX);
        }
        
        return null;
    }
    
    private Client client() {
        return context.riverMouth().client();
    }
    
    private void writeCustomInfo(long lastRunAt) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder
                .startObject()
                .startObject("jdbc")
                .field(LAST_RUN_TIME, lastRunAt)
                .endObject()
                .endObject();

        client().prepareBulk()
                .add(Requests.indexRequest(context.riverIndexName())
                    .type(context.riverName())
                    .id(ID_INFO_RIVER_INDEX)
                    .source(builder)
                )
                .execute()
                .actionGet();
    }

    private void writeTimesToJdbcSettings(TimeValue lastRunTime, TimeValue currentTime) {
        Map<String, Object> jdbcSettings = (Map<String, Object>) context.riverSettings().get(JDBCRiver.TYPE);
        
        if(jdbcSettings == null) {
            jdbcSettings = new HashMap<String, Object>();
            context.riverSettings().put(JDBCRiver.TYPE, jdbcSettings);
        }

        jdbcSettings.put(LAST_RUN_TIME, lastRunTime);
        jdbcSettings.put(CURRENT_RUN_STARTED_TIME, currentTime);
    }
}
