/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.river.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverFlow;

import java.io.IOException;
import java.util.Map;

/**
 * River flow implementation for the 'column' strategy
 *
 * @author <a href="piotr.sliwa@zineinc.com">Piotr Śliwa</a>
 */
public class ColumnRiverFlow extends SimpleRiverFlow<ColumnRiverContext> {

    private static final ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.ColumnRiverFlow");

    public static final String LAST_RUN_TIME = "last_run_time";

    public static final String CURRENT_RUN_STARTED_TIME = "current_run_started_time";

    @Override
    public String strategy() {
        return "column";
    }

    @Override
    public ColumnRiverFlow newInstance() {
        return new ColumnRiverFlow();
    }

    @Override
    public ColumnRiverContext newRiverContext() {
        return new ColumnRiverContext();
    }

    @Override
    protected ColumnRiverContext fillRiverContext(ColumnRiverContext riverContext, RiverState state,
                                                  RiverSource riverSource,
                                                  RiverMouth riverMouth) throws IOException {
        ColumnRiverContext context = super.fillRiverContext(riverContext, state, riverSource, riverMouth);
        // defaults for column strategy
        Map<String, Object> params = riverContext.getDefinition();
        String columnCreatedAt = XContentMapValues.nodeStringValue(params.get("created_at"), "created_at");
        String columnUpdatedAt = XContentMapValues.nodeStringValue(params.get("updated_at"), "updated_at");
        String columnDeletedAt = XContentMapValues.nodeStringValue(params.get("deleted_at"), null);
        boolean columnEscape = XContentMapValues.nodeBooleanValue(params.get("column_escape"), true);
        TimeValue lastRunTimeStampOverlap = XContentMapValues.nodeTimeValue(params.get("last_run_timestamp_overlap"),
                TimeValue.timeValueSeconds(0));
        context.columnCreatedAt(columnCreatedAt)
                .columnUpdatedAt(columnUpdatedAt)
                .columnDeletedAt(columnDeletedAt)
                .columnEscape(columnEscape)
                .setLastRunTimeStampOverlap(lastRunTimeStampOverlap);
        return context;
    }

    @Override
    protected void fetch(RiverContext riverContext) throws Exception {
        DateTime currentTime = new DateTime();
        riverContext.getRiverSource().fetch();
        riverContext.getRiverState().getMap().put(LAST_RUN_TIME, currentTime);
    }

}
