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
package org.xbib.elasticsearch.jdbc.strategy.column;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.xbib.elasticsearch.common.task.Task;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.Mouth;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardFlow;

import java.io.IOException;
import java.util.Map;

/**
 * Flow implementation for the 'column' strategy
 *
 * @author <a href="piotr.sliwa@zineinc.com">Piotr Śliwa</a>
 */
public class ColumnFlow extends StandardFlow<ColumnContext> {

    private static final ESLogger logger = ESLoggerFactory.getLogger("jdbc");

    public static final String LAST_RUN_TIME = "last_run_time";

    public static final String CURRENT_RUN_STARTED_TIME = "current_run_started_time";

    @Override
    public String strategy() {
        return "column";
    }

    @Override
    public ColumnFlow newInstance() {
        return new ColumnFlow();
    }

    @Override
    public ColumnContext newContext() {
        return new ColumnContext();
    }

    @Override
    protected ColumnContext fillContext(ColumnContext columnContext, Task task,
                                        JDBCSource JDBCSource,
                                        Mouth mouth) throws IOException {
        ColumnContext context = super.fillContext(columnContext, task, JDBCSource, mouth);
        // defaults for column strategy
        Map<String, Object> params = context.getDefinition();
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
    protected void fetch(Context context) throws Exception {
        DateTime currentTime = new DateTime();
        context.getSource().fetch();
        context.getTask().getMap().put(LAST_RUN_TIME, currentTime);
    }

}
