/*
 * Copyright (C) 2015 Jörg Prante
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

import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTime;
import org.xbib.elasticsearch.jdbc.strategy.Sink;
import org.xbib.elasticsearch.jdbc.strategy.standard.StandardContext;

import java.io.IOException;

public class ColumnContext<S extends ColumnSource> extends StandardContext<S> {

    public static final String LAST_RUN_TIME = "last_run_time";

    public static final String CURRENT_RUN_STARTED_TIME = "current_run_started_time";


    private DateTime lastRunTimestamp;

    /**
     * Contains overlap value for last run timestamp.
     */
    private TimeValue lastRunTimeStampOverlap;

    @Override
    public String strategy() {
        return "column";
    }

    @Override
    public ColumnContext newInstance() {
        return new ColumnContext();
    }

    public void setLastRunTimeStamp(DateTime dateTime) {
        this.lastRunTimestamp = dateTime;
    }

    public DateTime getLastRunTimestamp() {
        return lastRunTimestamp;
    }

    public ColumnContext setLastRunTimeStampOverlap(TimeValue lastRunTimeStampOverlap) {
        this.lastRunTimeStampOverlap = lastRunTimeStampOverlap;
        return this;
    }

    public TimeValue getLastRunTimeStampOverlap() {
        return lastRunTimeStampOverlap;
    }

    @Override
    protected void prepareContext(S source, Sink sink) throws IOException {
        super.prepareContext(source, sink);
        source.columnCreatedAt(getSettings().get("created_at", "created_at"));
        source.columnUpdatedAt(getSettings().get("updated_at", "updated_at"));
        source.columnDeletedAt(getSettings().get("deleted_at"));
        source.columnEscape(getSettings().getAsBoolean("column_escape", true));
        TimeValue lastRunTimeStampOverlap = getSettings().getAsTime("last_run_timestamp_overlap", TimeValue.timeValueSeconds(0));
        setLastRunTimeStampOverlap(lastRunTimeStampOverlap);
    }

    @Override
    public void fetch() throws Exception {
        DateTime currentTime = new DateTime();
        getSource().fetch();
        setLastRunTimeStamp(currentTime);
    }

}
