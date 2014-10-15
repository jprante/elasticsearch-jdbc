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

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ColumnRiverContext extends SimpleRiverContext {
    /**
     * Column name that contains creation time (for column strategy)
     */
    private String columnCreatedAt;

    /**
     * Column name that contains last update time (for column strategy)
     */
    private String columnUpdatedAt;

    /**
     * Column name that contains deletion time (for column strategy)
     */
    private String columnDeletedAt;

    /**
     * Contains overlap value for last run timestamp.
     */
    private TimeValue lastRunTimeStampOverlap;

    /**
     * Columns name should be automatically escaped by proper db quote mark or not (for column strategy)
     */
    private boolean columnEscape;

    public ColumnRiverContext columnUpdatedAt(String updatedAt) {
        this.columnUpdatedAt = updatedAt;
        return this;
    }

    public String columnUpdatedAt() {
        return columnUpdatedAt;
    }

    public ColumnRiverContext columnCreatedAt(String createdAt) {
        this.columnCreatedAt = createdAt;
        return this;
    }

    public String columnCreatedAt() {
        return columnCreatedAt;
    }

    public ColumnRiverContext columnDeletedAt(String deletedAt) {
        this.columnDeletedAt = deletedAt;
        return this;
    }

    public String columnDeletedAt() {
        return columnDeletedAt;
    }

    public ColumnRiverContext columnEscape(boolean escape) {
        this.columnEscape = escape;
        return this;
    }

    public boolean columnEscape() {
        return this.columnEscape;
    }

    public TimeValue getLastRunTimeStampOverlap() {
        return lastRunTimeStampOverlap;
    }

    public ColumnRiverContext setLastRunTimeStampOverlap(TimeValue lastRunTimeStampOverlap) {
        this.lastRunTimeStampOverlap = lastRunTimeStampOverlap;
        return this;
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = super.asMap();
        try {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("columnCreatedAt", columnCreatedAt)
                    .field("columnUpdatedAt", columnUpdatedAt)
                    .field("columnDeletedAt", columnDeletedAt)
                    .field("columnEscape", columnEscape)
                    .endObject();
            map.putAll(XContentHelper.convertToMap(builder.bytes(), true).v2());
            return map;
        } catch (IOException e) {
            // should really not happen
            return new HashMap<String, Object>();
        }
    }
}
