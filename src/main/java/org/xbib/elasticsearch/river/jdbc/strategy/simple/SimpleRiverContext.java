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
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.plugin.jdbc.util.SQLCommand;
import org.xbib.elasticsearch.river.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * The river context consists of the parameters that span source and mouth settings.
 * It represents the river state, for supporting the river task execution, and river scripting.
 */
public class SimpleRiverContext implements RiverContext {

    private final static ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.SimpleRiverContext");

    private Map<String, Object> definition;

    /**
     * The state of the river
     */
    private RiverState riverState;

    /**
     * The metrics
     */
    private MeterMetric metric;

    /**
     * The source of the river
     */
    private RiverSource source;

    /**
     * The target of the river
     */
    private RiverMouth mouth;

    /**
     * Autocomit enabled or not
     */
    private boolean autocommit;

    /**
     * The fetch size
     */
    private int fetchSize;

    /**
     * The maximum numbe rof rows per statement execution
     */
    private int maxRows;

    /**
     * The number of retries
     */
    private int retries = 1;

    /**
     * The time to wait between retries
     */
    private TimeValue maxretrywait = TimeValue.timeValueSeconds(30);

    private int rounding;

    private int scale = -1;

    private String resultSetType = "TYPE_FORWARD_ONLY";

    private String resultSetConcurrency = "CONCUR_UPDATABLE";

    private boolean shouldIgnoreNull;

    private boolean shouldPrepareResultSetMetadata;

    private boolean shouldPrepareDatabaseMetadata;

    private Map<String, Object> lastResultSetMetadata = new HashMap<String, Object>();

    private Map<String, Object> lastDatabaseMetadata = new HashMap<String, Object>();

    private long lastRowCount;

    private Map<String, Object> columnNameMap;

    private Map<String, Object> lastRow = new HashMap<String, Object>();

    private List<SQLCommand> sql;

    private boolean isTimestampDiffSupported;

    private int queryTimeout;

    private Map<String, Object> connectionProperties = new HashMap<String, Object>();

    private boolean shouldTreatBinaryAsString;

    @Override
    public SimpleRiverContext setDefinition(Map<String, Object> definition) {
        this.definition = definition;
        return this;
    }

    @Override
    public Map<String, Object> getDefinition() {
        return definition;
    }

    @Override
    public SimpleRiverContext setRiverState(RiverState riverState) {
        this.riverState = riverState;
        return this;
    }

    @Override
    public RiverState getRiverState() {
        return riverState;
    }

    @Override
    public SimpleRiverContext setRiverSource(RiverSource source) {
        this.source = source;
        return this;
    }

    @Override
    public RiverSource getRiverSource() {
        return source;
    }

    public SimpleRiverContext setRiverMouth(RiverMouth mouth) {
        this.mouth = mouth;
        return this;
    }

    public RiverMouth getRiverMouth() {
        return mouth;
    }

    @Override
    public RiverContext setMetric(MeterMetric metric) {
        this.metric = metric;
        return this;
    }

    @Override
    public MeterMetric getMetric() {
        return metric;
    }

    public SimpleRiverContext setAutoCommit(boolean autocommit) {
        this.autocommit = autocommit;
        return this;
    }

    public boolean getAutoCommit() {
        return autocommit;
    }

    public SimpleRiverContext setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public SimpleRiverContext setMaxRows(int maxRows) {
        this.maxRows = maxRows;
        return this;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public SimpleRiverContext setRetries(int retries) {
        this.retries = retries;
        return this;
    }

    public int getRetries() {
        return retries;
    }

    public SimpleRiverContext setMaxRetryWait(TimeValue maxretrywait) {
        this.maxretrywait = maxretrywait;
        return this;
    }

    public TimeValue getMaxRetryWait() {
        return maxretrywait;
    }

    public SimpleRiverContext setRounding(String rounding) {
        if ("ceiling".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_CEILING;
        } else if ("down".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_DOWN;
        } else if ("floor".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_FLOOR;
        } else if ("halfdown".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_DOWN;
        } else if ("halfeven".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_EVEN;
        } else if ("halfup".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_UP;
        } else if ("unnecessary".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_UNNECESSARY;
        } else if ("up".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_UP;
        }
        return this;
    }

    public int getRounding() {
        return rounding;
    }

    public SimpleRiverContext setScale(int scale) {
        this.scale = scale;
        return this;
    }

    public int getScale() {
        return scale;
    }

    public SimpleRiverContext setResultSetType(String resultSetType) {
        this.resultSetType = resultSetType;
        return this;
    }

    public String getResultSetType() {
        return resultSetType;
    }

    public SimpleRiverContext setResultSetConcurrency(String resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
        return this;
    }

    public String getResultSetConcurrency() {
        return resultSetConcurrency;
    }

    public SimpleRiverContext shouldIgnoreNull(boolean shouldIgnoreNull) {
        this.shouldIgnoreNull = shouldIgnoreNull;
        return this;
    }

    public boolean shouldIgnoreNull() {
        return shouldIgnoreNull;
    }

    public SimpleRiverContext shouldPrepareResultSetMetadata(boolean shouldPrepareResultSetMetadata) {
        this.shouldPrepareResultSetMetadata = shouldPrepareResultSetMetadata;
        return this;
    }

    public boolean shouldPrepareResultSetMetadata() {
        return shouldPrepareResultSetMetadata;
    }

    public SimpleRiverContext shouldPrepareDatabaseMetadata(boolean shouldPrepareDatabaseMetadata) {
        this.shouldPrepareDatabaseMetadata = shouldPrepareDatabaseMetadata;
        return this;
    }

    public boolean shouldPrepareDatabaseMetadata() {
        return shouldPrepareDatabaseMetadata;
    }

    public SimpleRiverContext setLastResultSetMetadata(Map<String, Object> lastResultSetMetadata) {
        this.lastResultSetMetadata = lastResultSetMetadata;
        return this;
    }

    public Map<String, Object> getLastResultSetMetadata() {
        return lastResultSetMetadata;
    }

    public SimpleRiverContext setLastDatabaseMetadata(Map<String, Object> lastDatabaseMetadata) {
        this.lastDatabaseMetadata = lastDatabaseMetadata;
        return this;
    }

    public Map<String, Object> getLastDatabaseMetadata() {
        return lastDatabaseMetadata;
    }

    public SimpleRiverContext setLastRowCount(long lastRowCount) {
        this.lastRowCount = lastRowCount;
        return this;
    }

    public long getLastRowCount() {
        return lastRowCount;
    }

    public SimpleRiverContext setLastStartDate(long lastStartDate) {
        riverState.setLastStartDate(lastStartDate);
        return this;
    }

    public long getLastStartDate() {
        return riverState.getLastStartDate();
    }

    public SimpleRiverContext setLastEndDate(long lastEndDate) {
        riverState.setLastEndDate(lastEndDate);
        return this;
    }

    public long getLastEndDate() {
        return riverState.getLastEndDate();
    }

    public SimpleRiverContext setLastExecutionStartDate(long lastExecutionStartDate) {
        riverState.setLastExecutionStartDate(lastExecutionStartDate);
        return this;
    }

    public long getLastExecutionStartDate() {
        return riverState.getLastExecutionStartDate();
    }

    public SimpleRiverContext setLastExecutionEndDate(long lastExecutionEndDate) {
        riverState.setLastExecutionEndDate(lastExecutionEndDate);
        return this;
    }

    public long getLastExecutionEndDate() {
        return riverState.getLastExecutionEndDate();
    }

    public SimpleRiverContext setColumnNameMap(Map<String, Object> columnNameMap) {
        this.columnNameMap = columnNameMap;
        return this;
    }

    public Map<String, Object> getColumnNameMap() {
        return columnNameMap;
    }


    public SimpleRiverContext setLastRow(Map<String, Object> lastRow) {
        this.lastRow = lastRow;
        return this;
    }

    public Map<String, Object> getLastRow() {
        return lastRow;
    }


    public SimpleRiverContext setStatements(List<SQLCommand> sql) {
        this.sql = sql;
        return this;
    }

    public List<SQLCommand> getStatements() {
        return sql;
    }

    public SimpleRiverContext setTimestampDiffSupported(boolean supported) {
        this.isTimestampDiffSupported = supported;
        return this;
    }

    public boolean isTimestampDiffSupported() {
        return isTimestampDiffSupported;
    }

    public SimpleRiverContext setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
        return this;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public SimpleRiverContext setConnectionProperties(Map<String, Object> connectionProperties) {
        this.connectionProperties = connectionProperties;
        return this;
    }

    public Map<String, Object> getConnectionProperties() {
        return connectionProperties;
    }

    public SimpleRiverContext shouldTreatBinaryAsString(boolean shouldTreatBinaryAsString) {
        this.shouldTreatBinaryAsString = shouldTreatBinaryAsString;
        return this;
    }

    public boolean shouldTreatBinaryAsString() {
        return shouldTreatBinaryAsString;
    }

    @Override
    public SimpleRiverContext release() {
        try {
            if (mouth != null) {
                mouth.release();
                mouth = null;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        try {
            if (source != null) {
                source.release();
                source = null;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    @Override
    public SimpleRiverContext shutdown() {
        try {
            if (mouth != null) {
                mouth.shutdown();
                mouth = null;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        try {
            if (source != null) {
                source.shutdown();
                source = null;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return this;
    }

    public Map<String, Object> asMap() {
        try {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("autocommit", autocommit)
                    .field("fetchsize", fetchSize)
                    .field("maxrows", maxRows)
                    .field("retries", retries)
                    .field("maxretrywait", maxretrywait)
                    .field("resultsetconcurrency", resultSetConcurrency)
                    .field("resultsettype", resultSetType)
                    .field("rounding", rounding)
                    .field("scale", scale)
                    .field("shouldignorenull", shouldIgnoreNull)
                    .field("lastResultSetMetadata", lastResultSetMetadata)
                    .field("lastDatabaseMetadata", lastDatabaseMetadata)
                    .field("lastStartDate", riverState.getLastStartDate())
                    .field("lastEndDate", riverState.getLastEndDate())
                    .field("lastExecutionStartDate", riverState.getLastExecutionStartDate())
                    .field("lastExecutionEndDate", riverState.getLastExecutionEndDate())
                    .field("columnNameMap", columnNameMap)
                    .field("lastRow", lastRow)
                    .field("sql", sql)
                    .field("isTimestampDiffSupported", isTimestampDiffSupported)
                    .field("queryTimeout", queryTimeout)
                    .field("connectionProperties")
                    .map(connectionProperties)
                    .endObject();
            return XContentHelper.convertToMap(builder.bytes(), true).v2();
        } catch (IOException e) {
            // should really not happen
            return new HashMap<String, Object>();
        }
    }

    @Override
    public String toString() {
        return asMap().toString();
    }
}
