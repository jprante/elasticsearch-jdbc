package org.xbib.elasticsearch.river.jdbc.strategy.mock;

import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.plugin.jdbc.util.SQLCommand;
import org.xbib.elasticsearch.river.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;

import java.util.List;
import java.util.Map;

public class MockRiverContext implements RiverContext {

    @Override
    public RiverContext setDefinition(Map<String, Object> definition) {
        return this;
    }

    @Override
    public Map<String, Object> getDefinition() {
        return null;
    }

    @Override
    public RiverContext setRiverState(RiverState riverState) {
        return this;
    }

    @Override
    public RiverState getRiverState() {
        return null;
    }

    @Override
    public RiverContext setRiverSource(RiverSource riverSource) {
        return this;
    }

    @Override
    public RiverSource getRiverSource() {
        return null;
    }

    @Override
    public RiverContext setRiverMouth(RiverMouth riverMouth) {
        return this;
    }

    @Override
    public RiverMouth getRiverMouth() {
        return null;
    }

    @Override
    public RiverContext setMetric(MeterMetric metric) {
        return this;
    }

    @Override
    public MeterMetric getMetric() {
        return null;
    }

    @Override
    public RiverContext setScale(int scale) {
        return this;
    }

    @Override
    public RiverContext setRounding(String rounding) {
        return this;
    }

    @Override
    public RiverContext setStatements(List<SQLCommand> sql) {
        return this;
    }

    @Override
    public RiverContext setAutoCommit(boolean autocommit) {
        return this;
    }

    @Override
    public RiverContext setMaxRows(int maxRows) {
        return this;
    }

    @Override
    public RiverContext setFetchSize(int fetchSize) {
        return this;
    }

    @Override
    public RiverContext setRetries(int retries) {
        return this;
    }

    @Override
    public RiverContext setMaxRetryWait(TimeValue maxretrywait) {
        return this;
    }

    @Override
    public RiverContext setResultSetType(String resultSetType) {
        return this;
    }

    @Override
    public RiverContext setResultSetConcurrency(String resultSetConcurrency) {
        return this;
    }

    @Override
    public RiverContext shouldIgnoreNull(boolean shouldIgnoreNull) {
        return this;
    }

    @Override
    public RiverContext shouldPrepareResultSetMetadata(boolean shouldPrepareResultSetMetadata) {
        return this;
    }

    @Override
    public RiverContext shouldPrepareDatabaseMetadata(boolean shouldPrepareDatabaseMetadata) {
        return this;
    }

    @Override
    public RiverContext setQueryTimeout(int queryTimeout) {
        return this;
    }

    @Override
    public RiverContext setConnectionProperties(Map<String, Object> connectionProperties) {
        return this;
    }

    @Override
    public RiverContext setColumnNameMap(Map<String, Object> columnNameMap) {
        return this;
    }

    @Override
    public RiverContext shouldTreatBinaryAsString(boolean shouldTreatBinaryAsString) {
        return this;
    }

    @Override
    public RiverContext release() {
        return this;
    }

    @Override
    public RiverContext shutdown() {
        return this;
    }
}
