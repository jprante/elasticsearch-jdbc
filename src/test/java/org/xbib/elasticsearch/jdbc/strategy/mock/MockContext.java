package org.xbib.elasticsearch.jdbc.strategy.mock;

import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.jdbc.state.State;
import org.xbib.elasticsearch.jdbc.util.SQLCommand;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.Mouth;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;

import java.util.List;
import java.util.Map;

public class MockContext implements Context {

    @Override
    public Context setDefinition(Map<String, Object> definition) {
        return this;
    }

    @Override
    public Map<String, Object> getDefinition() {
        return null;
    }

    @Override
    public Context setState(State state) {
        return this;
    }

    @Override
    public State getState() {
        return null;
    }

    @Override
    public Context setSource(JDBCSource JDBCSource) {
        return this;
    }

    @Override
    public JDBCSource getSource() {
        return null;
    }

    @Override
    public Context setMouth(Mouth mouth) {
        return this;
    }

    @Override
    public Mouth getMouth() {
        return null;
    }

    @Override
    public Context setMetric(MeterMetric metric) {
        return this;
    }

    @Override
    public MeterMetric getMetric() {
        return null;
    }

    @Override
    public Context setScale(int scale) {
        return this;
    }

    @Override
    public Context setRounding(String rounding) {
        return this;
    }

    @Override
    public Context setStatements(List<SQLCommand> sql) {
        return this;
    }

    @Override
    public Context setAutoCommit(boolean autocommit) {
        return this;
    }

    @Override
    public Context setMaxRows(int maxRows) {
        return this;
    }

    @Override
    public Context setFetchSize(int fetchSize) {
        return this;
    }

    @Override
    public Context setRetries(int retries) {
        return this;
    }

    @Override
    public Context setMaxRetryWait(TimeValue maxretrywait) {
        return this;
    }

    @Override
    public Context setResultSetType(String resultSetType) {
        return this;
    }

    @Override
    public Context setResultSetConcurrency(String resultSetConcurrency) {
        return this;
    }

    @Override
    public Context shouldIgnoreNull(boolean shouldIgnoreNull) {
        return this;
    }

    @Override
    public Context shouldPrepareResultSetMetadata(boolean shouldPrepareResultSetMetadata) {
        return this;
    }

    @Override
    public Context shouldPrepareDatabaseMetadata(boolean shouldPrepareDatabaseMetadata) {
        return this;
    }

    @Override
    public Context setQueryTimeout(int queryTimeout) {
        return this;
    }

    @Override
    public Context setConnectionProperties(Map<String, Object> connectionProperties) {
        return this;
    }

    @Override
    public Context setColumnNameMap(Map<String, Object> columnNameMap) {
        return this;
    }

    @Override
    public Context shouldTreatBinaryAsString(boolean shouldTreatBinaryAsString) {
        return this;
    }

    @Override
    public Context release() {
        return this;
    }

}
