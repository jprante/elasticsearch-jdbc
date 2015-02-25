package org.xbib.elasticsearch.jdbc.strategy;

import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.common.state.State;
import org.xbib.elasticsearch.common.util.SQLCommand;

import java.util.List;
import java.util.Map;

/**
 * The Context is a collection of objects that are relevant to parameterization of
 * a run. Beside holding references to definition, source and mouth,
 * the objects control the behavior of the source.
 */
public interface Context<S extends Source, M extends Mouth> {

    /**
     * Set instance definition
     *
     * @param definition the instance definition
     * @return this context
     */
    Context setDefinition(Map<String, Object> definition);

    /**
     * Get instance definition
     *
     * @return instance definition
     */
    Map<String, Object> getDefinition();

    /**
     * Set state
     *
     * @param state the state
     * @return this context
     */
    Context setState(State state);

    /**
     * Get state
     *
     * @return the state
     */
    State getState();

    /**
     * Set source
     *
     * @param source the source
     * @return this context
     */
    Context setSource(S source);

    /**
     * Get source
     *
     * @return the source
     */
    S getSource();

    /**
     * Set mouth
     *
     * @param mouth the mouth
     * @return this context
     */
    Context setMouth(M mouth);

    /**
     * Get mouth
     *
     * @return the mouth
     */
    M getMouth();

    /**
     * Set metric
     *
     * @param metric the meter metric
     * @return this context
     */
    Context setMetric(MeterMetric metric);

    /**
     * Get metric
     *
     * @return metric
     */
    MeterMetric getMetric();

    /**
     * Set scale of big decimal values.  See java.math.BigDecimal#setScale
     *
     * @param scale the scale of big decimal values
     * @return this context
     */
    Context setScale(int scale);

    /**
     * Set rounding of big decimal values. See java.math.BigDecimal#setScale
     *
     * @param rounding the rounding of big decimal values
     * @return this context
     */
    Context setRounding(String rounding);

    /**
     * Set the list of SQL statements
     *
     * @param sql the list of SQL statements
     * @return this context
     */
    Context setStatements(List<SQLCommand> sql);

    /**
     * Set auto commit
     *
     * @param autocommit true if automatic commit should be performed
     * @return this context
     */
    Context setAutoCommit(boolean autocommit);

    /**
     * Set max rows
     *
     * @param maxRows max rows
     * @return this context
     */
    Context setMaxRows(int maxRows);

    /**
     * Set fetch size
     *
     * @param fetchSize fetch size
     * @return this context
     */
    Context setFetchSize(int fetchSize);

    /**
     * Set retries
     *
     * @param retries number of retries
     * @return this context
     */
    Context setRetries(int retries);

    /**
     * Set maximum count of retries
     *
     * @param maxretrywait maximum count of retries
     * @return this context
     */
    Context setMaxRetryWait(TimeValue maxretrywait);

    /**
     * Set result set type
     *
     * @param resultSetType result set type
     * @return this context
     */
    Context setResultSetType(String resultSetType);

    /**
     * Set result set concurrency
     *
     * @param resultSetConcurrency result set concurrency
     * @return this context
     */
    Context setResultSetConcurrency(String resultSetConcurrency);

    /**
     * Should null values in columns be ignored for indexing
     *
     * @param shouldIgnoreNull true if null values in columns should be ignored for indexing
     * @return this context
     */
    Context shouldIgnoreNull(boolean shouldIgnoreNull);

    /**
     * Should result set metadata be used in parameter variables
     *
     * @param shouldPrepareResultSetMetadata true if result set metadata should be used in parameter variables
     * @return this context
     */
    Context shouldPrepareResultSetMetadata(boolean shouldPrepareResultSetMetadata);

    /**
     * Should database metadata be used in parameter variables
     *
     * @param shouldPrepareDatabaseMetadata true if database metadata should be used in parameter variables
     * @return this context
     */
    Context shouldPrepareDatabaseMetadata(boolean shouldPrepareDatabaseMetadata);

    /**
     * Set result set query timeout
     *
     * @param queryTimeout the query timeout in seconds
     * @return this context
     */
    Context setQueryTimeout(int queryTimeout);

    /**
     * Optional JDBC connection properties
     *
     * @param connectionProperties connection properties
     * @return this context
     */
    Context setConnectionProperties(Map<String, Object> connectionProperties);

    /**
     * Set column name map. Useful for expanding shortcolumn names to longer variants.
     *
     * @param columnNameMap the column name map
     * @return this context
     */
    Context setColumnNameMap(Map<String, Object> columnNameMap);

    /**
     * Should binary types (byte arrays) be treated as JSON strings
     *
     * @param shouldTreatBinaryAsString true if binary types (byte arrays) should be treated as JSON strings
     * @return this context
     */
    Context shouldTreatBinaryAsString(boolean shouldTreatBinaryAsString);

    /**
     * Release all resources
     *
     * @return this context
     */
    Context release();

}
