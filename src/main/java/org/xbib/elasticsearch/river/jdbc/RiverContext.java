package org.xbib.elasticsearch.river.jdbc;

import org.elasticsearch.common.metrics.MeterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.plugin.jdbc.util.SQLCommand;

import java.util.List;
import java.util.Map;

/**
 * The River Context is a collection of objects that are relevant to parameterization of
 * a river run. Beside holding references to river definition, source and mouth,
 * the objects control the behavior of the river source.
 */
public interface RiverContext {

    /**
     * Set river instance definition
     *
     * @param definition the river instance definition
     * @return this context
     */
    RiverContext setDefinition(Map<String, Object> definition);

    /**
     * Get river instance definition
     *
     * @return river instance definition
     */
    Map<String, Object> getDefinition();

    /**
     * Set river state
     *
     * @param riverState the river state
     * @return this context
     */
    RiverContext setRiverState(RiverState riverState);

    /**
     * Get river state
     *
     * @return the river state
     */
    RiverState getRiverState();

    /**
     * Set river source
     *
     * @param riverSource the river source
     * @return this context
     */
    RiverContext setRiverSource(RiverSource riverSource);

    /**
     * Get river source
     *
     * @return the river source
     */
    RiverSource getRiverSource();

    /**
     * Set river mouth
     *
     * @param riverMouth the river mouth
     * @return this context
     */
    RiverContext setRiverMouth(RiverMouth riverMouth);

    /**
     * Get river mouth
     *
     * @return the river mouth
     */
    RiverMouth getRiverMouth();

    /**
     * Set metric
     *
     * @param metric the meter metric
     * @return this context
     */
    RiverContext setMetric(MeterMetric metric);

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
    RiverContext setScale(int scale);

    /**
     * Set rounding of big decimal values. See java.math.BigDecimal#setScale
     *
     * @param rounding the rounding of big decimal values
     * @return this river context
     */
    RiverContext setRounding(String rounding);

    /**
     * Set the list of SQL statements
     *
     * @param sql the list of SQL statements
     * @return this river context
     */
    RiverContext setStatements(List<SQLCommand> sql);

    /**
     * Set auto commit
     *
     * @param autocommit true if automatic commit should be performed
     * @return this river context
     */
    RiverContext setAutoCommit(boolean autocommit);

    /**
     * Set max rows
     *
     * @param maxRows max rows
     * @return this river context
     */
    RiverContext setMaxRows(int maxRows);

    /**
     * Set fetch size
     *
     * @param fetchSize fetch size
     * @return this river context
     */
    RiverContext setFetchSize(int fetchSize);

    /**
     * Set retries
     *
     * @param retries number of retries
     * @return this river context
     */
    RiverContext setRetries(int retries);

    /**
     * Set maximum count of retries
     *
     * @param maxretrywait maximum count of retries
     * @return this river context
     */
    RiverContext setMaxRetryWait(TimeValue maxretrywait);

    /**
     * Set result set type
     *
     * @param resultSetType result set type
     * @return this river context
     */
    RiverContext setResultSetType(String resultSetType);

    /**
     * Set result set concurrency
     *
     * @param resultSetConcurrency result set concurrency
     * @return this river context
     */
    RiverContext setResultSetConcurrency(String resultSetConcurrency);

    /**
     * Should null values in columns be ignored for indexing
     *
     * @param shouldIgnoreNull true if null values in columns should be ignored for indexing
     * @return this river context
     */
    RiverContext shouldIgnoreNull(boolean shouldIgnoreNull);

    /**
     * Should result set metadata be used in parameter variables
     *
     * @param shouldPrepareResultSetMetadata true if result set metadata should be used in parameter variables
     * @return this river context
     */
    RiverContext shouldPrepareResultSetMetadata(boolean shouldPrepareResultSetMetadata);

    /**
     * Should database metadata be used in parameter variables
     *
     * @param shouldPrepareDatabaseMetadata true if database metadata should be used in parameter variables
     * @return this river context
     */
    RiverContext shouldPrepareDatabaseMetadata(boolean shouldPrepareDatabaseMetadata);

    /**
     * Set result set query timeout
     *
     * @param queryTimeout the query timeout in seconds
     * @return this river context
     */
    RiverContext setQueryTimeout(int queryTimeout);

    /**
     * Optional JDBC connection properties
     *
     * @param connectionProperties connection properties
     * @return this river context
     */
    RiverContext setConnectionProperties(Map<String, Object> connectionProperties);

    /**
     * Set column name map. Useful for expanding shortcolumn names to longer variants.
     *
     * @param columnNameMap the column name map
     * @return this river context
     */
    RiverContext setColumnNameMap(Map<String, Object> columnNameMap);

    /**
     * Should binary types (byte arrays) be treated as JSON strings
     *
     * @param shouldTreatBinaryAsString true if binary types (byte arrays) should be treated as JSON strings
     * @return this river context
     */
    RiverContext shouldTreatBinaryAsString(boolean shouldTreatBinaryAsString);

    /**
     * Release all resources
     *
     * @return this river context
     */
    RiverContext release();

    RiverContext shutdown();

}
