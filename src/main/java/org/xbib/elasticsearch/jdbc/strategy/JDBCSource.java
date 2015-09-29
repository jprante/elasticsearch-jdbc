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
package org.xbib.elasticsearch.jdbc.strategy;

import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.common.keyvalue.KeyValueStreamListener;
import org.xbib.elasticsearch.common.util.SQLCommand;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/**
 * The source models the data producing side
 */
public interface JDBCSource<C extends Context> extends Source<C> {

    /**
     * Create new source instance
     *
     * @return a new source instance
     */
    JDBCSource<C> newInstance();

    /**
     * Set the context
     *
     * @param context the context
     * @return this source
     */
    JDBCSource<C> setContext(C context);

    /**
     * Set JDBC connection URL
     *
     * @param url the JDBC connection URL
     * @return this source
     */
    JDBCSource<C> setUrl(String url);

    /**
     * Set the user authentication
     *
     * @param user the user
     * @return this source
     */
    JDBCSource<C> setUser(String user);

    /**
     * Set the password authentication
     *
     * @param password the password
     * @return this source
     */
    JDBCSource<C> setPassword(String password);

    /**
     * Set scale of big decimal values.  See java.math.BigDecimal#setScale
     *
     * @param scale the scale of big decimal values
     * @return this context
     */
    JDBCSource<C> setScale(int scale);

    /**
     * Set rounding of big decimal values. See java.math.BigDecimal#setScale
     *
     * @param rounding the rounding of big decimal values
     * @return this context
     */
    JDBCSource<C> setRounding(String rounding);

    /**
     * Set the list of SQL statements
     *
     * @param sql the list of SQL statements
     * @return this context
     */
    JDBCSource<C> setStatements(List<SQLCommand> sql);

    /**
     * Set auto commit
     *
     * @param autocommit true if automatic commit should be performed
     * @return this context
     */
    JDBCSource<C> setAutoCommit(boolean autocommit);

    /**
     * Set max rows
     *
     * @param maxRows max rows
     * @return this context
     */
    JDBCSource<C> setMaxRows(int maxRows);

    /**
     * Set fetch size
     *
     * @param fetchSize fetch size
     * @return this context
     */
    JDBCSource<C> setFetchSize(int fetchSize);

    /**
     * Set retries
     *
     * @param retries number of retries
     * @return this context
     */
    JDBCSource<C> setRetries(int retries);

    /**
     * Set maximum count of retries
     *
     * @param maxretrywait maximum count of retries
     * @return this context
     */
    JDBCSource<C> setMaxRetryWait(TimeValue maxretrywait);

    /**
     * Set result set type
     *
     * @param resultSetType result set type
     * @return this context
     */
    JDBCSource<C> setResultSetType(String resultSetType);

    /**
     * Set result set concurrency
     *
     * @param resultSetConcurrency result set concurrency
     * @return this context
     */
    JDBCSource<C> setResultSetConcurrency(String resultSetConcurrency);

    /**
     * Should null values in columns be ignored for indexing. Default is false
     *
     * @param shouldIgnoreNull true if null values in columns should be ignored for indexing
     * @return this context
     */
    JDBCSource<C> shouldIgnoreNull(boolean shouldIgnoreNull);

    /**
     * Should geo values in columns be detected for indexing. Default is true
     *
     * @param shouldDetectGeo true if geo values in columns should be detected for indexing
     * @return this context
     */
    JDBCSource<C> shouldDetectGeo(boolean shouldDetectGeo);

    /**
     * Should json structures in columns be parsed for indexing. Default is true
     *
     * @param shouldDetectJson true if json structures in columns should be parsed for indexing
     * @return this context
     */
    JDBCSource<C> shouldDetectJson(boolean shouldDetectJson);

    /**
     * Should result set metadata be used in parameter variables
     *
     * @param shouldPrepareResultSetMetadata true if result set metadata should be used in parameter variables
     * @return this context
     */
    JDBCSource<C> shouldPrepareResultSetMetadata(boolean shouldPrepareResultSetMetadata);

    /**
     * Should database metadata be used in parameter variables
     *
     * @param shouldPrepareDatabaseMetadata true if database metadata should be used in parameter variables
     * @return this context
     */
    JDBCSource<C> shouldPrepareDatabaseMetadata(boolean shouldPrepareDatabaseMetadata);

    /**
     * Set result set query timeout
     *
     * @param queryTimeout the query timeout in seconds
     * @return this context
     */
    JDBCSource<C> setQueryTimeout(int queryTimeout);

    /**
     * Optional JDBC connection properties
     *
     * @param connectionProperties connection properties
     * @return this context
     */
    JDBCSource<C> setConnectionProperties(Map<String, Object> connectionProperties);

    /**
     * Set column name map. Useful for expanding shortcolumn names to longer variants.
     *
     * @param columnNameMap the column name map
     * @return this context
     */
    JDBCSource<C> setColumnNameMap(Map<String, Object> columnNameMap);

    /**
     * Should binary types (byte arrays) be treated as JSON strings
     *
     * @param shouldTreatBinaryAsString true if binary types (byte arrays) should be treated as JSON strings
     * @return this context
     */
    JDBCSource<C> shouldTreatBinaryAsString(boolean shouldTreatBinaryAsString);

    /**
     * Get a connection for reading data
     *
     * @return connection
     * @throws SQLException when SQL execution gives an error
     */
    Connection getConnectionForReading() throws SQLException;

    /**
     * Get a connection for writing data
     *
     * @return connection
     * @throws SQLException when SQL execution gives an error
     */
    Connection getConnectionForWriting() throws SQLException;

    /**
     * Prepare query statement
     *
     * @param sql SQL statement
     * @return a prepared statement
     * @throws SQLException when SQL execution gives an error
     */
    PreparedStatement prepareQuery(String sql) throws SQLException;

    /**
     * Prepare insert/update statement
     *
     * @param sql SQL statement
     * @return a prepared statement
     * @throws SQLException when SQL execution gives an error
     */
    PreparedStatement prepareUpdate(String sql) throws SQLException;

    /**
     * Bind query variables
     *
     * @param statement prepared statement
     * @param values    values
     * @return this source
     * @throws SQLException when SQL execution gives an error
     */
    JDBCSource<C> bind(PreparedStatement statement, List<Object> values) throws SQLException;

    /**
     * Register output variables for callable statement
     *
     * @param statement callable statement
     * @param values    values
     * @return this source
     * @throws SQLException when SQL execution gives an error
     */
    JDBCSource<C> register(CallableStatement statement, Map<String, Object> values) throws SQLException;

    /**
     * Execute query
     *
     * @param statement prepared statement
     * @return the result set
     * @throws SQLException when SQL execution gives an error
     */
    ResultSet executeQuery(PreparedStatement statement) throws SQLException;

    /**
     * Execute query without binding parameters
     *
     * @param statement the SQL statement
     * @param sql       the SQL query
     * @return the result set
     * @throws SQLException when SQL execution gives an error
     */
    ResultSet executeQuery(Statement statement, String sql) throws SQLException;

    /**
     * Execute insert/update
     *
     * @param statement statement
     * @return this source
     * @throws SQLException when SQL execution gives an error
     */
    JDBCSource<C> executeUpdate(PreparedStatement statement) throws SQLException;

    /**
     * Execute insert update
     *
     * @param statement statement
     * @param sql       SQL query
     * @return this source
     * @throws SQLException when SQL execution fails
     */
    JDBCSource<C> executeUpdate(Statement statement, String sql) throws SQLException;

    /**
     * Executed before rows are fetched from result set
     *
     * @param results  the result set
     * @param listener a result set listener or null
     * @throws SQLException when result set fails
     * @throws IOException when method fails
     */
    void beforeRows(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * Executed when next row is fetched from result set
     *
     * @param results  the result set
     * @param listener a result set listener or null
     * @return true if next row could be processed, otherwise false
     * @throws SQLException when result set fails
     * @throws IOException when method fails
     */
    boolean nextRow(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * Executed after all rows have been fetched from result set
     *
     * @param results  the result set
     * @param listener a result set listener or null
     * @throws SQLException when result set fails
     * @throws IOException when method fails
     */
    void afterRows(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * This routine is executed before the result set is evaluated
     *
     * @param command  the SQL command that created this result set
     * @param results  the result set
     * @param listener listener for the key/value stream generated from the result set
     * @throws SQLException when result set fails
     * @throws IOException when method fails
     */
    void beforeRows(SQLCommand command, ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * Action for the next row of the result set to be processed
     *
     * @param command  the SQL command that created this result set
     * @param results  result
     * @param listener listener
     * @return true if next row exists
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/output error occurs
     */
    boolean nextRow(SQLCommand command, ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * After the result set is processed, this method is called.
     *
     * @param command  the SQL command that created this result set
     * @param results  the result set
     * @param listener listener for the key/value stream generated from the result set
     * @throws SQLException  when SQL execution gives an error
     * @throws IOException   when input/output error occurs
     */
    void afterRows(SQLCommand command, ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * Parse a value in a row column
     *
     * @param results result set
     * @param num     position
     * @param type    type
     * @param locale  locale
     * @return object
     * @throws SQLException   when SQL execution gives an error
     * @throws IOException    when input/output error occurs
     * @throws ParseException if number format could not be parsed
     */
    Object parseType(ResultSet results, Integer num, int type, Locale locale) throws SQLException, IOException, ParseException;

    /**
     * Close result set
     *
     * @param result result set
     * @return this source
     * @throws SQLException when SQL execution gives an error
     */
    JDBCSource<C> close(ResultSet result) throws SQLException;

    /**
     * Close statement
     *
     * @param statement statement
     * @return this source
     * @throws SQLException when SQL execution gives an error
     */
    JDBCSource<C> close(Statement statement) throws SQLException;

    /**
     * Close reading from this source
     *
     * @return this source
     */
    JDBCSource<C> closeReading();

    /**
     * Close writing to this source
     *
     * @return this source
     */
    JDBCSource<C> closeWriting();

    /**
     * Set the locale
     *
     * @param locale locale
     * @return this source
     */
    JDBCSource<C> setLocale(Locale locale);

    /**
     * Set the timezone for setTimestamp() calls with calendar object.
     *
     * @param timeZone the time zone
     * @return this source
     */
    JDBCSource<C> setTimeZone(TimeZone timeZone);

}
