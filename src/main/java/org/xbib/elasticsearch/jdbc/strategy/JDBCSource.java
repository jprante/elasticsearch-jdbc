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
package org.xbib.elasticsearch.jdbc.strategy;

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
    JDBCSource executeUpdate(PreparedStatement statement) throws SQLException;

    /**
     * Execute insert update
     *
     * @param statement statement
     * @param sql       SQL query
     * @return this source
     * @throws SQLException
     */
    JDBCSource<C> executeUpdate(Statement statement, String sql) throws SQLException;

    /**
     * Executed before rows are fetched from result set
     *
     * @param results  the result set
     * @param listener a result set listener or null
     * @throws SQLException
     * @throws IOException
     */
    void beforeRows(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * Executed when next row is fetched from result set
     *
     * @param results  the result set
     * @param listener a result set listener or null
     * @return true if next row could be processed, otherwise false
     * @throws SQLException
     * @throws IOException
     */
    boolean nextRow(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * Executed after all rows have been fetched from result set
     *
     * @param results  the result set
     * @param listener a result set listener or null
     * @throws SQLException
     * @throws IOException
     */
    void afterRows(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * This routine is executed before the result set is evaluated
     *
     * @param command  the SQL command that created this result set
     * @param results  the result set
     * @param listener listener for the key/value stream generated from the result set
     * @throws SQLException
     * @throws IOException
     */
    void beforeRows(SQLCommand command, ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * Action for the next row of the result set to be processed
     *
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
     * @throws SQLException
     * @throws IOException
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
     * Set the locale for JDBC
     *
     * @param locale locale
     * @return this source
     */
    JDBCSource<C> setLocale(Locale locale);

    /**
     * Get the current locale
     *
     * @return the time zone
     */
    Locale getLocale();

    /**
     * Set the timezone for JDBC setTimestamp() calls with calendar object.
     *
     * @param timeZone the time zone
     * @return this source
     */
    JDBCSource<C> setTimeZone(TimeZone timeZone);

    /**
     * Get the current timezone of this source for the JDBC setTimestamp() call
     *
     * @return the time zone
     */
    TimeZone getTimeZone();

}
