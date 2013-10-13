/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.xbib.elasticsearch.river.jdbc;

import org.elasticsearch.action.bulk.BulkResponse;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.ValueListener;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;
import java.util.Locale;

/**
 * The river source models the data producing side
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public interface RiverSource {

    /**
     * The strategy this river source supports.
     *
     * @return the strategy as a string
     */
    String strategy();

    /**
     * Set the river context
     *
     * @param context the context
     * @return this river source
     */
    RiverSource riverContext(RiverContext context);

    /**
     * Fetch a data portion from the database and pass it to the river task
     * for firther processing.
     *
     * @return a checksum of the fetched data or null
     * @throws SQLException
     * @throws IOException
     */
    String fetch() throws SQLException, IOException;

    /**
     * Set the driver for the JDBC source
     *
     * @param driver
     * @return this river source
     */
    RiverSource driver(String driver);

    /**
     * Set the driver URL
     *
     * @param url
     * @return this river source
     */
    RiverSource url(String url);

    /**
     * Set the user authentication
     *
     * @param user
     * @return this river source
     */
    RiverSource user(String user);

    /**
     * Set the password authentication
     *
     * @param password
     * @return this river source
     */
    RiverSource password(String password);

    /**
     * Set rounding for transporting java.math.BigDecimal
     *
     * @param rounding
     * @return this river source
     */
    RiverSource rounding(String rounding);

    /**
     * Set scale precision for transporting java.math.BigDecimal
     *
     * @param scale
     * @return this river source
     */
    RiverSource precision(int scale);

    /**
     * Get a connection for reading data
     *
     * @return connection
     * @throws SQLException
     */
    Connection connectionForReading() throws SQLException;

    /**
     * Get a connection for writing data
     *
     * @return connection
     * @throws SQLException
     */
    Connection connectionForWriting() throws SQLException;

    /**
     * Prepare query statement
     *
     * @param sql
     * @return a prepared statement
     * @throws SQLException
     */
    PreparedStatement prepareQuery(String sql) throws SQLException;

    /**
     * Prepare insert/update statement
     * @param sql
     * @return a prepared statement
     * @throws SQLException
     */
    PreparedStatement prepareUpdate(String sql) throws SQLException;

    /**
     * Bind query variables
     *
     * @param pstmt
     * @param values
     * @return this river source
     * @throws SQLException
     */
    RiverSource bind(PreparedStatement pstmt, List<? extends Object> values) throws SQLException;

    /**
     * Execute query
     * @param statement
     * @return the result set
     * @throws SQLException
     */
    ResultSet executeQuery(PreparedStatement statement) throws SQLException;

    /**
     * Execute query without binding parameters
     * @param sql the SQL statement
     * @return the result set
     * @throws SQLException
     */
    ResultSet executeQuery(String sql) throws SQLException;

    /**
     * Execute insert/update
     * @param statement
     * @return this river source
     * @throws SQLException
     */
    RiverSource executeUpdate(PreparedStatement statement) throws SQLException;

    /**
     * Action before the first row of the result set is processed
     * @param result
     * @param listener
     * @throws SQLException
     * @throws IOException
     * @throws ParseException
     */
    void beforeFirstRow(ResultSet result, ValueListener listener) throws SQLException, IOException, ParseException;

    /**
     * Action while next row of the result set is processed
     * @param result
     * @param listener
     * @return true if next row exists
     * @throws SQLException
     * @throws IOException
     * @throws ParseException
     */
    boolean nextRow(ResultSet result, ValueListener listener) throws SQLException, IOException, ParseException;

    /**
     * Parse a value in a row column
     *
     * @param result
     * @param num
     * @param type
     * @param locale
     * @return object
     * @throws SQLException
     * @throws IOException
     * @throws ParseException
     */
    Object parseType(ResultSet result, Integer num, int type, Locale locale) throws SQLException, IOException, ParseException;

    /**
     * Acknowledge a bulk response
     *
     * @param response
     * @return this river source
     * @throws IOException
     */
    RiverSource acknowledge(BulkResponse response) throws IOException;

    /**
     * Close result set
     *
     * @param result
     * @return this river source
     * @throws SQLException
     */
    RiverSource close(ResultSet result) throws SQLException;

    /**
     * Close statement
     * @param statement
     * @return this river source
     * @throws SQLException
     */
    RiverSource close(PreparedStatement statement) throws SQLException;

    /**
     * Close reading from this river source
     * @return this river source
     */
    RiverSource closeReading();

    /**
     * Close writing to this river source
     * @return this river source
     */
    RiverSource closeWriting();
}
