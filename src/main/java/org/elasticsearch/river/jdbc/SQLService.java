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
package org.elasticsearch.river.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import org.elasticsearch.common.Base64;

/**
 * The SQL service class manages the SQL access to the JDBC connection.
 */
public class SQLService {

    /**
     * Get JDBC connection
     * @param driverClassName
     * @param jdbcURL
     * @param username
     * @param password
     * @return the connection
     * @throws ClassNotFoundException
     * @throws SQLException 
     */
    public Connection getConnection(final String driverClassName,
            final String jdbcURL, final String username, final String password)
            throws ClassNotFoundException, SQLException {
        Class.forName(driverClassName);
        Connection connection = DriverManager.getConnection(jdbcURL, username, password);
        connection.setReadOnly(true);
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * Prepare statement
     * @param connection
     * @param sql
     * @return a prepared statement
     * @throws SQLException 
     */
    public PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
        return connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * Bind values to prepared statement
     * @param pstmt
     * @param values
     * @throws SQLException 
     */
    public void bind(PreparedStatement pstmt, List<Object> values) throws SQLException {
        if (values == null) {
            return;
        }
        for (int i = 1; i <= values.size(); i++) {
            bind(pstmt, i, values.get(i - 1));
        }
    }

    /**
     * Execute prepared statement
     * @param statement
     * @param fetchSize
     * @return the result set
     * @throws SQLException 
     */
    public ResultSet execute(PreparedStatement statement, int fetchSize) throws SQLException {
        statement.setMaxRows(0);
        statement.setFetchSize(fetchSize);
        return statement.executeQuery();
    }

    /**
     * Get next row and prepare the values for processing. 
     * The labels of each columns are used for the 
     * RowListener as paths for JSON object merging.
     * 
     * @param result the result set
     * @param listener the listener
     * @return true if row exists and was process, false otherwise
     * @throws SQLException
     * @throws IOException 
     */
    public boolean nextRow(ResultSet result, RowListener listener) throws SQLException, IOException {
        int row = result.getRow();
        if (result.next()) {
            ResultSetMetaData metadata = result.getMetaData();
            LinkedList<String> keys = new LinkedList();
            LinkedList<Object> values = new LinkedList();
            String index = null;
            String type = null;
            String id = null;
            int columns = metadata.getColumnCount();
            for (int i = 1; i <= columns; i++) {
                String name = metadata.getColumnLabel(i);
                if ("_index".equalsIgnoreCase(name)) {
                    index = result.getString(i);
                    continue;
                } else if ("_type".equalsIgnoreCase(name)) {
                    type = result.getString(i);
                    continue;
                } else if ("_id".equalsIgnoreCase(name)) {
                    id = result.getString(i);
                    continue;
                }
                keys.add(name);
                if (metadata.getColumnType(i) == Types.ARRAY) {
                    values.add(result.getArray(i).toString());
                } else if (metadata.getColumnType(i) == Types.BIGINT) {
                    values.add(result.getInt(i));
                } else if (metadata.getColumnType(i) == Types.BOOLEAN) {
                    values.add(result.getBoolean(i));
                } else if (metadata.getColumnType(i) == Types.DOUBLE) {
                    values.add(result.getDouble(i));
                } else if (metadata.getColumnType(i) == Types.FLOAT) {
                    values.add(result.getFloat(i));
                } else if (metadata.getColumnType(i) == Types.INTEGER) {
                    values.add(result.getInt(i));
                } else if (metadata.getColumnType(i) == Types.NVARCHAR) {
                    values.add(result.getNString(i));
                } else if (metadata.getColumnType(i) == Types.VARCHAR) {
                    values.add(result.getString(i));
                } else if (metadata.getColumnType(i) == Types.TINYINT) {
                    values.add(result.getInt(i));
                } else if (metadata.getColumnType(i) == Types.SMALLINT) {
                    values.add(result.getInt(i));
                } else if (metadata.getColumnType(i) == Types.DATE) {
                    values.add(result.getDate(i).getTime());
                } else if (metadata.getColumnType(i) == Types.TIMESTAMP) {
                    try {
                        values.add(result.getTimestamp(i).getTime());
                    } catch (SQLException e) {
                        // java.sql.SQLException: Cannot convert value '0000-00-00 00:00:00' from column ... to TIMESTAMP.
                        values.add(null);
                    }
                } else if (metadata.getColumnType(i) == Types.BLOB) {
                    Blob blob = result.getBlob(i);
                    String value = Base64.encodeBytes(blob.getBytes(0L, (int) blob.length()));
                    values.add(value);
                } else {
                    values.add(result.getObject(name).toString());
                }
            }
            if (id == null) {
                id = Integer.toString(row);
            }
            listener.row(index, type, id, keys, values);
            return true;
        }
        return false;
    }

    /**
     * Close result set
     * @param result
     * @throws SQLException 
     */
    public void close(ResultSet result) throws SQLException {
        result.close();
    }

    /**
     * Close statement
     * @param statement
     * @throws SQLException 
     */
    public void close(PreparedStatement statement) throws SQLException {
        statement.close();
    }

    /**
     * Close connection
     * @param connection
     * @throws SQLException 
     */
    public void close(Connection connection) throws SQLException {
        connection.close();
    }

    private void bind(PreparedStatement pstmt, int i, Object value) throws SQLException {
        if (value == null) {
            pstmt.setNull(i, Types.VARCHAR);
        } else if (value instanceof String) {
            String s = (String) value;
            if ("$now".equals(s)) {
                pstmt.setDate(i, new Date(new java.util.Date().getTime()));
            } else {
                pstmt.setString(i, (String) value);
            }
        } else if (value instanceof Integer) {
            pstmt.setInt(i, ((Integer) value).intValue());
        } else if (value instanceof Long) {
            pstmt.setLong(i, ((Long) value).longValue());
        } else if (value instanceof BigDecimal) {
            pstmt.setBigDecimal(i, (BigDecimal) value);
        } else if (value instanceof Date) {
            pstmt.setDate(i, (Date) value);
        } else if (value instanceof Timestamp) {
            pstmt.setTimestamp(i, (Timestamp) value);
        } else if (value instanceof Float) {
            pstmt.setFloat(i, ((Float) value).floatValue());
        } else if (value instanceof Double) {
            pstmt.setDouble(i, ((Double) value).doubleValue());
        } else {
            pstmt.setObject(i, value);
        }
    }
}