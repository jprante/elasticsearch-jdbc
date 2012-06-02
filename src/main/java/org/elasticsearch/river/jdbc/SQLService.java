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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
import org.elasticsearch.common.Base64;

public class SQLService {

    public Connection getConnection(final String driverClassName,
            final String jdbcURL, final String username, final String password)
            throws ClassNotFoundException, SQLException {
        Class.forName(driverClassName);
        Connection connection = DriverManager.getConnection(jdbcURL, username, password);
        connection.setReadOnly(true);
        connection.setAutoCommit(false);
        return connection;
    }

    public PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }

    public ResultSet execute(PreparedStatement statement) throws SQLException {
        return statement.executeQuery();
    }

    public boolean nextRow(ResultSet result, ResultListener listener) throws SQLException, IOException {
        ResultSetMetaData metadata = result.getMetaData();
        if (result.next()) {
            int columns = metadata.getColumnCount();
            for (int i = 1; i <= columns; i++) {
                String name = metadata.getColumnName(i);
                if ("id".equalsIgnoreCase(name)) {
                    listener.id(result.getString(name));
                }
                else if (metadata.getColumnType(i) == java.sql.Types.ARRAY) {
                    listener.field(name, result.getArray(name).toString());
                } else if (metadata.getColumnType(i) == java.sql.Types.BIGINT) {
                    listener.field(name, result.getInt(name));
                } else if (metadata.getColumnType(i) == java.sql.Types.BOOLEAN) {
                    listener.field(name, result.getBoolean(name));
                } else if (metadata.getColumnType(i) == java.sql.Types.DOUBLE) {
                    listener.field(name, result.getDouble(name));
                } else if (metadata.getColumnType(i) == java.sql.Types.FLOAT) {
                    listener.field(name, result.getFloat(name));
                } else if (metadata.getColumnType(i) == java.sql.Types.INTEGER) {
                    listener.field(name, result.getInt(name));
                } else if (metadata.getColumnType(i) == java.sql.Types.NVARCHAR) {
                    listener.field(name, result.getNString(name));
                } else if (metadata.getColumnType(i) == java.sql.Types.VARCHAR) {
                    listener.field(name, result.getString(name));
                } else if (metadata.getColumnType(i) == java.sql.Types.TINYINT) {
                    listener.field(name, result.getInt(name));
                } else if (metadata.getColumnType(i) == java.sql.Types.SMALLINT) {
                    listener.field(name, result.getInt(name));
                } else if (metadata.getColumnType(i) == java.sql.Types.DATE) {
                    listener.field(name, result.getDate(name).getTime() );
                } else if (metadata.getColumnType(i) == java.sql.Types.TIMESTAMP) {
                    listener.field(name, result.getTimestamp(name).getTime() );
                } else if (metadata.getColumnType(i) == java.sql.Types.BLOB) {
                    Blob blob = result.getBlob(name);
                    String value = Base64.encodeBytes(blob.getBytes(0L, (int)blob.length()));
                    listener.field(name, value );
                } else {
                    listener.field(name, result.getObject(name).toString() );
                }
            }
            return true;
        }
        return false;
    }

    public void close(ResultSet result) throws SQLException {
        result.close();
    }

    public void close(PreparedStatement statement) throws SQLException {
        statement.close();
    }

    public void close(Connection connection) throws SQLException {
        connection.close();
    }

    public void bind(PreparedStatement pstmt, Map m, Object[] keys) throws SQLException {
        for (int i = 1; i <= keys.length; i++) {
            Object value = m.get(keys[i - 1]);
            if (keys[i - 1] != null) {
                bind(pstmt, i, value);
            }
        }
    }

    public void bind(PreparedStatement pstmt, int i, Object value) throws SQLException {
        if (value == null) {
            pstmt.setNull(i, Types.VARCHAR);
        } else if (value instanceof String) {
            pstmt.setString(i, (String) value);
        } else if (value instanceof Integer) {
            pstmt.setInt(i, ((Integer) value).intValue());
        } else if (value instanceof BigDecimal) {
            pstmt.setBigDecimal(i, (BigDecimal) value);
        } else if (value instanceof Timestamp) {
            pstmt.setTimestamp(i, (Timestamp) value);
        } else if (value instanceof Double) {
            pstmt.setDouble(i, ((Double) value).doubleValue());
        } else {
            pstmt.setObject(i, value);
        }
    }
}