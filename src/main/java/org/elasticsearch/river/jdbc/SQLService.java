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
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.logging.ESLogger;

/**
 * The SQL service class manages the SQL access to the JDBC connection.
 */
public class SQLService implements BulkAcknowledge {

    private ESLogger logger;
    private Connection connection;

    public SQLService() {
        this(null);
    }

    public SQLService(ESLogger logger) {
        this.logger = logger;
    }

    /**
     * Get JDBC connection
     *
     * @param driverClassName
     * @param jdbcURL
     * @param user
     * @param password
     * @return the connection
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public Connection getConnection(final String driverClassName,
            final String jdbcURL, final String user, final String password, boolean readOnly)
            throws ClassNotFoundException, SQLException {
        Class.forName(driverClassName);
        this.connection = DriverManager.getConnection(jdbcURL, user, password);
        connection.setReadOnly(readOnly);
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * Prepare statement
     *
     * @param connection
     * @param sql
     * @return a prepared statement
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(Connection connection, String sql)
            throws SQLException {
        return connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * Prepare statement for processing a river sync table
     *
     * @param connection the JDBC connection
     * @param riverSyncTable the river sync table
     * @param target the target name
     * @param from start date for sync
     * @param to rnd date for sync
     * @return
     * @throws SQLException
     */
    public PreparedStatement prepareRiverTableStatement(Connection connection, String riverName, String optype, long interval)
            throws SQLException {
        PreparedStatement p = connection.prepareStatement("select * from " + riverName + " where source_operation = ? and target_operation = 'n/a' and source_timestamp between ? and ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        p.setString(1, optype);
        java.util.Date d = new java.util.Date();
        long now = d.getTime();
        p.setTimestamp(2, new Timestamp(now - interval));
        p.setTimestamp(3, new Timestamp(now));
        return p;
    }

    /**
     * Bind values to prepared statement
     *
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
     *
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
     * Get next row and prepare the values for processing. The labels of each
     * columns are used for the RowListener as paths for JSON object merging.
     *
     * @param result the result set
     * @param listener the listener
     * @return true if row exists and was processed, false otherwise
     * @throws SQLException
     * @throws IOException
     */
    public boolean nextRow(ResultSet result, RowListener listener)
            throws SQLException, IOException {
        if (result.next()) {
            String index = null;
            String type = null;
            String id = null;
            processRow(result, listener, "index", index, type, id);
            return true;
        }
        return false;
    }

    /**
     * Get next row from a river table. The river table contains embedded SQL
     * which is executed.
     *
     * @param result the result set
     * @param listener the row listener
     * @return true if there was a row being executed, false otherwise
     * @throws SQLException
     * @throws IOException
     */
    public boolean nextRiverTableRow(ResultSet result, RowListener listener)
            throws SQLException, IOException {
        if (result.next()) {
            ResultSetMetaData metadata = result.getMetaData();
            String operation = null;
            String index = null;
            String type = null;
            String id = null;
            String sql = null;
            int columns = metadata.getColumnCount();
            for (int i = 1; i <= columns; i++) {
                String name = metadata.getColumnLabel(i);
                if ("_index".equalsIgnoreCase(name)) {
                    index = result.getString(i);
                } else if ("_type".equalsIgnoreCase(name)) {
                    type = result.getString(i);
                } else if ("_id".equalsIgnoreCase(name)) {
                    id = result.getString(i);
                } else if ("source_operation".equalsIgnoreCase(name)) {
                    operation = result.getString(i);
                } else if ("source_sql".equalsIgnoreCase(name)) {
                    sql = result.getString(i);
                }
            }
            if (sql != null) {
                // execute embedded SQL
                PreparedStatement stmt = prepareStatement(connection, sql);
                ResultSet rs = stmt.executeQuery();
                long rows = 0L;
                if (rs.next()) {
                    processRow(rs, listener, operation, index, type, id);
                    rows++;
                }
                logger.info("embedded sql gave " + rows + " rows");
                rs.close();
                stmt.close();
            }
            return true;
        }
        return false;
    }

    private void processRow(ResultSet result, RowListener listener, String operation, String index, String type, String id)
            throws SQLException, IOException {
        LinkedList<String> keys = new LinkedList();
        LinkedList<Object> values = new LinkedList();
        ResultSetMetaData metadata = result.getMetaData();
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
                values.add(DateUtil.formatDateISO(result.getDate(i).getTime()));
            } else if (metadata.getColumnType(i) == Types.TIMESTAMP) {
                try {
                    values.add(DateUtil.formatDateISO(result.getTimestamp(i).getTime()));
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
            id = Integer.toString(result.getRow());
        }
        if (listener != null) {
            listener.row(operation, index, type, id, keys, values);
        }
    }

    /**
     * Close result set
     *
     * @param result
     * @throws SQLException
     */
    public void close(ResultSet result) throws SQLException {
        result.close();
    }

    /**
     * Close statement
     *
     * @param statement
     * @throws SQLException
     */
    public void close(PreparedStatement statement) throws SQLException {
        statement.close();
    }

    /**
     * Close connection
     *
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

    /**
     * Acknowledge a bulk item response back to the river table. Fill columns
     * target_timestamp, taget_operation, target_failed, target_message.
     *
     * @param riverName
     * @param response
     * @throws IOException
     */
    @Override
    public void acknowledge(String riverName, BulkItemResponse[] response) throws IOException {
        if (response == null) {
            logger.warn("bulk response is null");
        }
        try {
            String sql = " update " + riverName + " set target_timestamp = ?, target_operation = ?, target_failed = ?, target_message = ? where source_operation = ? and _index = ? and _type = ? and _id = ? and target_operation = 'n/a'";
            PreparedStatement pstmt = connection.prepareStatement(sql);
            for (BulkItemResponse resp : response) {
                pstmt.setTimestamp(1, new Timestamp(new java.util.Date().getTime()));
                pstmt.setString(2, resp.opType());
                pstmt.setBoolean(3, resp.failed());
                pstmt.setString(4, resp.failureMessage());
                pstmt.setString(5, resp.opType());
                pstmt.setString(6, resp.index());
                pstmt.setString(7, resp.type());
                pstmt.setString(8, resp.id());
                pstmt.executeUpdate();                
            }
            pstmt.close();
            connection.commit();
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
    }
}