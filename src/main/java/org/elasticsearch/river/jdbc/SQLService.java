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
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.logging.ESLogger;

/**
 * The SQL service class manages the SQL access to the JDBC connection.
 */
public class SQLService implements BulkAcknowledge {

    private ESLogger logger;
    private Connection connection;
    private int rounding;
    private int scale = -1;

    public SQLService() {
        this(null);
    }

    public SQLService(ESLogger logger) {
        this.logger = logger;
    }

    public SQLService setRounding(String rounding) {
        if ("ceiling".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_CEILING;
        } else if ("down".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_DOWN;
        } else if ("floor".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_FLOOR;
        } else if ("halfdown".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_DOWN;
        } else if ("halfeven".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_EVEN;
        } else if ("halfup".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_HALF_UP;
        } else if ("unnecessary".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_UNNECESSARY;
        } else if ("up".equalsIgnoreCase(rounding)) {
            this.rounding = BigDecimal.ROUND_UP;
        }
        return this;
    }

    public SQLService setPrecision(int scale) {
        this.scale = scale;
        return this;
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
     * @param riverName the name of the river
     * @param optype the operation type
     * @param interval the interval to cover, in milliseconds (back from current
     * time)
     * @return a preapred river table statement
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
            String parent = null;
            processRow(result, listener, "index", index, type, id, parent);
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
            String parent = null;
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
                } else if ("_parent".equalsIgnoreCase(name)) {
                	parent = result.getString(i);
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
                    processRow(rs, listener, operation, index, type, id, parent);
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

    private void processRow(ResultSet result, RowListener listener, String operation, String index, String type, String id, String parent)
            throws SQLException, IOException {
        LinkedList<String> keys = new LinkedList<String>();
        LinkedList<Object> values = new LinkedList<Object>();
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
            } else if ("_parent".equalsIgnoreCase(name)) {
            	parent = result.getString(i);
            	continue;
            }
            keys.add(name);
            switch (metadata.getColumnType(i)) {
                /**
                 * The JDBC types CHAR, VARCHAR, and LONGVARCHAR are closely
                 * related. CHAR represents a small, fixed-length character
                 * string, VARCHAR represents a small, variable-length character
                 * string, and LONGVARCHAR represents a large, variable-length
                 * character string.
                 */
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR: {
                    String s = result.getString(i);
                    values.add(s);
                    break;
                }
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR: {
                    String s = result.getNString(i);
                    values.add(s);
                    break;
                }
                /**
                 * The JDBC types BINARY, VARBINARY, and LONGVARBINARY are
                 * closely related. BINARY represents a small, fixed-length
                 * binary value, VARBINARY represents a small, variable-length
                 * binary value, and LONGVARBINARY represents a large,
                 * variable-length binary value
                 */
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY: {
                    byte[] b = result.getBytes(name);
                    values.add(b);
                    break;
                }
                /**
                 * The JDBC type ARRAY represents the SQL3 type ARRAY.
                 *
                 * An ARRAY value is mapped to an instance of the Array
                 * interface in the Java programming language. If a driver
                 * follows the standard implementation, an Array object
                 * logically points to an ARRAY value on the server rather than
                 * containing the elements of the ARRAY object, which can
                 * greatly increase efficiency. The Array interface contains
                 * methods for materializing the elements of the ARRAY object on
                 * the client in the form of either an array or a ResultSet
                 * object.
                 */
                case Types.ARRAY: {
                    Array a = result.getArray(i);
                    values.add(a != null ? a.toString() : null);
                    break;
                }
                /**
                 * The JDBC type BIGINT represents a 64-bit signed integer value
                 * between -9223372036854775808 and 9223372036854775807.
                 *
                 * The corresponding SQL type BIGINT is a nonstandard extension
                 * to SQL. In practice the SQL BIGINT type is not yet currently
                 * implemented by any of the major databases, and we recommend
                 * that its use be avoided in code that is intended to be
                 * portable.
                 *
                 * The recommended Java mapping for the BIGINT type is as a Java
                 * long.
                 */
                case Types.BIGINT: {
                    long l = result.getLong(i);
                    values.add(l);
                    break;
                }
                /**
                 * The JDBC type BIT represents a single bit value that can be
                 * zero or one.
                 *
                 * SQL-92 defines an SQL BIT type. However, unlike the JDBC BIT
                 * type, this SQL-92 BIT type can be used as a parameterized
                 * type to define a fixed-length binary string. Fortunately,
                 * SQL-92 also permits the use of the simple non-parameterized
                 * BIT type to represent a single binary digit, and this usage
                 * corresponds to the JDBC BIT type. Unfortunately, the SQL-92
                 * BIT type is only required in "full" SQL-92 and is currently
                 * supported by only a subset of the major databases. Portable
                 * code may therefore prefer to use the JDBC SMALLINT type,
                 * which is widely supported.
                 */
                case Types.BIT: {
                    int n = result.getInt(i);
                    values.add(n);
                    break;
                }
                /**
                 * The JDBC type BOOLEAN, which is new in the JDBC 3.0 API, maps
                 * to a boolean in the Java programming language. It provides a
                 * representation of true and false, and therefore is a better
                 * match than the JDBC type BIT, which is either 1 or 0.
                 */
                case Types.BOOLEAN: {
                    boolean b = result.getBoolean(i);
                    values.add(b);
                    break;
                }
                /**
                 * The JDBC type BLOB represents an SQL3 BLOB (Binary Large
                 * Object).
                 *
                 * A JDBC BLOB value is mapped to an instance of the Blob
                 * interface in the Java programming language. If a driver
                 * follows the standard implementation, a Blob object logically
                 * points to the BLOB value on the server rather than containing
                 * its binary data, greatly improving efficiency. The Blob
                 * interface provides methods for materializing the BLOB data on
                 * the client when that is desired.
                 */
                case Types.BLOB: {
                    Blob blob = result.getBlob(i);
                    if (blob != null) {
                        long n = blob.length();
                        if (n > Integer.MAX_VALUE) {
                            throw new IOException("can't process blob larger than Integer.MAX_VALUE");
                        }
                        values.add(blob.getBytes(1, (int) n));
                        blob.free();
                    }
                    break;
                }
                /**
                 * The JDBC type CLOB represents the SQL3 type CLOB (Character
                 * Large Object).
                 *
                 * A JDBC CLOB value is mapped to an instance of the Clob
                 * interface in the Java programming language. If a driver
                 * follows the standard implementation, a Clob object logically
                 * points to the CLOB value on the server rather than containing
                 * its character data, greatly improving efficiency. Two of the
                 * methods on the Clob interface materialize the data of a CLOB
                 * object on the client.
                 */
                case Types.CLOB: {
                    Clob clob = result.getClob(i);
                    if (clob != null) {
                        long n = clob.length();
                        if (n > Integer.MAX_VALUE) {
                            throw new IOException("can't process clob larger than Integer.MAX_VALUE");
                        }
                        values.add(clob.getSubString(1, (int) n));
                        clob.free();
                    }
                    break;
                }
                case Types.NCLOB: {
                    NClob nclob = result.getNClob(i);
                    if (nclob != null) {
                        long n = nclob.length();
                        if (n > Integer.MAX_VALUE) {
                            throw new IOException("can't process nclob larger than Integer.MAX_VALUE");
                        }
                        values.add(nclob.getSubString(1, (int) n));
                        nclob.free();
                    }
                    break;
                }
                /**
                 * The JDBC type DATALINK, new in the JDBC 3.0 API, is a column
                 * value that references a file that is outside of a data source
                 * but is managed by the data source. It maps to the Java type
                 * java.net.URL and provides a way to manage external files. For
                 * instance, if the data source is a DBMS, the concurrency
                 * controls it enforces on its own data can be applied to the
                 * external file as well.
                 *
                 * A DATALINK value is retrieved from a ResultSet object with
                 * the ResultSet methods getURL or getObject. If the Java
                 * platform does not support the type of URL returned by getURL
                 * or getObject, a DATALINK value can be retrieved as a String
                 * object with the method getString.
                 *
                 * java.net.URL values are stored in a database using the method
                 * setURL. If the Java platform does not support the type of URL
                 * being set, the method setString can be used instead.
                 *
                 *
                 */
                case Types.DATALINK: {
                    URL url = result.getURL(i);
                    values.add(url);
                    break;
                }
                /**
                 * The JDBC DATE type represents a date consisting of day,
                 * month, and year. The corresponding SQL DATE type is defined
                 * in SQL-92, but it is implemented by only a subset of the
                 * major databases. Some databases offer alternative SQL types
                 * that support similar semantics.
                 */
                case Types.DATE: {
                    try {
                        Date d = result.getDate(i);
                        values.add(d != null ? DateUtil.formatDateISO(d.getTime()) : null);
                    } catch (SQLException e) {
                        values.add(null);
                    }
                    break;
                }
                case Types.TIME: {
                    try {
                        Time t = result.getTime(i);
                        values.add(t != null ? DateUtil.formatDateISO(t.getTime()) : null);
                    } catch (SQLException e) {
                        values.add(null);
                    }
                    break;
                }
                case Types.TIMESTAMP: {
                    try {
                        Timestamp t = result.getTimestamp(i);
                        values.add(t != null ? DateUtil.formatDateISO(t.getTime()) : null);
                    } catch (SQLException e) {
                        // java.sql.SQLException: Cannot convert value '0000-00-00 00:00:00' from column ... to TIMESTAMP.
                        values.add(null);
                    }
                    break;
                }
                /**
                 * The JDBC types DECIMAL and NUMERIC are very similar. They
                 * both represent fixed-precision decimal values.
                 *
                 * The corresponding SQL types DECIMAL and NUMERIC are defined
                 * in SQL-92 and are very widely implemented. These SQL types
                 * take precision and scale parameters. The precision is the
                 * total number of decimal digits supported, and the scale is
                 * the number of decimal digits after the decimal point. For
                 * most DBMSs, the scale is less than or equal to the precision.
                 * So for example, the value "12.345" has a precision of 5 and a
                 * scale of 3, and the value ".11" has a precision of 2 and a
                 * scale of 2. JDBC requires that all DECIMAL and NUMERIC types
                 * support both a precision and a scale of at least 15.
                 *
                 * The sole distinction between DECIMAL and NUMERIC is that the
                 * SQL-92 specification requires that NUMERIC types be
                 * represented with exactly the specified precision, whereas for
                 * DECIMAL types, it allows an implementation to add additional
                 * precision beyond that specified when the type was created.
                 * Thus a column created with type NUMERIC(12,4) will always be
                 * represented with exactly 12 digits, whereas a column created
                 * with type DECIMAL(12,4) might be represented by some larger
                 * number of digits.
                 *
                 * The recommended Java mapping for the DECIMAL and NUMERIC
                 * types is java.math.BigDecimal. The java.math.BigDecimal type
                 * provides math operations to allow BigDecimal types to be
                 * added, subtracted, multiplied, and divided with other
                 * BigDecimal types, with integer types, and with floating point
                 * types.
                 *
                 * The method recommended for retrieving DECIMAL and NUMERIC
                 * values is ResultSet.getBigDecimal. JDBC also allows access to
                 * these SQL types as simple Strings or arrays of char. Thus,
                 * Java programmers can use getString to receive a DECIMAL or
                 * NUMERIC result. However, this makes the common case where
                 * DECIMAL or NUMERIC are used for currency values rather
                 * awkward, since it means that application writers have to
                 * perform math on strings. It is also possible to retrieve
                 * these SQL types as any of the Java numeric types.
                 */
                case Types.DECIMAL:
                case Types.NUMERIC: {
                    BigDecimal bd = null;
                    try {
                        bd = result.getBigDecimal(i);
                    } catch (NullPointerException e) {
                        // getBigDecimal() should get obsolete. Most seem to use getString/getObject anayway.
                        // But is it true? JDBC NPE exists since 13 years? 
                        // http://forums.codeguru.com/archive/index.php/t-32443.html
                        // Null values are driving us nuts in JDBC:
                        // http://stackoverflow.com/questions/2777214/when-accessing-resultsets-in-jdbc-is-there-an-elegant-way-to-distinguish-betwee
                    }
                    values.add(bd == null ? null
                            : scale >= 0 ? bd.setScale(scale, rounding).doubleValue()
                            : bd.toString());
                    break;
                }
                /**
                 * The JDBC type DOUBLE represents a "double precision" floating
                 * point number that supports 15 digits of mantissa.
                 *
                 * The corresponding SQL type is DOUBLE PRECISION, which is
                 * defined in SQL-92 and is widely supported by the major
                 * databases. The SQL-92 standard leaves the precision of DOUBLE
                 * PRECISION up to the implementation, but in practice all the
                 * major databases supporting DOUBLE PRECISION support a
                 * mantissa precision of at least 15 digits.
                 *
                 * The recommended Java mapping for the DOUBLE type is as a Java
                 * double.
                 */
                case Types.DOUBLE: {
                    double d = result.getDouble(i);
                    values.add(d);
                    break;
                }
                /**
                 * The JDBC type FLOAT is basically equivalent to the JDBC type
                 * DOUBLE. We provided both FLOAT and DOUBLE in a possibly
                 * misguided attempt at consistency with previous database APIs.
                 * FLOAT represents a "double precision" floating point number
                 * that supports 15 digits of mantissa.
                 *
                 * The corresponding SQL type FLOAT is defined in SQL-92. The
                 * SQL-92 standard leaves the precision of FLOAT up to the
                 * implementation, but in practice all the major databases
                 * supporting FLOAT support a mantissa precision of at least 15
                 * digits.
                 *
                 * The recommended Java mapping for the FLOAT type is as a Java
                 * double. However, because of the potential confusion between
                 * the double precision SQL FLOAT and the single precision Java
                 * float, we recommend that JDBC programmers should normally use
                 * the JDBC DOUBLE type in preference to FLOAT.
                 */
                case Types.FLOAT: {
                    double d = result.getDouble(i);
                    values.add(d);
                    break;
                }
                /**
                 * The JDBC type INTEGER represents a 32-bit signed integer
                 * value ranging between -2147483648 and 2147483647.
                 *
                 * The corresponding SQL type, INTEGER, is defined in SQL-92 and
                 * is widely supported by all the major databases. The SQL-92
                 * standard leaves the precision of INTEGER up to the
                 * implementation, but in practice all the major databases
                 * support at least 32 bits.
                 *
                 * The recommended Java mapping for the INTEGER type is as a
                 * Java int.
                 */
                case Types.INTEGER: {
                    int n = result.getInt(i);
                    values.add(n);
                    break;
                }
                /**
                 * The JDBC type JAVA_OBJECT, added in the JDBC 2.0 core API,
                 * makes it easier to use objects in the Java programming
                 * language as values in a database. JAVA_OBJECT is simply a
                 * type code for an instance of a class defined in the Java
                 * programming language that is stored as a database object. The
                 * type JAVA_OBJECT is used by a database whose type system has
                 * been extended so that it can store Java objects directly. The
                 * JAVA_OBJECT value may be stored as a serialized Java object,
                 * or it may be stored in some vendor-specific format.
                 *
                 * The type JAVA_OBJECT is one of the possible values for the
                 * column DATA_TYPE in the ResultSet objects returned by various
                 * DatabaseMetaData methods, including getTypeInfo, getColumns,
                 * and getUDTs. The method getUDTs, part of the new JDBC 2.0
                 * core API, will return information about the Java objects
                 * contained in a particular schema when it is given the
                 * appropriate parameters. Having this information available
                 * facilitates using a Java class as a database type.
                 */
                case Types.OTHER:
                case Types.JAVA_OBJECT: {
                    Object o = result.getObject(i);
                    values.add(o);
                    break;
                }
                /**
                 * The JDBC type REAL represents a "single precision" floating
                 * point number that supports seven digits of mantissa.
                 *
                 * The corresponding SQL type REAL is defined in SQL-92 and is
                 * widely, though not universally, supported by the major
                 * databases. The SQL-92 standard leaves the precision of REAL
                 * up to the implementation, but in practice all the major
                 * databases supporting REAL support a mantissa precision of at
                 * least seven digits.
                 *
                 * The recommended Java mapping for the REAL type is as a Java
                 * float.
                 */
                case Types.REAL: {
                    float f = result.getFloat(i);
                    values.add(f);
                    break;
                }
                /**
                 * The JDBC type SMALLINT represents a 16-bit signed integer
                 * value between -32768 and 32767.
                 *
                 * The corresponding SQL type, SMALLINT, is defined in SQL-92
                 * and is supported by all the major databases. The SQL-92
                 * standard leaves the precision of SMALLINT up to the
                 * implementation, but in practice, all the major databases
                 * support at least 16 bits.
                 *
                 * The recommended Java mapping for the JDBC SMALLINT type is as
                 * a Java short.
                 */
                case Types.SMALLINT: {
                    int n = result.getInt(i);
                    values.add(n);
                    break;
                }
                case Types.SQLXML: {
                    SQLXML xml = result.getSQLXML(columns);
                    values.add(xml != null ? xml.getString() : null);
                    break;
                }
                /**
                 * The JDBC type TINYINT represents an 8-bit integer value
                 * between 0 and 255 that may be signed or unsigned.
                 *
                 * The corresponding SQL type, TINYINT, is currently supported
                 * by only a subset of the major databases. Portable code may
                 * therefore prefer to use the JDBC SMALLINT type, which is
                 * widely supported.
                 *
                 * The recommended Java mapping for the JDBC TINYINT type is as
                 * either a Java byte or a Java short. The 8-bit Java byte type
                 * represents a signed value from -128 to 127, so it may not
                 * always be appropriate for larger TINYINT values, whereas the
                 * 16-bit Java short will always be able to hold all TINYINT
                 * values.
                 */
                case Types.TINYINT: {
                    int n = result.getInt(i);
                    values.add(n);
                    break;
                }
                case Types.NULL: {
                    values.add(null);
                    break;
                }
                /**
                 * The JDBC type DISTINCT field (Types class)>DISTINCT
                 * represents the SQL3 type DISTINCT.
                 *
                 * The standard mapping for a DISTINCT type is to the Java type
                 * to which the base type of a DISTINCT object would be mapped.
                 * For example, a DISTINCT type based on a CHAR would be mapped
                 * to a String object, and a DISTINCT type based on an SQL
                 * INTEGER would be mapped to an int.
                 *
                 * The DISTINCT type may optionally have a custom mapping to a
                 * class in the Java programming language. A custom mapping
                 * consists of a class that implements the interface SQLData and
                 * an entry in a java.util.Map object.
                 */
                case Types.DISTINCT: {
                    logger.warn("JDBC type not implemented: {}", metadata.getColumnType(i));
                    values.add(null);
                    break;
                }
                /**
                 * The JDBC type STRUCT represents the SQL99 structured type. An
                 * SQL structured type, which is defined by a user with a CREATE
                 * TYPE statement, consists of one or more attributes. These
                 * attributes may be any SQL data type, built-in or
                 * user-defined.
                 *
                 * The standard mapping for the SQL type STRUCT is to a Struct
                 * object in the Java programming language. A Struct object
                 * contains a value for each attribute of the STRUCT value it
                 * represents.
                 *
                 * A STRUCT value may optionally be custom mapped to a class in
                 * the Java programming language, and each attribute in the
                 * STRUCT may be mapped to a field in the class. A custom
                 * mapping consists of a class that implements the interface
                 * SQLData and an entry in a java.util.Map object.
                 *
                 *
                 */
                case Types.STRUCT: {
                    logger.warn("JDBC type not implemented: {}", metadata.getColumnType(i));
                    values.add(null);
                    break;
                }
                case Types.REF: {
                    logger.warn("JDBC type not implemented: {}", metadata.getColumnType(i));
                    values.add(null);
                    break;
                }
                case Types.ROWID: {
                    logger.warn("JDBC type not implemented: {}", metadata.getColumnType(i));
                    values.add(null);
                    break;
                }
                default: {
                    logger.warn("unknown JDBC type ignored: {}", metadata.getColumnType(i));
                    values.add(null);
                    break;
                }
            }
        }
        if (id == null) {
            id = Integer.toString(result.getRow());
        }
        if (listener != null) {
            listener.row(operation, index, type, id, parent, keys, values);
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
     * target_timestamp, target_operation, target_failed, target_message.
     *
     * @param riverName
     * @param response
     * @throws IOException
     */
    @Override
    public void acknowledge(String riverName, BulkItemResponse[] response) throws IOException {
        if (response == null) {
            logger.warn("can't acknowledge null bulk response");
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