
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.SimpleValueListener;
import org.xbib.elasticsearch.river.jdbc.support.ValueListener;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

/**
 * A river source implementation for the 'simple' strategy.
 * <p/>
 * It connects to a JDCC database, fetches from it by using a merge method
 * provided by the river task. It does not process acknowledgements from the
 * river target side.
 * <p/>
 * The river source understands all the JDBC column types and parses them to an
 * appropriate Java object.
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class SimpleRiverSource implements RiverSource {

    private final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverSource.class.getSimpleName());

    protected RiverContext context;

    protected String url;

    protected String driver;

    protected String user;

    protected String password;

    protected Connection readConnection;

    protected Connection writeConnection;

    private int rounding;

    private int scale = -1;

    public SimpleRiverSource() {
    }

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "simple";
    }

    @Override
    public SimpleRiverSource riverContext(RiverContext context) {
        this.context = context;
        return this;
    }

    @Override
    public SimpleRiverSource driver(String driver) {
        this.driver = driver;
        try {
            // TODO: do we need this? older drivers?
            Class.forName(driver);
        } catch (ClassNotFoundException ex) {
            logger().error(ex.getMessage(), ex);
        }
        return this;
    }

    public String driver() {
        return driver;
    }

    @Override
    public SimpleRiverSource url(String url) {
        this.url = url;
        return this;
    }

    public String url() {
        return url;
    }

    @Override
    public SimpleRiverSource user(String user) {
        this.user = user;
        return this;
    }

    @Override
    public SimpleRiverSource password(String password) {
        this.password = password;
        return this;
    }

    /**
     * Get JDBC connection for reading
     *
     * @return the connection
     * @throws SQLException
     */
    @Override
    public Connection connectionForReading() throws SQLException {
        boolean cond = readConnection == null || readConnection.isClosed();
        try {
            cond = cond || !readConnection.isValid(5);
        } catch (AbstractMethodError e) {
            // old/buggy JDBC driver
            logger().debug(e.getMessage());
        } catch (SQLFeatureNotSupportedException e) {
            // postgresql does not support isValid()
            logger().debug(e.getMessage());
        }
        if (cond) {
            int retries = context != null ? context.retries() : 1;
            while (retries > 0) {
                retries--;
                try {
                    readConnection = DriverManager.getConnection(url, user, password);
                    // required by MySQL for large result streaming
                    readConnection.setReadOnly(true);
                    // Postgresql cursor mode condition:
                    // fetchsize > 0, no scrollable result set, no auto commit, no holdable cursors over commit
                    // https://github.com/pgjdbc/pgjdbc/blob/master/org/postgresql/jdbc2/AbstractJdbc2Statement.java#L514
                    //readConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    if (context != null) {
                        // many drivers don't like autocommit=true
                        readConnection.setAutoCommit(context.autocommit());
                    }
                    return readConnection;
                } catch (SQLException e) {
                    logger().error("while opening read connection: " + url + " " + e.getMessage(), e);
                    try {
                        Thread.sleep(context != null ? context.maxRetryWait().millis() : 1000L);
                    } catch (InterruptedException ex) {
                        // do nothing
                    }
                }
            }
        }
        return readConnection;
    }

    /**
     * Get JDBC connection for writing
     *
     * @return the connection
     * @throws SQLException
     */
    @Override
    public Connection connectionForWriting() throws SQLException {
        boolean cond = writeConnection == null || writeConnection.isClosed();
        try {
            cond = cond || !writeConnection.isValid(5);
        } catch (AbstractMethodError e) {
            // old JDBC driver
        } catch (SQLFeatureNotSupportedException e) {
            // postgresql does not support isValid()
        }
        if (cond) {
            int retries = context != null ? context.retries() : 1;
            while (retries > 0) {
                retries--;
                try {
                    writeConnection = DriverManager.getConnection(url, user, password);
                    if (context != null) {
                        // many drivers don't like autocommit=true
                        writeConnection.setAutoCommit(context.autocommit());
                    }
                    return writeConnection;
                } catch (SQLNonTransientConnectionException e) {
                    // ignore derby drop=true
                } catch (SQLException e) {
                    logger().error("while opening write connection: " + url + " " + e.getMessage(), e);
                    try {
                        Thread.sleep(context != null ? context.maxRetryWait().millis() : 1000L);
                    } catch (InterruptedException ex) {
                        // do nothing
                    }
                }
            }
        }
        return writeConnection;
    }

    @Override
    public String fetch() throws SQLException, IOException {
        String mergeDigest = null;
        if (context.pollStatementParams().isEmpty()) {
            Statement statement = null;
            ResultSet results = null;
            try {
                // Postgresql: do not use prepareStatement.
                // Postgresql requires direct use of executeQuery(sql) for cursor with fetchsize
                statement = connectionForReading().createStatement();
                results = executeQuery(statement, getSql());
                ValueListener listener = new SimpleValueListener()
                        .target(context.riverMouth())
                        .digest(context.digesting());
                mergeDigest = merge(results, listener);
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                close(results);
                close(statement);
                acknowledge();
                closeReading();
                closeWriting();
            }
        } else if (context.callable()) {
            // call stored procedure
            CallableStatement statement = null;
            ResultSet results = null;
            try {
                statement = connectionForReading().prepareCall(getSql());
                bind(statement, context.pollStatementParams());
                results = executeQuery(statement);
                ValueListener listener = new SimpleValueListener()
                        .target(context.riverMouth())
                        .digest(context.digesting());
                mergeDigest = merge(results, listener);
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                close(results);
                close(statement);
                acknowledge();
                closeReading();
                closeWriting();
            }
        } else {
            PreparedStatement statement = null;
            ResultSet results = null;
            try {
                statement = prepareQuery(getSql());
                bind(statement, context.pollStatementParams());
                results = executeQuery(statement);
                ValueListener listener = new SimpleValueListener()
                        .target(context.riverMouth())
                        .digest(context.digesting());
                mergeDigest = merge(results, listener);
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                close(results);
                close(statement);
                acknowledge();
                closeReading();
                closeWriting();
            }
        }
        return mergeDigest;
    }

    /**
     * Merge rows.
     *
     * @param results  the ResultSet
     * @param listener
     * @return a digest of the merged row content
     * @throws IOException
     * @throws java.security.NoSuchAlgorithmException
     * @throws SQLException
     */
    public String merge(ResultSet results, ValueListener listener)
            throws SQLException, IOException, ParseException, NoSuchAlgorithmException {
        long rows = 0L;
        beforeFirstRow(results, listener);
        while (nextRow(results, listener)) {
            rows++;
        }
        if (rows > 0) {
            logger().info("merged {} rows", rows);
        } else {
            logger().info("no rows to merge");
        }
        listener.reset();
        return context.digesting() && listener.digest() != null
                ? Base64.encodeBytes(listener.digest().digest()) : null;
    }


    /**
     * Send acknowledge SQL command if exists.
     *
     * @throws SQLException
     */
    public void acknowledge() throws SQLException {
        // send acknowledge statement if defined
        if (context.pollAckStatement() != null) {
            Connection connection = connectionForWriting();
            PreparedStatement statement = prepareUpdate(context.pollAckStatement());
            if (context.pollAckStatementParams() != null) {
                bind(statement, context.pollAckStatementParams());
            }
            statement.execute();
            close(statement);
            try {
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
            } catch (SQLException e) {
                //  Can't call commit when autocommit=true
            }
            closeWriting();
        }
    }

    private String getSql() throws IOException {
        String sql = context.pollStatement();
        if (sql.endsWith(".sql")) {
            Reader r = new InputStreamReader(new FileInputStream(sql), "UTF-8");
            sql = Streams.copyToString(r);
            r.close();
        }
        return sql;
    }

    /**
     * Prepare a query statement
     *
     * @param sql
     * @return a prepared statement
     * @throws SQLException
     */
    @Override
    public PreparedStatement prepareQuery(String sql) throws SQLException {
        Connection connection = connectionForReading();
        if (connection == null) {
            throw new SQLException("can't connect to source " + url);
        }
        logger().debug("preparing statement with SQL {}", sql);
        return connection.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * Prepare a query statement
     *
     * @param sql
     * @return a prepared statement
     * @throws SQLException
     */
    @Override
    public PreparedStatement prepareUpdate(String sql) throws SQLException {
        if (sql.endsWith(".sql")) {
            try {
                Reader r = new InputStreamReader(new FileInputStream(sql), "UTF-8");
                sql = Streams.copyToString(r);
                r.close();
            } catch (IOException e) {
                throw new SQLException("file not found: " + sql);
            }
        }
        Connection connection = connectionForWriting();
        if (connection == null) {
            throw new SQLException("can't connect to source " + url);
        }
        return connection.prepareStatement(sql);
    }

    /**
     * Bind values to prepared statement
     *
     * @param pstmt
     * @param values
     * @throws SQLException
     */
    @Override
    public SimpleRiverSource bind(PreparedStatement pstmt, List<? extends Object> values) throws SQLException {
        if (values == null) {
            logger().warn("no values given for bind");
            return this;
        }
        for (int i = 1; i <= values.size(); i++) {
            bind(pstmt, i, values.get(i - 1));
        }
        return this;
    }

    /**
     * Execute prepared query statement
     *
     * @param statement
     * @return the result set
     * @throws SQLException
     */
    @Override
    public ResultSet executeQuery(PreparedStatement statement) throws SQLException {
        statement.setMaxRows(context.maxRows());
        statement.setFetchSize(context.fetchSize());
        logger().debug("executing prepared statement");
        return statement.executeQuery();
    }

    /**
     * Execute query statement
     *
     * @param sql
     * @return the result set
     * @throws SQLException
     */
    @Override
    public ResultSet executeQuery(Statement statement, String sql) throws SQLException {
        statement.setMaxRows(context.maxRows());
        statement.setFetchSize(context.fetchSize());
        logger().debug("executing SQL {}", sql);
        return statement.executeQuery(sql);
    }


    /**
     * Execute prepared update statement
     *
     * @param statement
     * @return the result set
     * @throws SQLException
     */
    @Override
    public RiverSource executeUpdate(PreparedStatement statement) throws SQLException {
        statement.executeUpdate();
        if (!writeConnection.getAutoCommit()) {
            writeConnection.commit();
        }
        return this;
    }

    public void beforeFirstRow(ResultSet result, ValueListener listener)
            throws SQLException, IOException, ParseException {
        ResultSetMetaData metadata = result.getMetaData();
        int columns = metadata.getColumnCount();
        List<String> keys = new LinkedList();
        for (int i = 1; i <= columns; i++) {
            keys.add(metadata.getColumnLabel(i));
        }
        if (listener != null) {
            listener.keys(keys);
        }
    }

    /**
     * Get next row and prepare the values for processing. The labels of each
     * columns are used for the ValueListener as paths for JSON object merging.
     *
     * @param result   the result set
     * @param listener the listener
     * @return true if row exists and was processed, false otherwise
     * @throws SQLException
     * @throws IOException
     */
    @Override
    public boolean nextRow(ResultSet result, ValueListener listener)
            throws SQLException, IOException, ParseException {
        if (result.next()) {
            processRow(result, listener);
            return true;
        }
        return false;
    }

    private void processRow(ResultSet result, ValueListener listener)
            throws SQLException, IOException, ParseException {
        Locale locale = context != null ? context.locale() != null ? context.locale() : Locale.getDefault() : Locale.getDefault();
        List<Object> values = new LinkedList();
        ResultSetMetaData metadata = result.getMetaData();
        int columns = metadata.getColumnCount();
        for (int i = 1; i <= columns; i++) {
            Object value = parseType(result, i, metadata.getColumnType(i), locale);
            if (logger().isTraceEnabled()) {
                logger().trace("value={} class={}", value, value != null ? value.getClass().getName() : "");
            }
            values.add(value);
        }
        if (listener != null) {
            listener.values(values);
        }
    }

    /**
     * Close result set
     *
     * @param result
     * @throws SQLException
     */
    @Override
    public SimpleRiverSource close(ResultSet result) throws SQLException {
        if (result != null) {
            result.close();
        }
        return this;
    }

    /**
     * Close statement
     *
     * @param statement
     * @throws SQLException
     */
    @Override
    public SimpleRiverSource close(Statement statement) throws SQLException {
        if (statement != null) {
            statement.close();
        }
        return this;
    }

    /**
     * Close read connection
     *
     * @throws SQLException
     */
    @Override
    public SimpleRiverSource closeReading() {
        try {
            if (readConnection != null) {
                // always commit before close to finish cursors/transactions
                if (!readConnection.getAutoCommit()) {
                    readConnection.commit();
                }
                if (!readConnection.isClosed()) {
                    readConnection.close();
                }
            }
        } catch (SQLException e) {
            logger().warn("while closing read connection: " + e.getMessage());
        }
        return this;
    }

    /**
     * Close read connection
     *
     * @throws SQLException
     */
    @Override
    public SimpleRiverSource closeWriting() {
        try {
            if (writeConnection != null) {
                // always commit before close to finish cursors/transactions
                if (!writeConnection.getAutoCommit()) {
                    writeConnection.commit();
                }
                if (!writeConnection.isClosed()) {
                    writeConnection.close();
                }
            }
        } catch (SQLException e) {
            logger().warn("while closing write connection: " + e.getMessage());
        }
        return this;
    }

    @Override
    public SimpleRiverSource acknowledge(BulkResponse response) throws IOException {
        // no, we do not acknowledge bulk in this strategy
        return this;
    }

    @Override
    public SimpleRiverSource rounding(String rounding) {
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

    @Override
    public SimpleRiverSource precision(int scale) {
        this.scale = scale;
        return this;
    }

    private static final String ISO_FORMAT_SECONDS = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String FORMAT_SECONDS = "yyyy-MM-dd HH:mm:ss";
    private final static DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public static String formatNow() {
        return formatDateISO(new java.util.Date());
    }

    public static String formatDateISO(long millis) {
        return new DateTime(millis).toString(ISO_FORMAT_SECONDS);
    }

    public static String formatDateStandard(java.util.Date date) {
        if (date == null) {
            return null;
        }
        return new DateTime(date).toString(FORMAT_SECONDS);
    }

    public synchronized static String formatDateISO(java.util.Date date) {
        if (date == null) {
            return null;
        }
        return new DateTime(date).toString(ISO_FORMAT_SECONDS);
    }

    public synchronized static java.util.Date parseDateISO(String value) {
        if (value == null) {
            return null;
        }
        try {
            return df.parseDateTime(value).toDate();
        } catch (Exception e) {
            // ignore
        }

        try {
            return DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(value).toDate();
        } catch (Exception e) {
            return null;
        }
    }

    public synchronized static java.util.Date parseDate(String value) {
        if (value == null) {
            return null;
        }
        try {
            return DateTimeFormat.forPattern(FORMAT_SECONDS).parseDateTime(value).toDate();
        } catch (Exception e) {
        }

        try {
            return DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(value).toDate();
        } catch (Exception e) {
            return null;
        }
    }

    private void bind(PreparedStatement pstmt, int i, Object value) throws SQLException {
        if (value == null) {
            pstmt.setNull(i, Types.VARCHAR);
        } else if (value instanceof String) {
            String s = (String) value;
            if ("$now".equals(s)) {
                pstmt.setDate(i, new Date(new java.util.Date().getTime()));
            } else if ("$job".equals(s)) {
                logger().debug("job = {}", context.job());
                pstmt.setString(i, context.job());
            } else {
                pstmt.setString(i, (String) value);
            }
        } else if (value instanceof Integer) {
            pstmt.setInt(i, (Integer) value);
        } else if (value instanceof Long) {
            pstmt.setLong(i, (Long) value);
        } else if (value instanceof BigDecimal) {
            pstmt.setBigDecimal(i, (BigDecimal) value);
        } else if (value instanceof Date) {
            pstmt.setDate(i, (Date) value);
        } else if (value instanceof Timestamp) {
            pstmt.setTimestamp(i, (Timestamp) value);
        } else if (value instanceof Float) {
            pstmt.setFloat(i, (Float) value);
        } else if (value instanceof Double) {
            pstmt.setDouble(i, (Double) value);
        } else {
            pstmt.setObject(i, value);
        }
    }

    /**
     * Parse of value of resultset with the good type
     *
     * @param result
     * @param i
     * @param type
     * @param locale
     * @return The parse value
     * @throws SQLException
     * @throws IOException
     */
    @Override
    public Object parseType(ResultSet result, Integer i, int type, Locale locale)
            throws SQLException, IOException, ParseException {
        if (logger().isTraceEnabled()) {
            logger().trace("{} {} {}", i, type, result.getString(i));
        }

        switch (type) {
            /**
             * The JDBC types CHAR, VARCHAR, and LONGVARCHAR are closely
             * related. CHAR represents a small, fixed-length character string,
             * VARCHAR represents a small, variable-length character string, and
             * LONGVARCHAR represents a large, variable-length character string.
             */
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR: {
                return result.getString(i);
            }
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR: {
                return result.getNString(i);
            }
            /**
             * The JDBC types BINARY, VARBINARY, and LONGVARBINARY are closely
             * related. BINARY represents a small, fixed-length binary value,
             * VARBINARY represents a small, variable-length binary value, and
             * LONGVARBINARY represents a large, variable-length binary value
             */
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                return result.getBytes(i);
            }
            /**
             * The JDBC type ARRAY represents the SQL3 type ARRAY.
             *
             * An ARRAY value is mapped to an instance of the Array interface in
             * the Java programming language. If a driver follows the standard
             * implementation, an Array object logically points to an ARRAY
             * value on the server rather than containing the elements of the
             * ARRAY object, which can greatly increase efficiency. The Array
             * interface contains methods for materializing the elements of the
             * ARRAY object on the client in the form of either an array or a
             * ResultSet object.
             */
            case Types.ARRAY: {
                Array a = result.getArray(i);
                return a != null ? a.toString() : null;
            }
            /**
             * The JDBC type BIGINT represents a 64-bit signed integer value
             * between -9223372036854775808 and 9223372036854775807.
             *
             * The corresponding SQL type BIGINT is a nonstandard extension to
             * SQL. In practice the SQL BIGINT type is not yet currently
             * implemented by any of the major databases, and we recommend that
             * its use be avoided in code that is intended to be portable.
             *
             * The recommended Java mapping for the BIGINT type is as a Java
             * long.
             */
            case Types.BIGINT: {
                Object o = result.getLong(i);
                return result.wasNull() ? null : o;
            }
            /**
             * The JDBC type BIT represents a single bit value that can be zero
             * or one.
             *
             * SQL-92 defines an SQL BIT type. However, unlike the JDBC BIT
             * type, this SQL-92 BIT type can be used as a parameterized type to
             * define a fixed-length binary string. Fortunately, SQL-92 also
             * permits the use of the simple non-parameterized BIT type to
             * represent a single binary digit, and this usage corresponds to
             * the JDBC BIT type. Unfortunately, the SQL-92 BIT type is only
             * required in "full" SQL-92 and is currently supported by only a
             * subset of the major databases. Portable code may therefore prefer
             * to use the JDBC SMALLINT type, which is widely supported.
             */
            case Types.BIT: {
                try {
                    Object o = result.getInt(i);
                    return result.wasNull() ? null : o;
                } catch (Exception e) {
                    // PSQLException: Bad value for type int : t
                    if (e.getMessage().startsWith("Bad value for type int")) {
                        return "t".equals(result.getString(i));
                    }
                    throw new IOException(e);
                }
            }
            /**
             * The JDBC type BOOLEAN, which is new in the JDBC 3.0 API, maps to
             * a boolean in the Java programming language. It provides a
             * representation of true and false, and therefore is a better match
             * than the JDBC type BIT, which is either 1 or 0.
             */
            case Types.BOOLEAN: {
                return result.getBoolean(i);
            }
            /**
             * The JDBC type BLOB represents an SQL3 BLOB (Binary Large Object).
             *
             * A JDBC BLOB value is mapped to an instance of the Blob interface
             * in the Java programming language. If a driver follows the
             * standard implementation, a Blob object logically points to the
             * BLOB value on the server rather than containing its binary data,
             * greatly improving efficiency. The Blob interface provides methods
             * for materializing the BLOB data on the client when that is
             * desired.
             */
            case Types.BLOB: {
                Blob blob = result.getBlob(i);
                if (blob != null) {
                    long n = blob.length();
                    if (n > Integer.MAX_VALUE) {
                        throw new IOException("can't process blob larger than Integer.MAX_VALUE");
                    }
                    byte[] tab = blob.getBytes(1, (int) n);
                    blob.free();
                    return tab;
                }
                break;
            }
            /**
             * The JDBC type CLOB represents the SQL3 type CLOB (Character Large
             * Object).
             *
             * A JDBC CLOB value is mapped to an instance of the Clob interface
             * in the Java programming language. If a driver follows the
             * standard implementation, a Clob object logically points to the
             * CLOB value on the server rather than containing its character
             * data, greatly improving efficiency. Two of the methods on the
             * Clob interface materialize the data of a CLOB object on the
             * client.
             */
            case Types.CLOB: {
                Clob clob = result.getClob(i);
                if (clob != null) {
                    long n = clob.length();
                    if (n > Integer.MAX_VALUE) {
                        throw new IOException("can't process clob larger than Integer.MAX_VALUE");
                    }
                    String str = clob.getSubString(1, (int) n);
                    clob.free();
                    return str;
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
                    String str = nclob.getSubString(1, (int) n);
                    nclob.free();
                    return str;
                }
                break;
            }
            /**
             * The JDBC type DATALINK, new in the JDBC 3.0 API, is a column
             * value that references a file that is outside of a data source but
             * is managed by the data source. It maps to the Java type
             * java.net.URL and provides a way to manage external files. For
             * instance, if the data source is a DBMS, the concurrency controls
             * it enforces on its own data can be applied to the external file
             * as well.
             *
             * A DATALINK value is retrieved from a ResultSet object with the
             * ResultSet methods getURL or getObject. If the Java platform does
             * not support the type of URL returned by getURL or getObject, a
             * DATALINK value can be retrieved as a String object with the
             * method getString.
             *
             * java.net.URL values are stored in a database using the method
             * setURL. If the Java platform does not support the type of URL
             * being set, the method setString can be used instead.
             *
             *
             */
            case Types.DATALINK: {
                return result.getURL(i);
            }
            /**
             * The JDBC DATE type represents a date consisting of day, month,
             * and year. The corresponding SQL DATE type is defined in SQL-92,
             * but it is implemented by only a subset of the major databases.
             * Some databases offer alternative SQL types that support similar
             * semantics.
             */
            case Types.DATE: {
                try {
                    Date d = result.getDate(i);
                    return d != null ? formatDateISO(d.getTime()) : null;
                } catch (SQLException e) {
                    return null;
                }
            }
            case Types.TIME: {
                try {
                    Time t = result.getTime(i);
                    return t != null ? formatDateISO(t.getTime()) : null;
                } catch (SQLException e) {
                    return null;
                }
            }
            case Types.TIMESTAMP: {
                try {
                    Timestamp t = result.getTimestamp(i);
                    return t != null ? formatDateISO(t.getTime()) : null;
                } catch (SQLException e) {
                    // java.sql.SQLException: Cannot convert value '0000-00-00 00:00:00' from column ... to TIMESTAMP.
                    return null;
                }
            }
            /**
             * The JDBC types DECIMAL and NUMERIC are very similar. They both
             * represent fixed-precision decimal values.
             *
             * The corresponding SQL types DECIMAL and NUMERIC are defined in
             * SQL-92 and are very widely implemented. These SQL types take
             * precision and scale parameters. The precision is the total number
             * of decimal digits supported, and the scale is the number of
             * decimal digits after the decimal point. For most DBMSs, the scale
             * is less than or equal to the precision. So for example, the value
             * "12.345" has a precision of 5 and a scale of 3, and the value
             * ".11" has a precision of 2 and a scale of 2. JDBC requires that
             * all DECIMAL and NUMERIC types support both a precision and a
             * scale of at least 15.
             *
             * The sole distinction between DECIMAL and NUMERIC is that the
             * SQL-92 specification requires that NUMERIC types be represented
             * with exactly the specified precision, whereas for DECIMAL types,
             * it allows an implementation to add additional precision beyond
             * that specified when the type was created. Thus a column created
             * with type NUMERIC(12,4) will always be represented with exactly
             * 12 digits, whereas a column created with type DECIMAL(12,4) might
             * be represented by some larger number of digits.
             *
             * The recommended Java mapping for the DECIMAL and NUMERIC types is
             * java.math.BigDecimal. The java.math.BigDecimal type provides math
             * operations to allow BigDecimal types to be added, subtracted,
             * multiplied, and divided with other BigDecimal types, with integer
             * types, and with floating point types.
             *
             * The method recommended for retrieving DECIMAL and NUMERIC values
             * is ResultSet.getBigDecimal. JDBC also allows access to these SQL
             * types as simple Strings or arrays of char. Thus, Java programmers
             * can use getString to receive a DECIMAL or NUMERIC result.
             * However, this makes the common case where DECIMAL or NUMERIC are
             * used for currency values rather awkward, since it means that
             * application writers have to perform math on strings. It is also
             * possible to retrieve these SQL types as any of the Java numeric
             * types.
             */
            case Types.DECIMAL:
            case Types.NUMERIC: {
                BigDecimal bd = null;
                try {
                    bd = result.getBigDecimal(i);
                } catch (NullPointerException e) {
                    // getBigDecimal() should get obsolete. Most seem to use getString/getObject anyway...
                    // But is it true? JDBC NPE exists since 13 years? 
                    // http://forums.codeguru.com/archive/index.php/t-32443.html
                    // Null values are driving us nuts in JDBC:
                    // http://stackoverflow.com/questions/2777214/when-accessing-resultsets-in-jdbc-is-there-an-elegant-way-to-distinguish-betwee
                }
                if (bd == null || result.wasNull()) {
                    return null;
                }
                if (scale >= 0) {
                    bd = bd.setScale(scale, rounding);
                    try {
                        long l = bd.longValueExact();
                        // TODO argh
                        if (Long.toString(l).equals(result.getString(i))) {
                            return l;
                        } else {
                            return bd.doubleValue();
                        }
                    } catch (ArithmeticException e) {
                        return bd.doubleValue();
                    }
                } else {
                    return bd.toPlainString();
                }
            }
            /**
             * The JDBC type DOUBLE represents a "double precision" floating
             * point number that supports 15 digits of mantissa.
             *
             * The corresponding SQL type is DOUBLE PRECISION, which is defined
             * in SQL-92 and is widely supported by the major databases. The
             * SQL-92 standard leaves the precision of DOUBLE PRECISION up to
             * the implementation, but in practice all the major databases
             * supporting DOUBLE PRECISION support a mantissa precision of at
             * least 15 digits.
             *
             * The recommended Java mapping for the DOUBLE type is as a Java
             * double.
             */
            case Types.DOUBLE: {
                String s = result.getString(i);
                if (result.wasNull() || s == null) {
                    return null;
                }
                NumberFormat format = NumberFormat.getInstance(locale);
                Number number = format.parse(s);
                return number.doubleValue();
            }
            /**
             * The JDBC type FLOAT is basically equivalent to the JDBC type
             * DOUBLE. We provided both FLOAT and DOUBLE in a possibly misguided
             * attempt at consistency with previous database APIs. FLOAT
             * represents a "double precision" floating point number that
             * supports 15 digits of mantissa.
             *
             * The corresponding SQL type FLOAT is defined in SQL-92. The SQL-92
             * standard leaves the precision of FLOAT up to the implementation,
             * but in practice all the major databases supporting FLOAT support
             * a mantissa precision of at least 15 digits.
             *
             * The recommended Java mapping for the FLOAT type is as a Java
             * double. However, because of the potential confusion between the
             * double precision SQL FLOAT and the single precision Java float,
             * we recommend that JDBC programmers should normally use the JDBC
             * DOUBLE type in preference to FLOAT.
             */
            case Types.FLOAT: {
                String s = result.getString(i);
                if (result.wasNull() || s == null) {
                    return null;
                }
                NumberFormat format = NumberFormat.getInstance(locale);
                Number number = format.parse(s);
                return number.doubleValue();
            }
            /**
             * The JDBC type JAVA_OBJECT, added in the JDBC 2.0 core API, makes
             * it easier to use objects in the Java programming language as
             * values in a database. JAVA_OBJECT is simply a type code for an
             * instance of a class defined in the Java programming language that
             * is stored as a database object. The type JAVA_OBJECT is used by a
             * database whose type system has been extended so that it can store
             * Java objects directly. The JAVA_OBJECT value may be stored as a
             * serialized Java object, or it may be stored in some
             * vendor-specific format.
             *
             * The type JAVA_OBJECT is one of the possible values for the column
             * DATA_TYPE in the ResultSet objects returned by various
             * DatabaseMetaData methods, including getTypeInfo, getColumns, and
             * getUDTs. The method getUDTs, part of the new JDBC 2.0 core API,
             * will return information about the Java objects contained in a
             * particular schema when it is given the appropriate parameters.
             * Having this information available facilitates using a Java class
             * as a database type.
             */
            case Types.OTHER:
            case Types.JAVA_OBJECT: {
                return result.getObject(i);
            }
            /**
             * The JDBC type REAL represents a "single precision" floating point
             * number that supports seven digits of mantissa.
             *
             * The corresponding SQL type REAL is defined in SQL-92 and is
             * widely, though not universally, supported by the major databases.
             * The SQL-92 standard leaves the precision of REAL up to the
             * implementation, but in practice all the major databases
             * supporting REAL support a mantissa precision of at least seven
             * digits.
             *
             * The recommended Java mapping for the REAL type is as a Java
             * float.
             */
            case Types.REAL: {
                String s = result.getString(i);
                if (result.wasNull() || s == null) {
                    return null;
                }
                NumberFormat format = NumberFormat.getInstance(locale);
                Number number = format.parse(s);
                return number.doubleValue();
            }
            /**
             * The JDBC type TINYINT represents an 8-bit integer value between 0
             * and 255 that may be signed or unsigned.
             *
             * The corresponding SQL type, TINYINT, is currently supported by
             * only a subset of the major databases. Portable code may therefore
             * prefer to use the JDBC SMALLINT type, which is widely supported.
             *
             * The recommended Java mapping for the JDBC TINYINT type is as
             * either a Java byte or a Java short. The 8-bit Java byte type
             * represents a signed value from -128 to 127, so it may not always
             * be appropriate for larger TINYINT values, whereas the 16-bit Java
             * short will always be able to hold all TINYINT values.
             */
            /**
             * The JDBC type SMALLINT represents a 16-bit signed integer value
             * between -32768 and 32767.
             *
             * The corresponding SQL type, SMALLINT, is defined in SQL-92 and is
             * supported by all the major databases. The SQL-92 standard leaves
             * the precision of SMALLINT up to the implementation, but in
             * practice, all the major databases support at least 16 bits.
             *
             * The recommended Java mapping for the JDBC SMALLINT type is as a
             * Java short.
             */
            /**
             * The JDBC type INTEGER represents a 32-bit signed integer value
             * ranging between -2147483648 and 2147483647.
             *
             * The corresponding SQL type, INTEGER, is defined in SQL-92 and is
             * widely supported by all the major databases. The SQL-92 standard
             * leaves the precision of INTEGER up to the implementation, but in
             * practice all the major databases support at least 32 bits.
             *
             * The recommended Java mapping for the INTEGER type is as a Java
             * int.
             */
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER: {
                try {
                    Integer integer = result.getInt(i);
                    return result.wasNull() ? null : integer;
                } catch (SQLDataException e) {
                    Long l = result.getLong(i);
                    return result.wasNull() ? null : l;
                }
            }

            case Types.SQLXML: {
                SQLXML xml = result.getSQLXML(i);
                return xml != null ? xml.getString() : null;
            }

            case Types.NULL: {
                return null;
            }
            /**
             * The JDBC type DISTINCT field (Types class)>DISTINCT represents
             * the SQL3 type DISTINCT.
             *
             * The standard mapping for a DISTINCT type is to the Java type to
             * which the base type of a DISTINCT object would be mapped. For
             * example, a DISTINCT type based on a CHAR would be mapped to a
             * String object, and a DISTINCT type based on an SQL INTEGER would
             * be mapped to an int.
             *
             * The DISTINCT type may optionally have a custom mapping to a class
             * in the Java programming language. A custom mapping consists of a
             * class that implements the interface SQLData and an entry in a
             * java.util.Map object.
             */
            case Types.DISTINCT: {
                logger().warn("JDBC type not implemented: {}", type);
                return null;
            }
            /**
             * The JDBC type STRUCT represents the SQL99 structured type. An SQL
             * structured type, which is defined by a user with a CREATE TYPE
             * statement, consists of one or more attributes. These attributes
             * may be any SQL data type, built-in or user-defined.
             *
             * The standard mapping for the SQL type STRUCT is to a Struct
             * object in the Java programming language. A Struct object contains
             * a value for each attribute of the STRUCT value it represents.
             *
             * A STRUCT value may optionally be custom mapped to a class in the
             * Java programming language, and each attribute in the STRUCT may
             * be mapped to a field in the class. A custom mapping consists of a
             * class that implements the interface SQLData and an entry in a
             * java.util.Map object.
             *
             *
             */
            case Types.STRUCT: {
                logger().warn("JDBC type not implemented: {}", type);
                return null;
            }
            case Types.REF: {
                logger().warn("JDBC type not implemented: {}", type);
                return null;
            }
            case Types.ROWID: {
                logger().warn("JDBC type not implemented: {}", type);
                return null;
            }
            default: {
                logger().warn("unknown JDBC type ignored: {}", type);
                return null;
            }
        }
        return null;
    }
}