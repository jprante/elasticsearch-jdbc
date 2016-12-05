package org.xbib.importer.jdbc;

import org.xbib.content.settings.Settings;
import org.xbib.content.util.unit.TimeValue;
import org.xbib.importer.ImporterListener;
import org.xbib.importer.Sink;
import org.xbib.importer.Source;
import org.xbib.importer.TabularDataStream;
import org.xbib.importer.elasticsearch.ElasticsearchDocumentBuilder;
import org.xbib.importer.util.LocaleUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class JDBCSource implements Source {
    
    private static final Logger logger = Logger.getLogger(JDBCSource.class.getName());

    private final Settings settings;

    private final ElasticsearchDocumentBuilder builder;

    private final JDBCState state;

    private String url;

    private String user;

    private String password;

    private Connection readConnection;

    private Connection writeConnection;

    private boolean autocommit;

    private int fetchSize;

    private int maxRows;

    private int maxRetries;

    private TimeValue maxretrywait;

    private boolean shouldPrepareResultSetMetadata;

    private boolean shouldPrepareDatabaseMetadata;

    private boolean enableTimestamp;

    private Map<String, Object> columnNameMap;

    private List<SQLCommand> sqlCommands;

    //private boolean isTimestampDiffSupported;

    private int queryTimeout;

    private Map<String, Object> connectionProperties;

    private int resultSetType;

    private int resultSetConcurrency;

    private SQLBinder binder;

    private SQLTypeParser typeParser;

    public JDBCSource(String url, String user, String password, Sink sink) {
        this(settings(), request(url, user, password), sink, state(url, user, password));
    }

    @SuppressWarnings("unchecked")
    public JDBCSource(Settings settings, Settings request, Sink sink, JDBCState state) {
        this.settings = settings;
        logger.log(Level.INFO, "settings=" + settings.getAsMap() + " request=" + request.getAsMap());
        this.state = state;
        this.url = request.get("url");
        this.user = request.get("user");
        this.password = request.get("password");
        String s = request.get("resultset_type");
        this.resultSetType = "TYPE_FORWARD_ONLY".equals(s) ?
                ResultSet.TYPE_FORWARD_ONLY : "TYPE_SCROLL_SENSITIVE".equals(s) ?
                ResultSet.TYPE_SCROLL_SENSITIVE : "TYPE_SCROLL_INSENSITIVE".equals(s) ?
                ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_FORWARD_ONLY;
        s = request.get("resultset_concurrency");
        this.resultSetConcurrency = "CONCUR_READ_ONLY".equals(s) ?
                ResultSet.CONCUR_READ_ONLY : ResultSet.CONCUR_UPDATABLE;
        this.maxRetries = request.getAsInt("maxretries", 1);
        this.maxretrywait = TimeValue.timeValueSeconds(30);
        this.columnNameMap = new HashMap<>();
        this.connectionProperties = new HashMap<>();
        this.typeParser = new SQLTypeParser(LocaleUtil.toLocale(request.get("locale")),
                TimeZone.getTimeZone(request.get("timezone", "GMT")),
                request.getAsBoolean("treat_binary_as_string", true),
                request.getAsInt("scaling", 0),
                getRounding(request.get("rounding"))
                );
        this.binder = new SQLBinder(request, typeParser.getCalendar(), state);
        this.sqlCommands = SQLCommand.parse(request.getAsStructuredMap());
        this.autocommit = request.getAsBoolean("autocommit",true);
        this.fetchSize = 10;
        String fetchSizeStr = request.get("fetchsize");
        if ("min".equals(fetchSizeStr)) {
            this.fetchSize = Integer.MIN_VALUE; // for MySQL streaming mode
        } else if (fetchSizeStr != null) {
            try {
                this.fetchSize = Integer.parseInt(fetchSizeStr);
            } catch (Exception e) {
                // ignore unparseable
            }
        } else {
            // if MySQL, enable streaming mode hack by default
            String url = request.get("url");
            if (url != null && url.startsWith("jdbc:mysql")) {
                this.fetchSize = Integer.MIN_VALUE; // for MySQL streaming mode
            }
        }
        this.maxRows = request.getAsInt("max_rows", 0);
        this.maxretrywait = request.getAsTime("max_retries_wait", TimeValue.timeValueSeconds(30));
        this.shouldPrepareDatabaseMetadata = request.getAsBoolean("prepare_database_metadata", false);
        this.shouldPrepareResultSetMetadata = request.getAsBoolean("prepare_resultset_metadata", false);
        this.enableTimestamp = request.getAsBoolean("timestamp", false);
        this.columnNameMap = (Map<String, Object>) request.getAsStructuredMap().get("column_name_map");
        this.queryTimeout = request.getAsInt("query_timeout", 1800);
        this.connectionProperties = (Map<String, Object>) request.getAsStructuredMap().get("connection_properties");
        boolean shouldIgnoreNull = request.getAsBoolean("ignore_null_values", true);
        //boolean shouldDetectGeo = request.getAsBoolean("detect_geo", true);
        //boolean shouldDetectJson = request.getAsBoolean("detect_json", true);
        this.builder = new ElasticsearchDocumentBuilder(sink)
                .shouldIgnoreNull(shouldIgnoreNull);
                //.shouldDetectGeo(shouldDetectGeo)
                //.shouldDetectJson(shouldDetectJson);
    }

    private int getRounding(String rounding) {
        if ("ceiling".equalsIgnoreCase(rounding)) {
            return BigDecimal.ROUND_CEILING;
        } else if ("down".equalsIgnoreCase(rounding)) {
            return BigDecimal.ROUND_DOWN;
        } else if ("floor".equalsIgnoreCase(rounding)) {
            return BigDecimal.ROUND_FLOOR;
        } else if ("halfdown".equalsIgnoreCase(rounding)) {
            return BigDecimal.ROUND_HALF_DOWN;
        } else if ("halfeven".equalsIgnoreCase(rounding)) {
            return BigDecimal.ROUND_HALF_EVEN;
        } else if ("halfup".equalsIgnoreCase(rounding)) {
            return BigDecimal.ROUND_HALF_UP;
        } else if ("unnecessary".equalsIgnoreCase(rounding)) {
            return BigDecimal.ROUND_UNNECESSARY;
        } else if ("up".equalsIgnoreCase(rounding)) {
            return BigDecimal.ROUND_UP;
        }
        return 0;
    }

    /**
     * Get JDBC connection for reading.
     *
     * @return the connection
     * @throws SQLException when SQL execution gives an error
     */
    public Connection openConnectionForReading() throws SQLException {
        boolean invalid = readConnection == null || readConnection.isClosed();
        try {
            invalid = invalid || !readConnection.isValid(5);
        } catch (AbstractMethodError | SQLFeatureNotSupportedException e) {
            // old/buggy JDBC driver
            logger.log(Level.FINE, e.getMessage(), e);
        }
        if (invalid) {
            int retries = this.maxRetries;
            while (retries > 0) {
                retries--;
                try {
                    if (user != null) {
                        Properties properties = new Properties();
                        properties.put("user", user);
                        if (password != null) {
                            properties.put("password", password);
                        }
                        if (connectionProperties != null) {
                            properties.putAll(connectionProperties);
                        }
                        readConnection = DriverManager.getConnection(url, properties);
                    } else {
                        readConnection = DriverManager.getConnection(url);
                    }
                    DatabaseMetaData metaData = readConnection.getMetaData();
                    if (shouldPrepareDatabaseMetadata) {
                        prepare(metaData);
                    }
                    //if (metaData.getTimeDateFunctions().contains("TIMESTAMPDIFF")) {
                    //    this.isTimestampDiffSupported = true;
                    //}
                    // "readonly" is required by MySQL for large result streaming
                    readConnection.setReadOnly(true);
                    // Postgresql cursor mode condition:
                    // fetchsize > 0, no scrollable result set, no auto commit, no holdable cursors over commit
                    // https://github.com/pgjdbc/pgjdbc/blob/master/org/postgresql/jdbc2/AbstractJdbc2Statement.java#L514
                    //readConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    // many drivers don't like autocommit=true
                    readConnection.setAutoCommit(autocommit);
                    return readConnection;
                } catch (SQLException e) {
                    logger.log(Level.SEVERE, "while opening read connection: " + url + " " + e.getMessage(), e);
                    try {
                        logger.log(Level.FINE, "delaying for {} seconds...", maxretrywait.seconds());
                        Thread.sleep(maxretrywait.millis());
                    } catch (InterruptedException ex) {
                        // do nothing
                    }
                }
            }
        }
        return readConnection;
    }

    /**
     * Get JDBC connection for writing. FOr executing "update", "insert", callable statements.
     *
     * @return the connection
     * @throws SQLException when SQL execution gives an error
     */
    public Connection openConnectionForWriting() throws SQLException {
        boolean invalid = writeConnection == null || writeConnection.isClosed();
        try {
            invalid = invalid || !writeConnection.isValid(5);
        } catch (AbstractMethodError | SQLFeatureNotSupportedException e) {
            // old/buggy JDBC driver do not implement isValid()
            // Example: postgresql does implement but not support isValid()
            logger.log(Level.FINE, e.getMessage(), e);
        }
        if (invalid) {
            int retries = this.maxRetries;
            while (retries > 0) {
                retries--;
                try {
                    if (user != null) {
                        Properties properties = new Properties();
                        properties.put("user", user);
                        if (password != null) {
                            properties.put("password", password);
                        }
                        if (connectionProperties != null) {
                            properties.putAll(connectionProperties);
                        }
                        writeConnection = DriverManager.getConnection(url, properties);
                    } else {
                        writeConnection = DriverManager.getConnection(url);
                    }
                    // many drivers don't like autocommit=true
                    writeConnection.setAutoCommit(autocommit);
                    return writeConnection;
                } catch (SQLNonTransientConnectionException e) {
                    // ignore derby drop=true silently
                } catch (SQLException e) {
                    //context.setThrowable(e);
                    logger.log(Level.SEVERE,"while opening write connection: " + url + " " + e.getMessage(), e);
                    try {
                        Thread.sleep(maxretrywait.millis());
                    } catch (InterruptedException ex) {
                        // do nothing
                    }
                }
            }
        }
        return writeConnection;
    }

    /**
     *  Execute SQL statements.
     *
     * @throws IOException  when input/setOutput error occurs
     */
    public void execute(ImporterListener importerListener) throws IOException {
        logger.log(Level.FINE,"executing SQL commands " + sqlCommands);
        try {
            for (SQLCommand command : sqlCommands) {
                try {
                    if (command.isCallable()) {
                        logger.log(Level.FINE, "executing callable SQL: " + command);
                        executeCallable(command);
                    } else if (!command.getParameters().isEmpty()) {
                        logger.log(Level.FINE, "executing SQL with params: " + command);
                        executeWithParameter(command);
                    } else {
                        logger.log(Level.FINE, "executing SQL without params: " + command);
                        execute(command);
                    }
                    Long succeeded = state.getVariable("${metrics.succeeded}");
                    if (succeeded == null) {
                        succeeded = 0L;
                    }
                    state.setVariable("${metrics.succeeded}", succeeded + 1);
                } catch (SQLRecoverableException e) {
                    long millis = maxretrywait.getMillis();
                    logger.log(Level.WARNING, "recoverable exception, retrying after " + millis / 1000 + " seconds", e);
                    try {
                        Thread.sleep(maxretrywait.getMillis());
                    } catch (InterruptedException ie) {
                        logger.log(Level.SEVERE, e.getMessage(), e);
                    }
                    try {
                        if (command.isCallable()) {
                            logger.log(Level.FINE, "retrying, executing callable SQL: " + command);
                            executeCallable(command);
                        } else if (!command.getParameters().isEmpty()) {
                            logger.log(Level.FINE, "retrying, executing SQL with params: " + command);
                            executeWithParameter(command);
                        } else {
                            logger.log(Level.FINE, "retrying, executing SQL without params: {}", command);
                            execute(command);
                        }
                        Long succeeded = state.getVariable("${metrics.succeeded}");
                        if (succeeded == null) {
                            succeeded = 0L;
                        }
                        state.setVariable("${metrics.succeeded}", succeeded + 1);
                    } catch (SQLException e1) {
                        throw new IOException(e1);
                    }
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
        } catch (IOException e) {
            Long failed = state.getVariable("${metrics.failed}");
            if (failed == null) {
                failed = 0L;
            }
            state.setVariable("${metrics.failed}", failed + 1);
            throw new IOException(e);
        }
    }

    /**
     * Execute SQL query command without parameter binding.
     *
     * @param command the SQL command
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/setOutput error occurs
     */
    private void execute(SQLCommand command) throws SQLException, IOException {
        Statement statement = null;
        ResultSet results = null;
        try {
            if (command.isQuery()) {
                // use read connection
                // we must not use prepareStatement for Postgresql!
                // Postgresql requires direct use of executeQuery(sqlCommands) for cursor with fetchsize set.
                Connection connection = openConnectionForReading();
                if (connection != null) {
                    logger.log(Level.FINE, "using read connection " + connection + " for executing query");
                    statement = connection.createStatement();
                    try {
                        statement.setQueryTimeout(queryTimeout);
                    } catch (SQLFeatureNotSupportedException e) {
                        // Postgresql does not support setQueryTimeout()
                        logger.log(Level.WARNING, "driver does not support setQueryTimeout(), skipped");
                    }
                    results = executeQuery(statement, command.getSQL());
                    if (shouldPrepareResultSetMetadata) {
                        prepare(results.getMetaData());
                    }
                    merge(command, results, builder);
                    builder.flush();
                }
            } else {
                // use write connection
                Connection connection = openConnectionForWriting();
                if (connection != null) {
                    logger.log(Level.FINE, "using write connection " + connection + " for executing insert/update");
                    statement = connection.createStatement();
                    executeUpdate(statement, command.getSQL());
                }
            }
        } finally {
            close(results);
            close(statement);
        }
    }

    /**
     * Execute SQL query command with parameter binding.
     *
     * @param command the SQL command
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/setOutput error occurs
     */
    private void executeWithParameter(SQLCommand command) throws SQLException, IOException {
        PreparedStatement statement = null;
        ResultSet results = null;
        try {
            if (command.isQuery()) {
                statement = prepareQuery(command.getSQL());
                bind(statement, command.getParameters());
                logger.log(Level.FINE, "execute sqlCommands is " + statement.toString());
                results = executeQuery(statement);
                merge(command, results, builder);
                builder.flush();
            } else {
                statement = prepareUpdate(command.getSQL());
                bind(statement, command.getParameters());
                executeUpdate(statement);
            }
        } finally {
            close(results);
            close(statement);
        }
    }

    /**
     * Execute callable SQL command.
     *
     * @param command the SQL command
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/setOutput error occurs
     */
    private void executeCallable(SQLCommand command) throws SQLException, IOException {
        // call stored procedure
        CallableStatement statement = null;
        try {
            // we do not make a difference betwwen read/write and we assume
            // it is safe to use the read connection and query the DB
            Connection connection = openConnectionForWriting();
            logger.log(Level.FINE, "using write connection " + connection + " for executing callable statement");
            if (connection != null) {
                statement = connection.prepareCall(command.getSQL());
                if (!command.getParameters().isEmpty()) {
                    bind(statement, command.getParameters());
                }
                if (!command.getRegister().isEmpty()) {
                    register(statement, command.getRegister());
                }
                boolean hasRows = statement.execute();
                if (hasRows) {
                    logger.log(Level.FINE, "callable execution created result set");
                    while (hasRows) {
                        // merge result set, but use register
                        merge(command, statement.getResultSet(), builder);
                        hasRows = statement.getMoreResults();
                    }
                } else {
                    // no result set, merge from registered params only
                    merge(command, statement, builder);
                }
                builder.flush();
            }
        } finally {
            close(statement);
        }
    }

    /**
     * Merge key/values from JDBC result set.
     *
     * @param command  the SQL command that created this result set
     * @param results  result set
     * @param listener the value listener
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/setOutput error occurs
     */
    private void merge(SQLCommand command, ResultSet results, TabularDataStream<String, Object> listener)
            throws SQLException, IOException {
        if (listener == null) {
            return;
        }
        beforeRows(command, results, listener);
        long rowcount = 0L;
        while (nextRow(results, listener)) {
            rowcount++;
        }
        state.setVariable("${lastrowcount}", rowcount);
        if (rowcount > 0) {
            logger.log(Level.FINE, "merged rows: " + rowcount);
        } else {
            logger.log(Level.FINE, "no rows merged");
        }
        Long totalrows = state.getVariable("${metrics.totalrows}");
        if (totalrows == null) {
            totalrows = 0L;
        }
        state.setVariable("${metrics.totalrows}", totalrows + rowcount);
        listener.end();
    }

    /**
     * Prepare a query statement
     *
     * @param sql the SQL statement
     * @return a prepared statement
     * @throws SQLException when SQL execution gives an error
     */
    public PreparedStatement prepareQuery(String sql) throws SQLException {
        Connection connection = openConnectionForReading();
        if (connection == null) {
            throw new SQLException("can't connect to source " + url);
        }
        logger.log(Level.FINE, "preparing statement with SQL " + sql);
        return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    /**
     * Prepare an update statement.
     *
     * @param sql the SQL statement
     * @return a prepared statement
     * @throws SQLException when SQL execution gives an error
     */
    private PreparedStatement prepareUpdate(String sql) throws SQLException {
        Connection connection = openConnectionForWriting();
        if (connection == null) {
            throw new SQLException("can't connect to source " + url);
        }
        logger.log(Level.FINE, "preparing statement with SQL " + sql);
        return connection.prepareStatement(sql);
    }

    /**
     * Bind values to prepared statement.
     *
     * @param statement the prepared statement
     * @param values    the values to bind
     *
     * @throws SQLException when SQL execution gives an error
     */
    public void bind(PreparedStatement statement, List<Object> values) throws SQLException {
        if (values == null) {
            logger.log(Level.WARNING, "no values given for bind");
            return;
        }
        for (int i = 1; i <= values.size(); i++) {
            binder.bind(statement, i, values.get(i - 1));
        }
    }

    /**
     * Merge key/values from registered params of a callable statement
     *
     * @param statement callable statement
     * @param listener  the value listener
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/setOutput error occurs
     */
    @SuppressWarnings({"unchecked"})
    private void merge(SQLCommand command, CallableStatement statement, TabularDataStream listener)
            throws SQLException, IOException {
        Map<String, Object> map = command.getRegister();
        if (map.isEmpty()) {
            // no register given, return without doing anything
            return;
        }
        List<String> keys = new LinkedList<>();
        List<Object> values = new LinkedList<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String k = entry.getKey();
            Map<String, Object> v = (Map<String, Object>) entry.getValue();
            Integer pos = (Integer) v.get("pos"); // the parameter position of the value
            String field = (String) v.get("field"); // the field for indexing the value (if not key name)
            keys.add(field != null ? field : k);
            values.add(statement.getObject(pos));
        }
        logger.log(Level.FINER, "merge callable statement result: keys=" + keys + " values=" + values);
        listener.keys(keys);
        listener.values(values);
        listener.end();
    }

    /**
     * Register variables in callable statement
     *
     * @param statement callable statement
     * @param values    values
     * @return this source
     * @throws SQLException when SQL execution gives an error
     */
    @SuppressWarnings({"unchecked"})
    private JDBCSource register(CallableStatement statement, Map<String, Object> values)
            throws SQLException {
        if (values == null) {
            return this;
        }
        for (Map.Entry<String, Object> me : values.entrySet()) {
            // { "key" : { "pos": n, "type" : "VARCHAR", "field" : "fieldname" }, ... }
            Map<String, Object> m = (Map<String, Object>) me.getValue();
            Object o = m.get("pos");
            if (o != null) {
                Integer n = o instanceof Integer ? (Integer) o : Integer.parseInt(o.toString());
                o =  m.get("type");
                String type = o instanceof String ? (String) o: o.toString();
                if (type != null) {
                    logger.log(Level.FINE, "registerOutParameter: n=" + n + " type=" + typeParser.toJDBCType(type));
                    try {
                        statement.registerOutParameter(n, typeParser.toJDBCType(type));
                    } catch (Throwable t) {
                        logger.log(Level.WARNING, "can't register out parameter " + n + " of type " + type);
                    }
                }
            }
        }
        return this;
    }

    /**
     * Execute prepared query statement
     *
     * @param statement the prepared statement
     * @return the result set
     * @throws SQLException when SQL execution gives an error
     */
    public ResultSet executeQuery(PreparedStatement statement) throws SQLException {
        statement.setMaxRows(maxRows);
        statement.setFetchSize(fetchSize);
        return statement.executeQuery();
    }

    /**
     * Execute query statement
     *
     * @param statement the statement
     * @param sql       the SQL
     * @return the result set
     * @throws SQLException when SQL execution gives an error
     */
    private ResultSet executeQuery(Statement statement, String sql) throws SQLException {
        statement.setMaxRows(maxRows);
        statement.setFetchSize(fetchSize);
        return statement.executeQuery(sql);
    }

    /**
     * Execute prepared update statement
     *
     * @param statement the prepared statement
     * @throws SQLException when SQL execution gives an error
     */
    private void executeUpdate(PreparedStatement statement) throws SQLException {
        statement.executeUpdate();
        if (!writeConnection.getAutoCommit()) {
            writeConnection.commit();
        }
    }

    /**
     * Execute prepared update statement
     *
     * @param statement the prepared statement
     * @return the result set
     * @throws SQLException when SQL execution gives an error
     */
    private JDBCSource executeUpdate(Statement statement, String sql) throws SQLException {
        statement.executeUpdate(sql);
        if (!writeConnection.getAutoCommit()) {
            writeConnection.commit();
        }
        return this;
    }

    public void beforeRows(ResultSet results, TabularDataStream<String, Object> listener)
            throws SQLException, IOException {
        beforeRows(null, results, listener);
    }

    /**
     * Before rows are read, let the TabularDataStream know about the keys.
     * If the SQL command was a callable statement and a register is there, look into the register map
     * for the key names, not in the result set metadata.
     *
     * @param command  the SQL command that created this result set
     * @param results  the result set
     * @param listener the key/value stream listener
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/setOutput error occurs
     */
    private void beforeRows(SQLCommand command, ResultSet results, TabularDataStream<String, Object> listener)
            throws SQLException, IOException {
        List<String> keys = new LinkedList<>();
        if (command != null && command.isCallable() && !command.getRegister().isEmpty()) {
            for (Map.Entry<String, Object> me : command.getRegister().entrySet()) {
                keys.add(me.getKey());
            }
        } else {
            ResultSetMetaData metadata = results.getMetaData();
            int columns = metadata.getColumnCount();
            for (int i = 1; i <= columns; i++) {
                if (columnNameMap == null) {
                    keys.add(metadata.getColumnLabel(i));
                } else {
                    keys.add(mapColumnName(columnNameMap, metadata.getColumnLabel(i)));
                }
            }
        }
        if (enableTimestamp && !keys.isEmpty()) {
            keys.add("_timestamp");
        }
        listener.begin();
        listener.keys(keys);
    }

    /**
     * Get next row and prepare the values for processing. The labels of each
     * columns are used for the ValueListener as paths for JSON object merging.
     *
     * @param results  the result set
     * @param listener the listener
     * @return true if row exists and was processed, false otherwise
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/setOutput error occurs
     */
    public boolean nextRow(ResultSet results, TabularDataStream<String, Object> listener)
            throws SQLException, IOException {
        if (results.next()) {
            processRow(results, listener);
            return true;
        }
        return false;
    }

    @SuppressWarnings({"unchecked"})
    private void processRow(ResultSet results, TabularDataStream listener)
            throws SQLException, IOException {
        List<Object> values = new LinkedList<>();
        ResultSetMetaData metadata = results.getMetaData();
        int columns = metadata.getColumnCount();
        Map<String, Object> row = new HashMap<>();
        for (int i = 1; i <= columns; i++) {
            try {
                Object value = typeParser.parseType(results, i, metadata.getColumnType(i));
                if (logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINE, "value=" + value + " class=" +
                            (value != null ? value.getClass().getName() : ""));
                }
                values.add(value);
                row.put("$row." + metadata.getColumnLabel(i), value);
                if (value != null) {
                    Long totalsize = state.getVariable("${metrics.totalbytes}");
                    if (totalsize == null) {
                        totalsize = 0L;
                    }
                    state.setVariable("${metrics.totalbytes}", totalsize + value.toString().length());
                }
            } catch (ParseException e) {
                logger.log(Level.WARNING, "parse error for value " + results.getObject(i) + ", using null instead");
                values.add(null);
            }
        }
        if (enableTimestamp && columns > 1) {
            long l = System.currentTimeMillis();
            values.add(l);
            row.put("_timestamp", l);
        }
        state.put(row);
        if (listener != null) {
            listener.values(values);
        }
    }

    /**
     * Close result set
     *
     * @param result the result set to be closed or null
     * @throws SQLException when SQL execution gives an error
     */
    public void close(ResultSet result) throws SQLException {
        if (result != null) {
            result.close();
        }
    }

    /**
     * Close statement
     *
     * @param statement the statement to be closed or null
     * @throws SQLException when SQL execution gives an error
     */
    public void close(Statement statement) throws SQLException {
        if (statement != null) {
            statement.close();
        }
    }

    /**
     * Close read connection.
     */
    public void closeReadConnection() {
        try {
            if (readConnection != null && !readConnection.isClosed()) {
                // always commit before close to finish cursors/transactions
                if (!readConnection.getAutoCommit()) {
                    readConnection.commit();
                }
                readConnection.close();
            }
        } catch (SQLException e) {
            logger.log(Level.WARNING, "while closing read connection: " + e.getMessage());
        }
    }

    /**
     * Close write connection.
     */
    public void closeWriteConnection() {
        try {
            if (writeConnection != null && !writeConnection.isClosed()) {
                // always commit before close to finish cursors/transactions
                if (!writeConnection.getAutoCommit()) {
                    writeConnection.commit();
                }
                writeConnection.close();
            }
        } catch (SQLException e) {
            logger.log(Level.WARNING, "while closing write connection: " + e.getMessage());
        }
    }

    private void prepare(final DatabaseMetaData metaData) throws SQLException {
        Map<String, Object> m = new HashMap<>();
        m.put("$meta.db.allproceduresarecallable", metaData.allProceduresAreCallable());
        m.put("$meta.db.alltablesareselectable", metaData.allTablesAreSelectable());
        m.put("$meta.db.autocommitclosesallresultsets", metaData.autoCommitFailureClosesAllResultSets());
        m.put("$meta.db.datadefinitioncasestransactioncommit", metaData.dataDefinitionCausesTransactionCommit());
        m.put("$meta.db.datadefinitionignoredintransactions", metaData.dataDefinitionIgnoredInTransactions());
        m.put("$meta.db.doesmaxrowsizeincludeblobs", metaData.doesMaxRowSizeIncludeBlobs());
        m.put("$meta.db.catalogseparator", metaData.getCatalogSeparator());
        m.put("$meta.db.catalogterm", metaData.getCatalogTerm());
        m.put("$meta.db.databasemajorversion", metaData.getDatabaseMajorVersion());
        m.put("$meta.db.databaseminorversion", metaData.getDatabaseMinorVersion());
        m.put("$meta.db.databaseproductname", metaData.getDatabaseProductName());
        m.put("$meta.db.databaseproductversion", metaData.getDatabaseProductVersion());
        m.put("$meta.db.defaulttransactionisolation", metaData.getDefaultTransactionIsolation());
        m.put("$meta.db.drivermajorversion", metaData.getDriverMajorVersion());
        m.put("$meta.db.driverminorversion", metaData.getDriverMinorVersion());
        m.put("$meta.db.drivername", metaData.getDriverName());
        m.put("$meta.db.driverversion", metaData.getDriverVersion());
        m.put("$meta.db.extranamecharacters", metaData.getExtraNameCharacters());
        m.put("$meta.db.identifierquotestring", metaData.getIdentifierQuoteString());
        m.put("$meta.db.jdbcmajorversion", metaData.getJDBCMajorVersion());
        m.put("$meta.db.jdbcminorversion", metaData.getJDBCMinorVersion());
        m.put("$meta.db.maxbinaryliterallength", metaData.getMaxBinaryLiteralLength());
        m.put("$meta.db.maxcatalognamelength", metaData.getMaxCatalogNameLength());
        m.put("$meta.db.maxcharliterallength", metaData.getMaxCharLiteralLength());
        m.put("$meta.db.maxcolumnnamelength", metaData.getMaxColumnNameLength());
        m.put("$meta.db.maxcolumnsingroupby", metaData.getMaxColumnsInGroupBy());
        m.put("$meta.db.maxcolumnsinindex", metaData.getMaxColumnsInIndex());
        m.put("$meta.db.maxcolumnsinorderby", metaData.getMaxColumnsInOrderBy());
        m.put("$meta.db.maxcolumnsinselect", metaData.getMaxColumnsInSelect());
        m.put("$meta.db.maxcolumnsintable", metaData.getMaxColumnsInTable());
        m.put("$meta.db.maxconnections", metaData.getMaxConnections());
        m.put("$meta.db.maxcursornamelength", metaData.getMaxCursorNameLength());
        m.put("$meta.db.maxindexlength", metaData.getMaxIndexLength());
        m.put("$meta.db.maxusernamelength", metaData.getMaxUserNameLength());
        m.put("$meta.db.maxprocedurenamelength", metaData.getMaxProcedureNameLength());
        m.put("$meta.db.maxrowsize", metaData.getMaxRowSize());
        m.put("$meta.db.maxschemanamelength", metaData.getMaxSchemaNameLength());
        m.put("$meta.db.maxstatementlength", metaData.getMaxStatementLength());
        m.put("$meta.db.maxstatements", metaData.getMaxStatements());
        m.put("$meta.db.maxtablenamelength", metaData.getMaxTableNameLength());
        m.put("$meta.db.maxtablesinselect", metaData.getMaxTablesInSelect());
        m.put("$meta.db.numericfunctions", metaData.getNumericFunctions());
        m.put("$meta.db.procedureterm", metaData.getProcedureTerm());
        m.put("$meta.db.resultsetholdability", metaData.getResultSetHoldability());
        m.put("$meta.db.rowidlifetime", metaData.getRowIdLifetime().name());
        m.put("$meta.db.schematerm", metaData.getSchemaTerm());
        m.put("$meta.db.searchstringescape", metaData.getSearchStringEscape());
        m.put("$meta.db.sqlkeywords", metaData.getSQLKeywords());
        m.put("$meta.db.sqlstatetype", metaData.getSQLStateType());
        state.put(m);
    }

    private void prepare(final ResultSetMetaData metaData) throws SQLException {
        Map<String, Object> m = new HashMap<>();
        m.put("$meta.row.columnCount", metaData.getColumnCount());
        for (int i = 1; i < metaData.getColumnCount(); i++) {
            m.put("$meta.rs.catalogname." + i, metaData.getCatalogName(i));
            m.put("$meta.rs.columnclassname." + i, metaData.getColumnClassName(i));
            m.put("$meta.rs.columndisplaysize." + i, metaData.getColumnDisplaySize(i));
            m.put("$meta.rs.columnlabel." + i, metaData.getColumnLabel(i));
            m.put("$meta.rs.columnname." + i, metaData.getColumnName(i));
            m.put("$meta.rs.columntype." + i, metaData.getColumnType(i));
            m.put("$meta.rs.columntypename." + i, metaData.getColumnTypeName(i));
            m.put("$meta.rs.precision." + i, metaData.getPrecision(i));
            m.put("$meta.rs.scale." + i, metaData.getScale(i));
            m.put("$meta.rs.schemaname." + i, metaData.getSchemaName(i));
            m.put("$meta.rs.tablename." + i, metaData.getTableName(i));
            m.put("$meta.rs.isautoincrement." + i, metaData.isAutoIncrement(i));
            m.put("$meta.rs.iscasesensitive." + i, metaData.isCaseSensitive(i));
            m.put("$meta.rs.iscurrency." + i, metaData.isCurrency(i));
            m.put("$meta.rs.isdefinitelywritable." + i, metaData.isDefinitelyWritable(i));
            m.put("$meta.rs.isnullable." + i, metaData.isNullable(i));
            m.put("$meta.rs.isreadonly." + i, metaData.isReadOnly(i));
            m.put("$meta.rs.issearchable." + i, metaData.isSearchable(i));
            m.put("$meta.rs.issigned." + i, metaData.isSigned(i));
            m.put("$meta.rs.iswritable." + i, metaData.isWritable(i));
        }
        state.put(m);
    }

    private String mapColumnName(Map<String, Object> columnNameMap, String columnName) {
        StringBuilder sb = new StringBuilder();
        String[] s = columnName.split("\\.");
        for (int i = 0; i < s.length; i++) {
            if (i > 0) {
                sb.append(".");
            }
            if (columnNameMap.containsKey(s[i])) {
                s[i] = columnNameMap.get(s[i]).toString();
            } else {
                logger.log(Level.WARNING, "no column map entry for " + s[i] + " in map " + columnNameMap);
            }
            sb.append(s[i]);
        }
        return sb.toString();
    }

    @Override
    public void close() throws IOException {
        logger.log(Level.FINE, "closing");
        closeReadConnection();
        logger.log(Level.FINE, "read connection closed");
        readConnection = null;
        closeWriteConnection();
        logger.log(Level.FINE, "write connection closed");
        writeConnection = null;
    }

    private static Settings settings() {
        return Settings.settingsBuilder()
                .build();
    }

    private static Settings request(String url, String user, String password) {
        return Settings.settingsBuilder()
                .put("url", url)
                .put("user", user)
                .put("password", password)
                .build();
    }

    private static JDBCState state(String url, String user, String password) {
        return new JDBCState(settings(), request(url, user, password), new HashMap<>());
    }

}
