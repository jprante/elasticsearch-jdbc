package org.xbib.elasticsearch.river.jdbc;

import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.keyvalue.KeyValueStreamListener;

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
 * The river source models the data producing side
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
    RiverSource setRiverContext(RiverContext context);

    /**
     * Fetch a data portion from the database and pass it to the river task
     * for firther processing.
     *
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/output error occurs
     */
    void fetch() throws SQLException, IOException;

    /**
     * Set the driver URL
     *
     * @param url the JDBC URL
     * @return this river source
     */
    RiverSource setUrl(String url);

    /**
     * Set the user authentication
     *
     * @param user the user
     * @return this river source
     */
    RiverSource setUser(String user);

    /**
     * Set the password authentication
     *
     * @param password the password
     * @return this river source
     */
    RiverSource setPassword(String password);


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
     * @return this river source
     * @throws SQLException when SQL execution gives an error
     */
    RiverSource bind(PreparedStatement statement, List<Object> values) throws SQLException;

    /**
     * Register output variables for callable statement
     *
     * @param statement callable statement
     * @param values    values
     * @return this river source
     * @throws SQLException when SQL execution gives an error
     */
    RiverSource register(CallableStatement statement, Map<String, Object> values) throws SQLException;

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
     * @return this river source
     * @throws SQLException when SQL execution gives an error
     */
    RiverSource executeUpdate(PreparedStatement statement) throws SQLException;

    RiverSource executeUpdate(Statement statement, String sql) throws SQLException;

    void beforeRows(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * Action for the next row of the result set to be processed
     *
     * @param results  result
     * @param listener listener
     * @return true if next row exists
     * @throws SQLException when SQL execution gives an error
     * @throws IOException  when input/output error occurs
     */
    boolean nextRow(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    void afterRows(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

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
     * @return this river source
     * @throws SQLException when SQL execution gives an error
     */
    RiverSource close(ResultSet result) throws SQLException;

    /**
     * Close statement
     *
     * @param statement statement
     * @return this river source
     * @throws SQLException when SQL execution gives an error
     */
    RiverSource close(Statement statement) throws SQLException;

    /**
     * Close reading from this river source
     *
     * @return this river source
     */
    RiverSource closeReading();

    /**
     * Close writing to this river source
     *
     * @return this river source
     */
    RiverSource closeWriting();

    /**
     * Set the timezone for JDBC setTimestamp() calls with calendar object.
     *
     * @param timeZone the time zone
     * @return this river source
     */
    RiverSource setTimeZone(TimeZone timeZone);

    /**
     * Get the current timezone of this river source for the JDBC setTimestamp() call
     *
     * @return the time zone
     */
    TimeZone getTimeZone();

}
