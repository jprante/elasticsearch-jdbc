
package org.xbib.elasticsearch.river.jdbc;

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

import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.io.keyvalue.KeyValueStreamListener;

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
    RiverSource riverContext(RiverContext context);

    /**
     * Fetch a data portion from the database and pass it to the river task
     * for firther processing.
     *
     * @throws SQLException
     * @throws IOException
     */
    void fetch() throws SQLException, IOException;

    /**
     * Set the driver URL
     *
     * @param url the JDBC URL
     * @return this river source
     */
    RiverSource url(String url);

    /**
     * Set the user authentication
     *
     * @param user the user
     * @return this river source
     */
    RiverSource user(String user);

    /**
     * Set the password authentication
     *
     * @param password the password
     * @return this river source
     */
    RiverSource password(String password);


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
     * @param sql SQL statement
     * @return a prepared statement
     * @throws SQLException
     */
    PreparedStatement prepareQuery(String sql) throws SQLException;

    /**
     * Prepare insert/update statement
     *
     * @param sql SQL statement
     * @return a prepared statement
     * @throws SQLException
     */
    PreparedStatement prepareUpdate(String sql) throws SQLException;

    /**
     * Bind query variables
     *
     * @param statement prepared statement
     * @param values values
     * @return this river source
     * @throws SQLException
     */
    RiverSource bind(PreparedStatement statement, List<? extends Object> values) throws SQLException;

    /**
     * Register output variables for callable statement
     *
     * @param statement callable statement
     * @param values values
     * @return this river source
     * @throws SQLException
     */
    RiverSource register(CallableStatement statement, Map<String, Object> values) throws SQLException;

    /**
     * Execute query
     *
     * @param statement prepared statement
     * @return the result set
     * @throws SQLException
     */
    ResultSet executeQuery(PreparedStatement statement) throws SQLException;

    /**
     * Execute query without binding parameters
     *
     * @param statement the SQL statement
     * @param sql       the SQL query
     * @return the result set
     * @throws SQLException
     */
    ResultSet executeQuery(Statement statement, String sql) throws SQLException;

    /**
     * Execute insert/update
     *
     * @param statement statement
     * @return this river source
     * @throws SQLException
     */
    RiverSource executeUpdate(PreparedStatement statement) throws SQLException;

    RiverSource executeUpdate(Statement statement, String sql) throws SQLException;

    void beforeRows(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * Action for the next row of the result set to be processed
     *
     * @param results result
     * @param listener listener
     * @return true if next row exists
     * @throws SQLException
     * @throws IOException
     * @throws ParseException
     */
    boolean nextRow(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException, ParseException;

    void afterRows(ResultSet results, KeyValueStreamListener listener) throws SQLException, IOException;

    /**
     * Parse a value in a row column
     *
     * @param results result set
     * @param num position
     * @param type type
     * @param locale locale
     * @return object
     * @throws SQLException
     * @throws IOException
     * @throws ParseException
     */
    Object parseType(ResultSet results, Integer num, int type, Locale locale) throws SQLException, IOException, ParseException;

    /**
     * Close result set
     *
     * @param result result set
     * @return this river source
     * @throws SQLException
     */
    RiverSource close(ResultSet result) throws SQLException;

    /**
     * Close statement
     *
     * @param statement statement
     * @return this river source
     * @throws SQLException
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
}
