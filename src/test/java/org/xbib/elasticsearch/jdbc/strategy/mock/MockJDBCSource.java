package org.xbib.elasticsearch.jdbc.strategy.mock;

import org.xbib.elasticsearch.jdbc.keyvalue.KeyValueStreamListener;
import org.xbib.elasticsearch.jdbc.util.SQLCommand;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;

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
 * @author <a href="piotr.sliwa@zineinc.com">Piotr Śliwa</a>
 */
public class MockJDBCSource implements JDBCSource<MockContext> {

    @Override
    public String strategy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource<MockContext> newInstance() {
        return new MockJDBCSource();
    }

    @Override
    public JDBCSource setContext(MockContext context) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void beforeFetch() throws Exception {
    }

    @Override
    public void afterFetch() throws Exception {
    }

    @Override
    public void fetch() throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource setUrl(String url) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource setUser(String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource setPassword(String password) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Connection getConnectionForReading() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Connection getConnectionForWriting() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareQuery(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource bind(PreparedStatement statement, List<Object> values) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource register(CallableStatement statement, Map<String, Object> values) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ResultSet executeQuery(PreparedStatement statement) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ResultSet executeQuery(Statement statement, String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource executeUpdate(PreparedStatement statement) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource executeUpdate(Statement statement, String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void beforeRows(ResultSet result, KeyValueStreamListener listener) throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void beforeRows(SQLCommand command, ResultSet result, KeyValueStreamListener listener) throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean nextRow(ResultSet result, KeyValueStreamListener listener) throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean nextRow(SQLCommand command, ResultSet result, KeyValueStreamListener listener) throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void afterRows(ResultSet result, KeyValueStreamListener listener) throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void afterRows(SQLCommand command, ResultSet result, KeyValueStreamListener listener) throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object parseType(ResultSet result, Integer num, int type, Locale locale) throws SQLException, IOException, ParseException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource close(ResultSet result) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource close(Statement statement) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource closeReading() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource closeWriting() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource setLocale(Locale locale) {
        return this;
    }

    @Override
    public Locale getLocale() {
        return Locale.ROOT;
    }

    @Override
    public JDBCSource setTimeZone(TimeZone timezone) {
        return this;
    }

    @Override
    public TimeZone getTimeZone() {
        return TimeZone.getDefault();
    }

    @Override
    public void suspend() throws Exception {
    }

    @Override
    public void resume() throws Exception {
    }

    @Override
    public void shutdown() throws IOException {
    }

}
