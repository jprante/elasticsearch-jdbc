
package org.xbib.elasticsearch.river.jdbc.strategy.mock;

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

import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.io.keyvalue.KeyValueStreamListener;

/**
 *
 * @author Piotr Śliwa <piotr.sliwa@zineinc.com>
 */
public class MockRiverSource implements RiverSource {

    @Override
    public String strategy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource riverContext(RiverContext context) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void fetch() throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource url(String url) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource user(String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource password(String password) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Connection connectionForReading() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Connection connectionForWriting() throws SQLException {
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
    public RiverSource bind(PreparedStatement statement, List<? extends Object> values) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource register(CallableStatement statement, Map<String, Object> values) throws SQLException {
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
    public RiverSource executeUpdate(PreparedStatement statement) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource executeUpdate(Statement statement, String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void beforeRows(ResultSet result, KeyValueStreamListener listener) throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean nextRow(ResultSet result, KeyValueStreamListener listener) throws SQLException, IOException, ParseException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void afterRows(ResultSet result, KeyValueStreamListener listener) throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object parseType(ResultSet result, Integer num, int type, Locale locale) throws SQLException, IOException, ParseException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource close(ResultSet result) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource close(Statement statement) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource closeReading() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource closeWriting() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
