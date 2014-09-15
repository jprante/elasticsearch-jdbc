package org.xbib.elasticsearch.river.jdbc.strategy.mock;

import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.plugin.jdbc.SQLCommand;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
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
 * @author <a href="piotr.sliwa@zineinc.com">Piotr Śliwa</a>
 */
public class MockRiverSource implements RiverSource {

    @Override
    public String strategy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource setRiverContext(RiverContext context) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void fetch() throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource setUrl(String url) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource setUser(String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RiverSource setPassword(String password) {
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
    public RiverSource bind(PreparedStatement statement, List<Object> values) throws SQLException {
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

    @Override
    public RiverSource setLocale(Locale locale) {
        return this;
    }

    @Override
    public Locale getLocale() {
        return Locale.getDefault();
    }

    @Override
    public RiverSource setTimeZone(TimeZone timezone) {
        return this;
    }

    @Override
    public TimeZone getTimeZone() {
        return TimeZone.getDefault();
    }
	@Override
	public RiverSource setSsl(boolean ssl) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public RiverSource setKeyStore(String keyStore) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public RiverSource setKeyStorePassword(String keyStorePassword) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public RiverSource setTrustStore(String strustStore) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public RiverSource setTrustStorePassword(String trustStorePassword) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Not supported yet.");
	}

}
