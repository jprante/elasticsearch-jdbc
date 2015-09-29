/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.jdbc.strategy.mock;

import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.common.keyvalue.KeyValueStreamListener;
import org.xbib.elasticsearch.common.util.SQLCommand;
import org.xbib.elasticsearch.common.util.SourceMetric;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.jdbc.strategy.Source;

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
    public JDBCSource<MockContext> setContext(MockContext context) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public MockContext getContext() {
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
    public JDBCSource<MockContext> setUrl(String url) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource<MockContext> setUser(String user) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource<MockContext> setPassword(String password) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource<MockContext> setScale(int scale) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setRounding(String rounding) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setStatements(List<SQLCommand> sql) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setAutoCommit(boolean autocommit) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setMaxRows(int maxRows) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setFetchSize(int fetchSize) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setRetries(int retries) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setMaxRetryWait(TimeValue maxretrywait) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setResultSetType(String resultSetType) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setResultSetConcurrency(String resultSetConcurrency) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> shouldIgnoreNull(boolean shouldIgnoreNull) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> shouldDetectGeo(boolean shouldDetectGeo) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> shouldDetectJson(boolean shouldDetectJson) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> shouldPrepareResultSetMetadata(boolean shouldPrepareResultSetMetadata) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> shouldPrepareDatabaseMetadata(boolean shouldPrepareDatabaseMetadata) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setQueryTimeout(int queryTimeout) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setConnectionProperties(Map<String, Object> connectionProperties) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setColumnNameMap(Map<String, Object> columnNameMap) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> shouldTreatBinaryAsString(boolean shouldTreatBinaryAsString) {
        return this;
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
    public JDBCSource<MockContext> bind(PreparedStatement statement, List<Object> values) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource<MockContext> register(CallableStatement statement, Map<String, Object> values) throws SQLException {
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
    public JDBCSource<MockContext> executeUpdate(PreparedStatement statement) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource<MockContext> executeUpdate(Statement statement, String sql) throws SQLException {
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
    public JDBCSource<MockContext> close(ResultSet result) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource<MockContext> close(Statement statement) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource<MockContext> closeReading() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource<MockContext> closeWriting() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JDBCSource<MockContext> setLocale(Locale locale) {
        return this;
    }

    @Override
    public JDBCSource<MockContext> setTimeZone(TimeZone timezone) {
        return this;
    }

    @Override
    public void shutdown() throws IOException {
    }

    @Override
    public Source setMetric(SourceMetric metric) {
        return this;
    }

    @Override
    public SourceMetric getMetric() {
        return null;
    }

}
