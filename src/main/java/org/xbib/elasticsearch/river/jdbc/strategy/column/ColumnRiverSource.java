/*
 * Copyright (C) 2014 Jörg Prante
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
package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.xbib.elasticsearch.plugin.jdbc.keyvalue.KeyValueStreamListener;
import org.xbib.elasticsearch.plugin.jdbc.util.IndexableObject;
import org.xbib.elasticsearch.plugin.jdbc.util.RiverMouthKeyValueStreamListener;
import org.xbib.elasticsearch.plugin.jdbc.util.SQLCommand;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * River source implementation for the 'column' strategy
 *
 * @author <a href="piotr.sliwa@zineinc.com">Piotr Śliwa</a>
 */
public class ColumnRiverSource<RC extends ColumnRiverContext> extends SimpleRiverSource<RC> {

    private static final ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.ColumnRiverSource");

    private static final String WHERE_CLAUSE_PLACEHOLDER = "$where";

    @Override
    public String strategy() {
        return "column";
    }

    @Override
    public ColumnRiverSource<RC> newInstance() {
        return new ColumnRiverSource<RC>();
    }

    @Override
    public void fetch() throws SQLException, IOException {
        logger.info("fetch state={}", context.getRiverState());
        for (SQLCommand command : context.getStatements()) {
            Connection connection = getConnectionForReading();
            if (connection != null) {
                List<OpInfo> opInfos = getOpInfos(connection);
                Timestamp lastRunTimestamp = getLastRunTimestamp();
                logger.debug("lastRunTimestamp={}", lastRunTimestamp);
                for (OpInfo opInfo : opInfos) {
                    logger.debug("opinfo={}", opInfo.toString());
                    fetch(connection, command, opInfo, lastRunTimestamp);
                }
            }
        }
    }

    private List<OpInfo> getOpInfos(Connection connection) throws SQLException {
        String quoteString = getIdentifierQuoteString(connection);
        List<OpInfo> opInfos = new LinkedList<OpInfo>();
        String noDeletedWhereClause = context.columnDeletedAt() != null ?
                " AND " + quoteColumn(context.columnDeletedAt(), quoteString) + " IS NULL" : "";
        if (context.isTimestampDiffSupported()) {
            opInfos.add(new OpInfo("create", "{fn TIMESTAMPDIFF(SQL_TSI_SECOND,?," + quoteColumn(context.columnCreatedAt(), quoteString) + ")} >= 0" + noDeletedWhereClause));
            opInfos.add(new OpInfo("index", "{fn TIMESTAMPDIFF(SQL_TSI_SECOND,?," + quoteColumn(context.columnUpdatedAt(), quoteString) + ")} >= 0 AND (" + quoteColumn(context.columnCreatedAt(), quoteString) + " IS NULL OR {fn TIMESTAMPDIFF(SQL_TSI_SECOND,?," + quoteColumn(context.columnCreatedAt(), quoteString) + ")} < 0) " + noDeletedWhereClause, 2));
            if (context.columnDeletedAt() != null) {
                opInfos.add(new OpInfo("delete", "{fn TIMESTAMPDIFF(SQL_TSI_SECOND,?," + quoteColumn(context.columnDeletedAt(), quoteString) + ")} >= 0"));
            }
        } else {
            // no TIMESTAMPDIFF support
            opInfos.add(new OpInfo("create", quoteColumn(context.columnCreatedAt(), quoteString) + " >= ?" + noDeletedWhereClause));
            opInfos.add(new OpInfo("index", quoteColumn(context.columnUpdatedAt(), quoteString) + " >= ? AND (" + quoteColumn(context.columnCreatedAt(), quoteString) + " IS NULL OR " + quoteColumn(context.columnCreatedAt(), quoteString) + " < ?)" + noDeletedWhereClause, 2));

            if (context.columnDeletedAt() != null) {
                opInfos.add(new OpInfo("delete", quoteColumn(context.columnDeletedAt(), quoteString) + " >= ?"));
            }
        }
        return opInfos;
    }

    private String getIdentifierQuoteString(Connection connection) throws SQLException {
        if (!context.columnEscape()) {
            return "";
        }
        String quoteString = connection.getMetaData().getIdentifierQuoteString();
        quoteString = quoteString == null ? "" : quoteString;
        return quoteString;
    }

    private String quoteColumn(String column, String quote) {
        return quote + column + quote;
    }

    private Timestamp getLastRunTimestamp() {
        DateTime lastRunTime = context.getRiverState() != null ?
                (DateTime) context.getRiverState().getMap().get(ColumnRiverFlow.LAST_RUN_TIME) : null;
        if (lastRunTime == null) {
            return new Timestamp(0);
        }
        return new Timestamp(lastRunTime.getMillis() - context.getLastRunTimeStampOverlap().millis());
    }

    private void fetch(Connection connection, SQLCommand command, OpInfo opInfo, Timestamp lastRunTimestamp) throws IOException, SQLException {
        String fullSql = addWhereClauseToSqlQuery(command.getSQL(), opInfo.where);
        PreparedStatement stmt = connection.prepareStatement(fullSql);
        List<Object> params = createQueryParams(command, lastRunTimestamp, opInfo.paramsInWhere);
        logger.debug("sql: {}, params {}", fullSql, params);
        ResultSet result = null;
        try {
            bind(stmt, params);
            result = executeQuery(stmt);
            KeyValueStreamListener<Object, Object> listener =
                    new ColumnKeyValueStreamListener<Object, Object>(opInfo.opType)
                            .output(context.getRiverMouth());
            merge(command, result, listener);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            close(result);
            close(stmt);
        }
    }

    private String addWhereClauseToSqlQuery(String sql, String whereClauseToAppend) {
        int wherePlaceholderIndex = sql.indexOf(WHERE_CLAUSE_PLACEHOLDER);
        final String whereKeyword = "where ";
        int whereIndex = sql.toLowerCase().indexOf(whereKeyword);
        if (wherePlaceholderIndex >= 0) {
            return sql.replace(WHERE_CLAUSE_PLACEHOLDER, whereClauseToAppend);
        } else if (whereIndex >= 0) {
            return sql.substring(0, whereIndex + whereKeyword.length()) + whereClauseToAppend + " AND " + sql.substring(whereIndex + whereKeyword.length());
        } else {
            return sql + " WHERE " + whereClauseToAppend;
        }
    }

    private List<Object> createQueryParams(SQLCommand command, Timestamp lastRunTimestamp, int lastRunTimestampParamsCount) {
        List<Object> statementParams = command.getParameters() != null ?
                command.getParameters() : Collections.emptyList();
        List<Object> params = new ArrayList<Object>(statementParams.size() + lastRunTimestampParamsCount);
        for (int i = 0; i < lastRunTimestampParamsCount; i++) {
            params.add(lastRunTimestamp);
        }
        for (Object param : statementParams) {
            params.add(param);
        }
        return params;
    }

    private class OpInfo {
        final String opType;
        final String where;
        final int paramsInWhere;

        public OpInfo(String opType, String where, int paramsInWhere) {
            if (where != null && !where.equals("")) {
                where = "(" + where + ")";
            }
            this.opType = opType;
            this.where = where;
            this.paramsInWhere = paramsInWhere;
        }

        public OpInfo(String opType, String where) {
            this(opType, where, 1);
        }

        public String toString() {
            return opType + " " + where + " " + paramsInWhere;
        }
    }

    private class ColumnKeyValueStreamListener<K, V> extends RiverMouthKeyValueStreamListener<K, V> {

        private String opType;

        public ColumnKeyValueStreamListener(String opType) {
            this.opType = opType;
        }

        @Override
        public ColumnKeyValueStreamListener<K, V> end(IndexableObject object) throws IOException {
            if (!object.source().isEmpty()) {
                object.optype(opType);
            }
            super.end(object);
            return this;
        }
    }
}
