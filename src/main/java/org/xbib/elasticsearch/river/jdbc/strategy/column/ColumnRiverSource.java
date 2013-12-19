
package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;
import org.xbib.elasticsearch.river.jdbc.support.Operations;
import org.xbib.elasticsearch.river.jdbc.support.SimpleValueListener;
import org.xbib.elasticsearch.river.jdbc.support.StructuredObject;
import org.xbib.elasticsearch.river.jdbc.support.ValueListener;

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
import java.util.Map;

/**
 * River source implementation for the 'column' strategy
 *
 * @author Piotr Śliwa <piotr.sliwa@zineinc.com>
 */
public class ColumnRiverSource extends SimpleRiverSource {

    private final ESLogger logger = ESLoggerFactory.getLogger(ColumnRiverSource.class.getSimpleName());

    protected ESLogger logger() {
        return logger;
    }

    @Override
    public String strategy() {
        return "column";
    }

    @Override
    public String fetch() throws SQLException, IOException {

        String sql = context.pollStatement();

        Connection connection = connectionForReading();

        List<OpInfo> opInfos = getOpInfos(connection);

        Timestamp lastRunTimestamp = getLastRunTimestamp();

        for (OpInfo opInfo : opInfos) {
            fetch(connection, sql, opInfo, lastRunTimestamp);
        }

        return null;
    }

    private List<OpInfo> getOpInfos(Connection connection) throws SQLException {

        String quoteString = getIdentifierQuoteString(connection);

        List<OpInfo> opInfos = new LinkedList<OpInfo>();

        String noDeletedWhereClause = context.columnDeletedAt() != null ? " AND " + quoteColumn(context.columnDeletedAt(), quoteString) + " IS NULL" : "";

        opInfos.add(new OpInfo(Operations.OP_CREATE, quoteColumn(context.columnCreatedAt(), quoteString) + " >= ?" + noDeletedWhereClause));
        opInfos.add(new OpInfo(Operations.OP_INDEX, quoteColumn(context.columnUpdatedAt(), quoteString) + " >= ? AND (" + quoteColumn(context.columnCreatedAt(), quoteString) + " IS NULL OR " + quoteColumn(context.columnCreatedAt(), quoteString) + " < ?)" + noDeletedWhereClause, 2));

        if (context.columnDeletedAt() != null) {
            opInfos.add(new OpInfo(Operations.OP_DELETE, quoteColumn(context.columnDeletedAt(), quoteString) + " >= ?"));
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
        Map jdbcSettings = (Map) context.riverSettings().get("jdbc");

        if (jdbcSettings == null || jdbcSettings.get(ColumnRiverFlow.LAST_RUN_TIME) == null) {
            return new Timestamp(0);
        }

        TimeValue lastRunTime = (TimeValue) jdbcSettings.get(ColumnRiverFlow.LAST_RUN_TIME);

        return new Timestamp(lastRunTime.millis());
    }

    private void fetch(Connection connection, String sql, OpInfo opInfo, Timestamp lastRunTimestamp) throws IOException, SQLException {
        preInit();
        String fullSql = addWhereClauseToSqlQuery(sql, opInfo.where);

        PreparedStatement stmt = connection.prepareStatement(fullSql);
        List<Object> params = createQueryParams(lastRunTimestamp, opInfo.paramsInWhere);

        logger.debug("sql: {}, params {}", fullSql, params);

        ResultSet result = null;

        try {
            bind(stmt, params);

            result = executeQuery(stmt);

            try {
                ValueListener listener = new ColumnValueListener(opInfo.opType)
                        .target(context.riverMouth())
                        .digest(context.digesting());

                merge(result, listener);
            } catch (Exception e) {
                throw new IOException(e);
            }
        } finally {
            close(result);
            close(stmt);
        }

        acknowledge();
    }

    private String addWhereClauseToSqlQuery(String sql, String whereClauseToAppend) {
        final String whereKeyword = "where ";
        int whereIndex = sql.toLowerCase().indexOf(whereKeyword);

        return whereIndex > 0 ?
                sql.substring(0, whereIndex + whereKeyword.length()) + whereClauseToAppend + " AND " + sql.substring(whereIndex + whereKeyword.length())
                : sql + " WHERE " + whereClauseToAppend;
    }

    private List<Object> createQueryParams(Timestamp lastRunTimestamp, int lastRunTimestampParamsCount) {
        List<? extends Object> statementParams = context.pollStatementParams() != null ? context.pollStatementParams() : Collections.emptyList();
        List<Object> params = new ArrayList<Object>(statementParams.size() + lastRunTimestampParamsCount);

        for (int i = 0; i < lastRunTimestampParamsCount; i++) {
            params.add(lastRunTimestamp);
        }

        for (Object param : statementParams) {
            params.add(param);
        }

        return params;
    }

    private static class OpInfo {
        final String where;
        final String opType;
        final int paramsInWhere;

        public OpInfo(String opType, String where, int paramsInWhere) {
            this.opType = opType;
            this.where = where;
            this.paramsInWhere = paramsInWhere;
        }

        public OpInfo(String opType, String where) {
            this(opType, where, 1);
        }
    }

    private static class ColumnValueListener extends SimpleValueListener<Object> {

        private String opType;

        public ColumnValueListener(String opType) {
            this.opType = opType;
        }

        @Override
        public SimpleValueListener end(StructuredObject object) throws IOException {

            if (!object.source().isEmpty()) {
                object.optype(opType);
            }

            return super.end(object);
        }
    }
}
