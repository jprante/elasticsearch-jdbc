/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.jdbc.strategy.table;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;
import org.elasticsearch.river.jdbc.support.Operations;
import org.elasticsearch.river.jdbc.support.ValueListener;

/**
 * River source implementation of the 'table' strategy
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class TableRiverSource extends SimpleRiverSource {

    private final ESLogger logger = ESLoggerFactory.getLogger(TableRiverSource.class.getName());

    @Override
    public String strategy() {
        return "table";
    }

    @Override
    public String fetch() throws SQLException, IOException {
        Connection connection = connectionForReading();
        String[] optypes = new String[]{Operations.OP_CREATE, Operations.OP_INDEX, Operations.OP_DELETE};
        for (String optype : optypes) {
            PreparedStatement statement;
            try {
                statement = connection.prepareStatement("select * from \"" + context.riverName() + "\" where \"source_operation\" = ? and \"source_timestamp\" between ? and ?");
            } catch (SQLException e) {
                // hsqldb
                statement = connection.prepareStatement("select * from " + context.riverName() + " where \"source_operation\" = ? and \"source_timestamp\" between ? and ?");
            }
            statement.setString(1, optype);
            java.util.Date d = new java.util.Date();
            long now = d.getTime();
            statement.setTimestamp(2, new Timestamp(now - context.pollingInterval().millis()));
            statement.setTimestamp(3, new Timestamp(now));
            ResultSet results;
            try {
                results = executeQuery(statement);
            } catch (SQLException e) {
                // mysql
                statement = connection.prepareStatement("select * from " + context.riverName() + " where source_operation = ? and source_timestamp between ? and ?");
                statement.setString(1, optype);
                statement.setTimestamp(2, new Timestamp(now - context.pollingInterval().millis()));
                statement.setTimestamp(3, new Timestamp(now));
                results = executeQuery(statement);
            }
            try {
                ValueListener listener = new TableValueListener()
                        .target(context.riverTarget())
                        .digest(context.digesting());
                merge(results, listener); // ignore digest
            } catch (Exception e) {
                throw new IOException(e);
            }
            close(results);
            close(statement);
            acknowledge();
        }
        return null;
    }

    /**
     * Acknowledge a bulk item response back to the river table. Fill columns
     * target_timestamp, taget_operation, target_failed, target_message.
     *
     * @param response
     * @throws IOException
     */
    @Override
    public SimpleRiverSource acknowledge(BulkResponse response) throws IOException {
        if (response == null) {
            logger.warn("can't acknowledge null bulk response");
        }
        try {
            Connection connection = connectionForWriting();
            String riverName = context.riverName();
            for (BulkItemResponse resp : response.items()) {
                PreparedStatement pstmt;
                try {
                    pstmt = prepareUpdate("update \"" + riverName + "\" set \"source_operation\" = 'ack' where \"_index\" = ? and \"_type\" = ? and \"_id\" = ?");
                } catch (SQLException e) {
                    try {
                        // hsqldb
                        pstmt = prepareUpdate("update " + riverName + " set \"source_operation\" = 'ack' where \"_index\" = ? and \"_type\" = ? and \"_id\" = ?");
                    } catch (SQLException e1) {
                        // mysql
                        pstmt = prepareUpdate("update " + riverName + " set source_operation = 'ack' where _index = ? and _type = ? and _id = ?");
                    }
                }
                List<Object> params = new ArrayList();
                params.add(resp.index());
                params.add(resp.type());
                params.add(resp.id());
                bind(pstmt, params);
                executeUpdate(pstmt);
                close(pstmt);
                try {
                    pstmt = prepareUpdate("update \"" + riverName + "_ack\" set \"target_timestamp\" = ?, \"target_operation\" = ?, \"target_failed\" = ?, \"target_message\" = ? where \"_index\" = ? and \"_type\" = ? and \"_id\" = ?");
                } catch (SQLException e) {
                    try {
                        // hsqldb
                        pstmt = prepareUpdate("update " + riverName + "_ack set \"target_timestamp\" = ?, \"target_operation\" = ?, \"target_failed\" = ?, \"target_message\" = ? where \"_index\" = ? and \"_type\" = ? and \"_id\" = ?");
                    } catch (SQLException e1) {
                        // mysql
                        pstmt = prepareUpdate("update " + riverName + "_ack set target_timestamp = ?, target_operation = ?, target_failed = ?, target_message = ? where _index = ? and _type = ? and _id = ?");
                    }
                }
                params = new ArrayList();
                params.add(new Timestamp(new java.util.Date().getTime()));
                params.add(resp.opType());
                params.add(resp.failed());
                params.add(resp.failureMessage());
                params.add(resp.index());
                params.add(resp.type());
                params.add(resp.id());
                bind(pstmt, params);
                executeUpdate(pstmt);
                close(pstmt);
            }
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
        return this;
    }
}
