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

package org.elasticsearch.river.jdbc.strategy.column;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;
import org.elasticsearch.river.jdbc.strategy.simple.SimpleValueListener;
import org.elasticsearch.river.jdbc.support.Operations;
import org.elasticsearch.river.jdbc.support.ValueListener;

/**
 * River source implementation for the 'column' strategy
 * 
 * @author Piotr Śliwa <piotr.sliwa@zineinc.com>
 */
public class ColumnRiverSource extends SimpleRiverSource {
    
    private final ESLogger logger = ESLoggerFactory.getLogger(ColumnRiverSource.class.getName());
    
    @Override
    public String strategy() {
        return "column";
    }

    @Override
    public String fetch() throws SQLException, IOException {
        
        String sql = context.pollStatement();
        
        Connection connection = connectionForReading();
        
        String quoteString = connection.getMetaData().getIdentifierQuoteString();
        quoteString = quoteString == null ? "" : quoteString;  

        OpInfo[] opInfos = new OpInfo[]{
            new OpInfo(Operations.OP_CREATE, quoteString+context.columnCreatedAt()+quoteString+" >= ?"),
            new OpInfo(Operations.OP_INDEX, quoteString+context.columnUpdatedAt()+quoteString+" >= ?"),
            new OpInfo(Operations.OP_DELETE, quoteString+context.columnDeletedAt()+quoteString+" >= ?"),
        };

        Timestamp from = getLastRunTimestamp();
        
        for(OpInfo opInfo : opInfos) {
            
            String fullSql = getFullSql(sql, opInfo.where);            
            logger.info("sql: {}", fullSql);
            
            PreparedStatement stmt = connection.prepareStatement(fullSql);
            stmt.setTimestamp(1, from);
            
            ResultSet result = executeQuery(stmt);
            
            try {
                ValueListener listener = new ColumnValueListener(opInfo.opType)
                        .target(context.riverMouth())
                        .digest(context.digesting());

                merge(result, listener);
            } catch (Exception e) {
                throw new IOException(e);
            }

            close(result);
            close(stmt);
            acknowledge();
        }

        return null;
    }
    
    private String getFullSql(String sql, String whereClauseToAppend) {
            final String whereKeyword = "where ";
            int whereIndex = sql.toLowerCase().indexOf(whereKeyword);
            
            String fullSql = whereIndex > 0 ? 
                    sql.substring(0, whereIndex + whereKeyword.length())+whereClauseToAppend+" AND "+sql.substring(whereIndex+whereKeyword.length()) 
                    : sql+" WHERE "+whereClauseToAppend;
            
            return fullSql;
    }

    private Timestamp getLastRunTimestamp() {
        Map jdbcSettings = (Map) context.riverSettings().get("jdbc");
        
        if(jdbcSettings == null || jdbcSettings.get(ColumnRiverFlow.LAST_RUN_TIME) == null) {
            return new Timestamp(0);
        }
        
        TimeValue lastRunTime = (TimeValue) jdbcSettings.get(ColumnRiverFlow.LAST_RUN_TIME);
        
        return new Timestamp(lastRunTime.millis());
    }
    
    private static class OpInfo {
        final String where;
        final String opType;
        
        public OpInfo(String opType, String where) {
            this.opType = opType;
            this.where = where;
        }
    }
}
