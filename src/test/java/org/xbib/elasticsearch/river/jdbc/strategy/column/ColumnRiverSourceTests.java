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

package org.xbib.elasticsearch.river.jdbc.strategy.column;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverSettings;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.AbstractRiverNodeTest;
import org.xbib.elasticsearch.river.jdbc.support.Operations;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.river.jdbc.support.PlainStructuredObject;

public class ColumnRiverSourceTests extends AbstractRiverNodeTest {

    private final ESLogger logger = ESLoggerFactory.getLogger(ColumnRiverSourceTests.class.getName());
    
    private Random random = new Random();    
    private TimeValue LAST_RUN_TIME = new TimeValue(new java.util.Date().getTime() - 60*60*1000);
    private TimeValue CURRENT_RUN_START_TIME = new TimeValue(new java.util.Date().getTime());
    
    @Override
    public RiverSource getRiverSource() {
        ColumnRiverSource source = new ColumnRiverSource();
        
        return source;
    }

    @Override
    public RiverContext getRiverContext() {
        RiverContext context = new RiverContext();
        context.digesting(true);
        return context;
    }
    
    @Test()
    @Parameters({"river-existedWhereClause", "sqlInsert"})
    public void testCreateObjects(String riverResource, String sql) throws SQLException, IOException, InterruptedException {        
        verifyCreateObjects(riverResource, sql);
    }
    
    @Test
    @Parameters({"river-whereClausePlaceholder", "sqlInsert"})
    public void testCreateObjects_configurationWithWherePlaceholder(String riverResource, String sql) throws SQLException, IOException, InterruptedException {
        verifyCreateObjects(riverResource, sql);
    }
    
    private void verifyCreateObjects(String riverResource, String sql) throws SQLException, IOException, InterruptedException {
        final int newRecordsOutOfTimeRange = 3;
        final int newRecordsInTimeRange = 2;
        final int updatedRecordsInTimeRange = 4;
        
        testColumnRiver(new MockRiverMouth(), riverResource, sql, new ProductFixture[] {
            ProductFixture
                .size(newRecordsOutOfTimeRange)
                .createdAt(oldTimestamp()),
            ProductFixture
                .size(newRecordsInTimeRange)
                .createdAt(okTimestamp()),
            ProductFixture
                .size(updatedRecordsInTimeRange)
                .createdAt(oldTimestamp())
                .updatedAt(okTimestamp()),
        }, newRecordsInTimeRange + updatedRecordsInTimeRange);
    }
    
    @Test()
    @Parameters({"river-sqlparams", "sqlInsert"})
    public void testCreateObjects_configurationWithSqlParams(String riverResource, String sql)  throws SQLException, IOException, InterruptedException {
        verifyCreateObjects(riverResource, sql);
    }
    
    @Test()
    @Parameters({"river-sqlForTestDeletions", "sqlInsert"})
    public void testRemoveObjects(String riverResource, String insertSql) throws SQLException, IOException, InterruptedException {
        verifyDeleteObjects(riverResource, insertSql);
    }
    
    private void verifyDeleteObjects(String riverResource, String insertSql) throws IOException, SQLException, InterruptedException {        
        MockRiverMouth riverMouth = new MockRiverMouth();

        boolean[] shouldProductsBeDeleted = new boolean[] { true, true, false };
        
        ProductFixtures productFixtures = createFixturesAndPopulateMouth(shouldProductsBeDeleted, riverMouth);
        
        testColumnRiver(riverMouth, riverResource, insertSql, productFixtures.fixtures, productFixtures.expectedCount);
    }
    
    @Test()
    @Parameters({"river-sqlForTestDeletionsAndWherePlaceholder", "sqlInsert"})
    public void testRemoveObjects_configurationWithWherePlaceholder(String riverResource, String insertSql) throws SQLException, IOException, InterruptedException {
        verifyDeleteObjects(riverResource, insertSql);
    }

    private ProductFixtures createFixturesAndPopulateMouth(boolean[] shouldProductsBeDeleted, MockRiverMouth riverMouth) throws IOException {
        ProductFixture[] fixtures = new ProductFixture[shouldProductsBeDeleted.length];
        int expectedExistsCountAfterRiverRun = 0;
        for(int i=0; i<shouldProductsBeDeleted.length; i++) {           
            riverMouth.index(new PlainStructuredObject() {}
                    .id(Integer.toString(i))
                    .source(createStructuredObjectSource(i))
                    .optype(Operations.OP_DELETE));
            
            Timestamp deletedAt;
            
            if(shouldProductsBeDeleted[i]) {
                deletedAt = okTimestamp();
            } else {
                deletedAt = oldTimestamp();
                expectedExistsCountAfterRiverRun++;
            }
            
            fixtures[i] = ProductFixture.one()
                    .setId(i)
                    .createdAt(oldTimestamp())
                    .updatedAt(oldTimestamp())
                    .deletedAt(deletedAt);
        }
        ProductFixtures productFixtures = new ProductFixtures(fixtures, expectedExistsCountAfterRiverRun);
        return productFixtures;
    }
    
    private Map<String, ? super Object> createStructuredObjectSource(int id) {
        Map<String, ? super Object> map = new HashMap<String, Object>();
        map.put("id", id);
        map.put("name", null);
        return map;
    }
    
    private void testColumnRiver(MockRiverMouth riverMouth, String riverResource, String sql, ProductFixture[] fixtures, int expectedHits) throws SQLException, IOException, InterruptedException {
        createData(sql, fixtures);

        context.riverMouth(riverMouth);        
        
        setContextSettings(riverResource);
        
        source.fetch();

        assertEquals(riverMouth.data().size(), expectedHits);
    }
    
    private void setContextSettings(String riverResource) throws IOException {
        RiverSettings riverSettings = riverSettings(riverResource);
        
        Map<String, Object> settings = (Map<String, Object>) riverSettings.settings().get("jdbc");
        logger.info("settings: "+settings);
        
        settings.put(ColumnRiverFlow.LAST_RUN_TIME, LAST_RUN_TIME);
        settings.put(ColumnRiverFlow.CURRENT_RUN_STARTED_TIME, CURRENT_RUN_START_TIME);
        
        context.pollStatement(XContentMapValues.nodeStringValue(settings.get("sql"), null));
        context.columnCreatedAt(XContentMapValues.nodeStringValue(settings.get("column_created_at"), null));
        context.columnUpdatedAt(XContentMapValues.nodeStringValue(settings.get("column_updated_at"), null));
        context.columnDeletedAt(XContentMapValues.nodeStringValue(settings.get("column_deleted_at"), null));
        context.riverSettings(riverSettings.settings());
        context.pollStatementParams(XContentMapValues.extractRawValues("sqlparams", settings));
        context.columnEscape(true);
    }
    
    private Timestamp okTimestamp() {
        return new Timestamp(LAST_RUN_TIME.getMillis() + 1000);
    }
    
    private Timestamp oldTimestamp() {
        return new Timestamp(LAST_RUN_TIME.getMillis() - 1000);
    }
    
    private void createData(String sql, ProductFixture[] fixtures) throws SQLException {
        Connection conn = source.connectionForWriting();
        
        for(ProductFixture fixture : fixtures) {
            createData(conn, sql, fixture);
        }
        
        source.closeWriting();
    }
    
    private void createData(Connection connection, String sql, ProductFixture fixture) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        
        logger.info("timestamps: ["+fixture.createdAt+", "+fixture.updatedAt+", "+fixture.deletedAt+"]");
        
        for(int i=0; i<fixture.size; i++) {
            stmt.setInt(1, fixture.id >= 0 ? fixture.id : random.nextInt());
            stmt.setNull(2, Types.VARCHAR);
            stmt.setInt(3, 1);
            stmt.setDouble(4, 1.1);
            
            if(fixture.createdAt != null) {
                stmt.setTimestamp(5, fixture.createdAt);
            } else {
                stmt.setNull(5, Types.TIMESTAMP);
            }
            
            if(fixture.updatedAt != null) {
                stmt.setTimestamp(6, fixture.updatedAt);
            } else {
                stmt.setNull(6, Types.TIMESTAMP);
            }
            
            if(fixture.deletedAt != null) {
                stmt.setTimestamp(7, fixture.deletedAt);
            } else {
                stmt.setNull(7, Types.TIMESTAMP);
            }

            stmt.execute();
        }
    }
    
    private static class ProductFixtures {
        int expectedCount;
        ProductFixture[] fixtures;
        
        ProductFixtures(ProductFixture[] fixtures, int expectedCount) {
            this.expectedCount = expectedCount;
            this.fixtures = fixtures;
        }
    }
    
    private static class ProductFixture {
        private int id = -1;
        private Timestamp createdAt;
        private Timestamp deletedAt;
        private Timestamp updatedAt;
        private int size;
        
        static ProductFixture one() {
            return size(1);
        }
        
        static ProductFixture size(int size) {
            return new ProductFixture(size);
        }
        
        ProductFixture(int size) {
            this.size = size;
        }
        
        ProductFixture createdAt(Timestamp ts) {            
            this.createdAt = ts;            
            return this;
        }
        
        ProductFixture deletedAt(Timestamp ts) {
            this.deletedAt = ts;
            return this;
        }
        
        ProductFixture updatedAt(Timestamp ts) {
            this.updatedAt = ts;
            return this;
        }
        
        ProductFixture setId(int id) {
            this.id = id;
            return this;
        }
    }
}
