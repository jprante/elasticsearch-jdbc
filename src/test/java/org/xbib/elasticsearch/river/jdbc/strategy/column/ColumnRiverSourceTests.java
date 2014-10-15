package org.xbib.elasticsearch.river.jdbc.strategy.column;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverSettings;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.plugin.jdbc.state.RiverState;
import org.xbib.elasticsearch.plugin.jdbc.util.IndexableObject;
import org.xbib.elasticsearch.plugin.jdbc.util.PlainIndexableObject;
import org.xbib.elasticsearch.plugin.jdbc.util.SQLCommand;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ColumnRiverSourceTests extends AbstractColumnRiverTest {

    private final ESLogger logger = ESLoggerFactory.getLogger(ColumnRiverSourceTests.class.getName());

    private Random random = new Random();

    private DateTime LAST_RUN_TIME = new DateTime(new DateTime().getMillis() - 60 * 60 * 1000);

    @Override
    public ColumnRiverSource newRiverSource() {
        return new ColumnRiverSource();
    }

    @Override
    public ColumnRiverContext newRiverContext() {
        return new ColumnRiverContext();
    }

    @Test
    @Parameters({"river-existedWhereClause", "sqlInsert"})
    public void testCreateObjects(String riverResource, String sql) throws Exception {
        verifyCreateObjects(riverResource, sql);
    }

    @Test
    @Parameters({"river-whereClausePlaceholder", "sqlInsert"})
    public void testCreateObjects_configurationWithWherePlaceholder(String riverResource, String sql)
            throws Exception {
        verifyCreateObjects(riverResource, sql);
    }

    @Test
    @Parameters({"river-sqlparams", "sqlInsert"})
    public void testCreateObjects_configurationWithSqlParams(String riverResource, String sql)
            throws Exception {
        verifyCreateObjects(riverResource, sql);
    }

    @Test
    @Parameters({"river-sqlForTestDeletions", "sqlInsert"})
    public void testRemoveObjects(String riverResource, String insertSql)
            throws Exception {
        verifyDeleteObjects(riverResource, insertSql);
    }

    @Test
    @Parameters({"river-sqlForTestDeletionsAndWherePlaceholder", "sqlInsert"})
    public void testRemoveObjects_configurationWithWherePlaceholder(String riverResource, String insertSql)
            throws Exception {
        verifyDeleteObjects(riverResource, insertSql);
    }

    @Test
    @Parameters({"river-existedWhereClauseWithOverlap", "sqlInsert"})
    public void testCreateObjects_withLastRunTimeStampOverlap(String riverResource, String sql)
            throws Exception {
        final int newRecordsOutOfTimeRange = 3;
        final int newRecordsInTimeRange = 2;
        final int updatedRecordsInTimeRange = 4;
        final int updatedRecordsInTimeRangeWithOverlap = 1;
        testColumnRiver(new MockRiverMouth(), riverResource, sql, new ProductFixture[]{
                ProductFixture.size(newRecordsOutOfTimeRange).createdAt(oldTimestamp()),
                ProductFixture.size(newRecordsInTimeRange).createdAt(okTimestamp()),
                ProductFixture.size(updatedRecordsInTimeRange).createdAt(oldTimestamp()).updatedAt(okTimestamp()),
                ProductFixture.size(updatedRecordsInTimeRangeWithOverlap).createdAt(oldTimestamp()).updatedAt(overlapTimestamp()),
        }, newRecordsInTimeRange + updatedRecordsInTimeRange + updatedRecordsInTimeRangeWithOverlap);
    }

    private void verifyCreateObjects(String riverResource, String sql)
            throws Exception {
        final int newRecordsOutOfTimeRange = 3;
        final int newRecordsInTimeRange = 2;
        final int updatedRecordsInTimeRange = 4;
        testColumnRiver(new MockRiverMouth(), riverResource, sql, new ProductFixture[]{
                new ProductFixture(newRecordsOutOfTimeRange).createdAt(oldTimestamp()),
                new ProductFixture(newRecordsInTimeRange).createdAt(okTimestamp()),
                new ProductFixture(updatedRecordsInTimeRange).createdAt(oldTimestamp()).updatedAt(okTimestamp()),
        }, newRecordsInTimeRange + updatedRecordsInTimeRange);
    }

    private void verifyDeleteObjects(String riverResource, String insertSql)
            throws Exception {
        MockRiverMouth riverMouth = new MockRiverMouth();
        boolean[] shouldProductsBeDeleted = new boolean[]{true, true, false};
        ProductFixtures productFixtures = createFixturesAndPopulateMouth(shouldProductsBeDeleted, riverMouth);
        testColumnRiver(riverMouth, riverResource, insertSql,
                productFixtures.fixtures,
                productFixtures.expectedCount);
    }

    private ProductFixtures createFixturesAndPopulateMouth(boolean[] shouldProductsBeDeleted, MockRiverMouth riverMouth)
            throws IOException {
        ProductFixture[] fixtures = new ProductFixture[shouldProductsBeDeleted.length];
        int expectedExistsCountAfterRiverRun = 0;
        for (int i = 0; i < shouldProductsBeDeleted.length; i++) {
            IndexableObject p = new PlainIndexableObject()
                    .id(Integer.toString(i))
                    .source(createSource(i))
                    .optype("delete");
            riverMouth.index(p, false);
            Timestamp deletedAt;
            if (shouldProductsBeDeleted[i]) {
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
        return new ProductFixtures(fixtures, expectedExistsCountAfterRiverRun);
    }

    private Map<String, Object> createSource(int id) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("id", id);
        map.put("name", null);
        return map;
    }

    private void testColumnRiver(MockRiverMouth riverMouth, String riverResource, String sql, ProductFixture[] fixtures, int expectedHits)
            throws Exception {
        createData(sql, fixtures);
        context.setRiverMouth(riverMouth);
        createRiverContext(riverResource);
        source.fetch();
        assertEquals(riverMouth.data().size(), expectedHits);
    }

    private void createRiverContext(String riverResource) throws IOException {
        RiverSettings riverSettings = riverSettings(riverResource);
        Map<String, Object> settings = (Map<String, Object>) riverSettings.settings().get("jdbc");

        RiverState riverState = new RiverState();
        riverState.getMap().put(ColumnRiverFlow.LAST_RUN_TIME, LAST_RUN_TIME);
        riverState.getMap().put(ColumnRiverFlow.CURRENT_RUN_STARTED_TIME, new DateTime());

        context
                .columnCreatedAt(XContentMapValues.nodeStringValue(settings.get("column_created_at"), null))
                .columnUpdatedAt(XContentMapValues.nodeStringValue(settings.get("column_updated_at"), null))
                .columnDeletedAt(XContentMapValues.nodeStringValue(settings.get("column_deleted_at"), null))
                .columnEscape(true)
                .setLastRunTimeStampOverlap(getLastRunTimestampOverlap(riverSettings))
                .setStatements(SQLCommand.parse(settings))
                .setRiverState(riverState);
    }

    private TimeValue getLastRunTimestampOverlap(RiverSettings riverSettings) {
        TimeValue overlap = TimeValue.timeValueMillis(0);
        Map<String, Object> settings = ((Map<String, Object>) (riverSettings.settings().get("jdbc")));
        if (settings != null && settings.containsKey("last_run_timestamp_overlap")) {
            overlap = XContentMapValues.nodeTimeValue(settings.get("last_run_timestamp_overlap"));
        }
        return overlap;
    }

    private Timestamp okTimestamp() {
        return new Timestamp(LAST_RUN_TIME.getMillis() + 60 * 2 * 1000);
    }

    private Timestamp oldTimestamp() {
        return new Timestamp(LAST_RUN_TIME.getMillis() - 60 * 2 * 1000);
    }

    private Timestamp overlapTimestamp() {
        return new Timestamp(LAST_RUN_TIME.getMillis() - 1000);
    }

    private void createData(String sql, ProductFixture[] fixtures) throws SQLException {
        Connection conn = source.getConnectionForWriting();
        for (ProductFixture fixture : fixtures) {
            createData(conn, sql, fixture);
        }
        source.closeWriting();
    }

    private void createData(Connection connection, String sql, ProductFixture fixture) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        logger.debug("timestamps: [" + fixture.createdAt + ", " + fixture.updatedAt + ", " + fixture.deletedAt + "]");
        for (int i = 0; i < fixture.size; i++) {
            int id = fixture.id >= 0 ? fixture.id : random.nextInt();
            logger.debug("id={}", id);
            stmt.setInt(1, id);
            stmt.setNull(2, Types.VARCHAR);
            stmt.setInt(3, 1);
            stmt.setDouble(4, 1.1);
            if (fixture.createdAt != null) {
                stmt.setTimestamp(5, fixture.createdAt);
            } else {
                stmt.setNull(5, Types.TIMESTAMP);
            }
            if (fixture.updatedAt != null) {
                stmt.setTimestamp(6, fixture.updatedAt);
            } else {
                stmt.setNull(6, Types.TIMESTAMP);
            }
            if (fixture.deletedAt != null) {
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
