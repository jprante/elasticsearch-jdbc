package org.xbib.elasticsearch.jdbc.strategy.column;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTime;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.mock.MockSink;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.common.util.PlainIndexableObject;
import org.xbib.elasticsearch.common.util.SQLCommand;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ColumnStrategySourceTests extends AbstractColumnStrategyTest {

    private final static Logger logger = LogManager.getLogger("test.column.source");

    private Random random = new Random();

    private DateTime LAST_RUN_TIME = new DateTime(new DateTime().getMillis() - 60 * 60 * 1000);

    @Override
    public ColumnSource newSource() {
        return new ColumnSource();
    }

    @Override
    public ColumnContext newContext() {
        return new ColumnContext();
    }

    @Test
    @Parameters({"existedWhereClause", "sqlInsert"})
    public void testCreateObjects(String resource, String sql) throws Exception {
        verifyCreateObjects(resource, sql);
    }

    @Test
    @Parameters({"whereClausePlaceholder", "sqlInsert"})
    public void testCreateObjects_configurationWithWherePlaceholder(String resource, String sql)
            throws Exception {
        verifyCreateObjects(resource, sql);
    }

    @Test
    @Parameters({"sqlparams", "sqlInsert"})
    public void testCreateObjects_configurationWithSqlParams(String resource, String sql)
            throws Exception {
        verifyCreateObjects(resource, sql);
    }

    @Test
    @Parameters({"sqlForTestDeletions", "sqlInsert"})
    public void testRemoveObjects(String resource, String insertSql)
            throws Exception {
        verifyDeleteObjects(resource, insertSql);
    }

    @Test
    @Parameters({"sqlForTestDeletionsAndWherePlaceholder", "sqlInsert"})
    public void testRemoveObjects_configurationWithWherePlaceholder(String resource, String insertSql)
            throws Exception {
        verifyDeleteObjects(resource, insertSql);
    }

    @Test
    @Parameters({"existedWhereClauseWithOverlap", "sqlInsert"})
    public void testCreateObjects_withLastRunTimeStampOverlap(String resource, String sql)
            throws Exception {
        final int newRecordsOutOfTimeRange = 3;
        final int newRecordsInTimeRange = 2;
        final int updatedRecordsInTimeRange = 4;
        final int updatedRecordsInTimeRangeWithOverlap = 1;
        testColumnStrategy(new MockSink(), resource, sql, new ProductFixture[]{
                ProductFixture.size(newRecordsOutOfTimeRange).createdAt(oldTimestamp()),
                ProductFixture.size(newRecordsInTimeRange).createdAt(okTimestamp()),
                ProductFixture.size(updatedRecordsInTimeRange).createdAt(oldTimestamp()).updatedAt(okTimestamp()),
                ProductFixture.size(updatedRecordsInTimeRangeWithOverlap).createdAt(oldTimestamp()).updatedAt(overlapTimestamp()),
        }, newRecordsInTimeRange + updatedRecordsInTimeRange + updatedRecordsInTimeRangeWithOverlap);
    }

    private void verifyCreateObjects(String resource, String sql)
            throws Exception {
        final int newRecordsOutOfTimeRange = 3;
        final int newRecordsInTimeRange = 2;
        final int updatedRecordsInTimeRange = 4;
        testColumnStrategy(new MockSink(), resource, sql, new ProductFixture[]{
                new ProductFixture(newRecordsOutOfTimeRange).createdAt(oldTimestamp()),
                new ProductFixture(newRecordsInTimeRange).createdAt(okTimestamp()),
                new ProductFixture(updatedRecordsInTimeRange).createdAt(oldTimestamp()).updatedAt(okTimestamp()),
        }, newRecordsInTimeRange + updatedRecordsInTimeRange);
    }

    private void verifyDeleteObjects(String resource, String insertSql)
            throws Exception {
        MockSink sink = new MockSink();
        boolean[] shouldProductsBeDeleted = new boolean[]{true, true, false};
        ProductFixtures productFixtures = createFixturesAndPopulateSink(shouldProductsBeDeleted, sink);
        testColumnStrategy(sink, resource, insertSql,
                productFixtures.fixtures,
                productFixtures.expectedCount);
    }

    private ProductFixtures createFixturesAndPopulateSink(boolean[] shouldProductsBeDeleted, MockSink sink)
            throws IOException {
        ProductFixture[] fixtures = new ProductFixture[shouldProductsBeDeleted.length];
        int expectedExistsCountAfterRun = 0;
        for (int i = 0; i < shouldProductsBeDeleted.length; i++) {
            IndexableObject p = new PlainIndexableObject()
                    .id(Integer.toString(i))
                    .source(createSource(i))
                    .optype("delete");
            sink.index(p, false);
            Timestamp deletedAt;
            if (shouldProductsBeDeleted[i]) {
                deletedAt = okTimestamp();
            } else {
                deletedAt = oldTimestamp();
                expectedExistsCountAfterRun++;
            }
            fixtures[i] = ProductFixture.one()
                    .setId(i)
                    .createdAt(oldTimestamp())
                    .updatedAt(oldTimestamp())
                    .deletedAt(deletedAt);
        }
        return new ProductFixtures(fixtures, expectedExistsCountAfterRun);
    }

    private Map<String, Object> createSource(int id) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("id", id);
        map.put("name", null);
        return map;
    }

    private void testColumnStrategy(MockSink sink, String resource, String sql, ProductFixture[] fixtures, int expectedHits)
            throws Exception {
        createData(sql, fixtures);
        context = createContext(resource);
        context.setSink(sink);
        source.fetch();
        assertEquals(sink.data().size(), expectedHits);
    }

    protected ColumnContext createContext(String resource) throws IOException {
        Settings settings = createSettings(resource);
        ColumnContext context = newContext();
        context.setSettings(settings);
        context.setLastRunTimeStamp(LAST_RUN_TIME);
        context.setLastRunTimeStampOverlap(getLastRunTimestampOverlap(settings));
        source.setContext(context);
        source.columnCreatedAt(settings.get("column_created_at"))
                .columnUpdatedAt(settings.get("column_updated_at"))
                .columnDeletedAt(settings.get("column_deleted_at"))
                .columnEscape(true);
        source.setStatements(SQLCommand.parse(settings.getAsStructuredMap()));
        return context;
    }

    private TimeValue getLastRunTimestampOverlap(Settings settings) {
        TimeValue overlap = TimeValue.timeValueMillis(0);
        if (settings != null && settings.getAsStructuredMap().containsKey("last_run_timestamp_overlap")) {
            overlap = settings.getAsTime("last_run_timestamp_overlap", null);
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
