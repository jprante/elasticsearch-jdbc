package org.elasticsearch.river.jdbc;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.testng.Assert.assertTrue;

public class JDBCRiverTest {

    private JDBCRiver river;


    @Test
    public void defaultSettings() throws Exception {
        // Given
        String riverName = "my_jdbc_river";
        String riverIndexName = "_river";
        String indexName = "jdbc";
        String typeName = "jdbc";
        String url = null;
        String driver = null;
        String user = null;
        String password = null;
        String sql = null;
        String param = null;
        int bulkSize = 100;
        int maxBulkRequests = 30;
        int fetchsize = 0;
        String bulkTimeout = "1m";
        String poll = "1h";
        String interval = "1h";
        boolean rivertable = false;
        boolean versioning = true;
        String rounding = null;
        int scale = 0;
        RiverName rn = new RiverName("jdbc", riverName);
        final RiverSettings rs = new RiverSettings(settingsBuilder().build(), new HashMap<String, Object>());

        // When
        river = new JDBCRiver(rn, rs, riverIndexName, null);
        final String riverSettings = river.toString();

        // Then
        System.out.println("riverSettings = " + riverSettings);
        assertTrue(riverSettings.contains("riverIndexName=" + riverIndexName));
        assertTrue(riverSettings.contains("indexName=" + indexName));
        assertTrue(riverSettings.contains("typeName=" + typeName));
        assertTrue(riverSettings.contains("url=" + url));
        assertTrue(riverSettings.contains("driver=" + driver));
        assertTrue(riverSettings.contains("user=" + user));
        assertTrue(riverSettings.contains("password=" + password));
        assertTrue(riverSettings.contains("sql=" + sql));
        assertTrue(riverSettings.contains("params=" + param));
        assertTrue(riverSettings.contains("bulkSize=" + bulkSize));
        assertTrue(riverSettings.contains("maxBulkRequests=" + maxBulkRequests));
        assertTrue(riverSettings.contains("fetchsize=" + fetchsize));
        assertTrue(riverSettings.contains("bulkTimeout=" + bulkTimeout));
        assertTrue(riverSettings.contains("poll=" + poll));
        assertTrue(riverSettings.contains("interval=" + interval));
        assertTrue(riverSettings.contains("versioning=" + versioning));
        assertTrue(riverSettings.contains("rounding=" + rounding));
        assertTrue(riverSettings.contains("scale=" + scale));
        assertTrue(riverSettings.contains("rivertable=" + rivertable));
    }

    @Test
    public void allSettingsSet() throws Exception {
        // Given
        String riverName = "my_jdbc_river";
        String riverIndexName = "_river";
        String indexName = "test";
        String typeName = "test";
        String url = "jdbc:mysql://localhost:3306/test";
        String driver = "com.mysql.jdbc.Driver";
        String user = "test";
        String password = "test";
        String sql = "select * from orders where order.name = ?";
        String param = "test_param";
        int bulkSize = 4;
        int maxBulkRequests = 5;
        int fetchsize = 6;
        String bulkTimeout = "2h";
        String poll = "3h";
        String interval = "4h";
        boolean versioning = false;
        String rounding = "ceiling";
        int scale = 4;
        boolean rivertable = true;

        XContentBuilder xb = XContentFactory.jsonBuilder()
                .startObject()
                .field("type", "jdbc")
                .startObject("jdbc")
                .field("driver", driver)
                .field("url", url)
                .field("user", user)
                .field("password", password)
                .field("sql", sql)
                .field("params", param)
                .field("poll", poll)
                .field("versioning", versioning)
                .field("rounding", rounding)
                .field("scale", scale)
                .field("fetchsize", fetchsize)
                .field("rivertable", rivertable)
                .field("interval", interval)
                .endObject()
                .startObject("index")
                .field("index", indexName)
                .field("type", typeName)
                .field("bulk_size", bulkSize)
                .field("max_bulk_requests", maxBulkRequests)
                .field("bulk_timeout", bulkTimeout)
                .endObject()
                .endObject();

        final byte[] content = xb.copiedBytes();
        final Map<String, Object> settings = XContentHelper.convertToMap(content, 0, content.length, false).v2();
        RiverName rn = new RiverName("jdbc", riverName);
        final RiverSettings rs = new RiverSettings(settingsBuilder().build(), settings);

        // When
        river = new JDBCRiver(rn, rs, riverIndexName, null);
        final String riverSettings = river.toString();

        // Then
        System.out.println("riverSettings = " + riverSettings);
        assertTrue(riverSettings.contains("riverIndexName=" + riverIndexName));
        assertTrue(riverSettings.contains("indexName=" + indexName));
        assertTrue(riverSettings.contains("typeName=" + typeName));
        assertTrue(riverSettings.contains("url=" + url));
        assertTrue(riverSettings.contains("driver=" + driver));
        assertTrue(riverSettings.contains("user=" + user));
        assertTrue(riverSettings.contains("password=" + password.replaceAll(".", "\\*")));
        assertTrue(riverSettings.contains("sql=" + sql));
        assertTrue(riverSettings.contains("params=[" + param + "]"));
        assertTrue(riverSettings.contains("bulkSize=" + bulkSize));
        assertTrue(riverSettings.contains("maxBulkRequests=" + maxBulkRequests));
        assertTrue(riverSettings.contains("fetchsize=" + fetchsize));
        assertTrue(riverSettings.contains("bulkTimeout=" + bulkTimeout));
        assertTrue(riverSettings.contains("poll=" + poll));
        assertTrue(riverSettings.contains("interval=" + interval));
        assertTrue(riverSettings.contains("versioning=" + versioning));
        assertTrue(riverSettings.contains("rounding=" + rounding));
        assertTrue(riverSettings.contains("scale=" + scale));
        assertTrue(riverSettings.contains("rivertable=" + rivertable));
    }

}
