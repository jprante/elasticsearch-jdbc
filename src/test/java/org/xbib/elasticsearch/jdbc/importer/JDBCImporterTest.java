package org.xbib.elasticsearch.jdbc.importer;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.Test;
import org.xbib.tools.JDBCImporter;

public class JDBCImporterTest {

    public void testImporter() throws Exception {
        final JDBCImporter importer = JDBCImporter.getInstance();
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("url", "jdbc:mysql://localhost:3306/test")
                .put("password", "")
                .put("sql", "select * from test")
                .put("index", "jdbc")
                .put("type", "jdbc")
                .build();
        importer.setSettings(settings);
        importer.run();
        Thread.sleep(12000L);
        importer.shutdown();
    }
}
