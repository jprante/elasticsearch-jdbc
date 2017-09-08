package org.xbib.jdbc.importer;

import org.elasticsearch.common.settings.Settings;
import org.xbib.jdbc.JdbcPipeline;

public class JDBCImporterTest {

    public void testImporter() throws Exception {
        final JdbcPipeline importer = new JdbcPipeline();
        Settings settings = Settings.settingsBuilder()
                .put("url", "jdbc:mysql://localhost:3306/test")
                //.put("password", "")
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
