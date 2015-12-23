/*
 * Copyright (C) 2015 Jörg Prante
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
package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.tools.JDBCImporter;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;

public class StandardScheduleTests extends AbstractSinkTest {

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    /**
     * Product table star select, scheduled for more than two runs
     *
     * @param resource the resource
     * @param sql           the SQL statement
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"task6", "sql1"})
    public void testSimpleSchedule(String resource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        JDBCImporter importer = createImporter(resource);
        Thread.sleep(12500L); // run more than twice
        importer.shutdown();
        long hits = client("1").prepareSearch(index).execute().actionGet().getHits().getTotalHits();
        logger.info("found {} hits", hits);
        assertTrue(hits > 104L);
    }

    /**
     * Test read and write of timestamps in a table. We create 100 timestamps over hour interval,
     * current timestamp $now is in the center.
     * Selecting timestamps from $now, there should be at least 50 rows/hits per run, if $now works.
     *
     * @param resource the JSON resource
     * @param sql           the sql statement to select timestamps
     * @throws Exception
     */
    @Test
    @Parameters({"task7", "sql2"})
    public void testTimestamps(String resource, String sql) throws Exception {
        // TODO make timezone/locale configurable for better randomized testing
        createTimestampedLogs(sql, 100, "iw_IL", "Asia/Jerusalem");
        JDBCImporter importer = createImporter(resource);
        Thread.sleep(12500L); // run more than twice
        importer.shutdown();
        long hits = client("1").prepareSearch(index).execute().actionGet().getHits().getTotalHits();
        // just an estimation, at least two runs should deliver 50 hits each.
        logger.info("found {} hits", hits);
        assertTrue(hits > 99L);
    }

    private JDBCImporter createImporter(final String resource) throws Exception {
        final JDBCImporter importer = new JDBCImporter();
        Context context = createContext(resource);
        logger.info("createImporter: setting context {}", context);
        importer.setContext(context);
        logger.info("createImporter: settings = {}", context.getSettings());
        // dispatch in a thread
        new Thread() {
            public void run() {
                importer.run();
            }
        }.start();
        return importer;
    }

}
