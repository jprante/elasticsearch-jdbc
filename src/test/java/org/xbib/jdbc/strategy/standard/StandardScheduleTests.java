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
package org.xbib.jdbc.strategy.standard;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.xbib.jdbc.strategy.Context;
import org.xbib.jdbc.strategy.JDBCSource;
import org.xbib.jdbc.JDBCImporter;

public class StandardScheduleTests extends AbstractSinkTest {

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    @Test
    @Parameters({"task8"})
    public void testInteval(String resource) throws Exception {
        JDBCImporter importer = createImporter(resource);
        Thread.sleep(15000L); // run more than twice
        importer.shutdown();
        long hits = client("1").prepareSearch(index).execute().actionGet().getHits().getTotalHits();
        // just an estimation, at least two runs should deliver 50 hits each.
        logger.info("found {} hits", hits);
        assertTrue(hits > 4L);
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
