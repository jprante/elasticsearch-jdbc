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
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;

public class StandardSinkTests extends AbstractSinkTest {

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    /**
     * Start the task and execute a simple star query
     *
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"task1", "sql1"})
    public void testOnce(String resource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        perform(resource);
        logger.info("success");
    }

    /**
     * Product table (star query)
     *
     * @param resource the resource
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"task2", "sql1"})
    public void testRandom(String resource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        perform(resource);
        assertHits("1", 104);
        logger.info("success");
    }

    /**
     * Product table
     *
     * @param resource the resource
     * @param sql           the SQL statement
     * @throws Exception if test fails
     */
    @Test
    @Parameters({"task3", "sql1"})
    public void testMaxrows(String resource, String sql) throws Exception {
        createRandomProducts(sql, 100);
        perform(resource);
        assertHits("1", 104);
        assertTimestampSort("1", 104);
        logger.info("success");
    }

}
