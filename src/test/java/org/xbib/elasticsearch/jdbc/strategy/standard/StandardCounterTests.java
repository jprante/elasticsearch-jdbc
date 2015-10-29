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

import java.sql.Connection;
import java.sql.ResultSet;

public class StandardCounterTests extends AbstractSinkTest {

    @Override
    public JDBCSource newSource() {
        return new StandardSource();
    }

    @Override
    public StandardContext newContext() {
        return new StandardContext();
    }

    protected void perform(String resource) throws Exception {
        // perform a single step
        logger.info("before execution, resetting counter");
        source.getMetric().setCounter(0);
        this.context = createContext(resource);
        logger.info("execution");
        context.execute();
        boolean b = waitFor(context, Context.State.IDLE, 5000L);
        logger.info("after execution: {}", b);
    }

    @Test
    @Parameters({"task1", "sql1", "sql2"})
    public void testCounter(String resource, String sql1, String sql2)
            throws Exception {
        createRandomProductsJob(sql2, 100);
        Connection connection = source.getConnectionForReading();
        ResultSet results = connection.createStatement().executeQuery(sql1);
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        int count = results.next() ? results.getInt(1) : -1;
        source.close(results);
        source.closeReading();
        assertEquals(count, 100);
        perform(resource);
        assertHits("1", 100);
        // count docs in source table, must be null
        connection = source.getConnectionForReading();
        // sql1 = select count(*)
        results = connection.createStatement().executeQuery(sql1);
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        count = results.next() ? results.getInt(1) : -1;
        results.close();
        assertEquals(count, 0);
    }
}