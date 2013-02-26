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
package org.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.jdbc.RiverSource;
import org.elasticsearch.river.jdbc.support.RiverContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractRiverTest extends Assert {

    private static final ESLogger logger = Loggers.getLogger(AbstractRiverTest.class);

    protected static RiverSource source;

    protected static RiverContext context;

    public abstract RiverSource getRiverSource();

    public abstract RiverContext getRiverContext();

    @BeforeMethod
    @Parameters({"driver", "starturl", "user", "password"})
    public void createClient(String driver, String starturl, String user, String password)
            throws Exception {
        source = getRiverSource()
                .driver(driver)
                .url(starturl)
                .user(user)
                .password(password);
        context = getRiverContext()
                .riverSource(source)
                .retries(1)
                .maxRetryWait(TimeValue.timeValueSeconds(5))
                .locale("en");
        context.contextualize();
    }

    @BeforeMethod(dependsOnMethods = {"createClient"})
    @Parameters({"create"})
    public void createTable(@Optional String resourceName) throws Exception {
        if (resourceName == null || "".equals(resourceName)) {
            return;
        }
        Connection connection = source.connectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        sqlScript(connection, resourceName);
        source.closeWriting();
    }

    @AfterMethod
    @Parameters({"delete"})
    public void removeTable(@Optional String resourceName) throws Exception {
        if (resourceName == null || "".equals(resourceName)) {
            return;
        }
        // before dropping tables, open read connection must be closed to avoid hangs in mysql/postgresql
        logger.debug("closing reads...");
        source.closeReading();

        logger.debug("connecting for close...");
        Connection connection = source.connectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        logger.debug("cleaning...");
        // clean up tables
        sqlScript(connection, resourceName);
        logger.debug("closing writes...");
        source.closeWriting();
    }

    @AfterMethod(dependsOnMethods = {"removeTable"})
    @Parameters({"driver", "starturl", "stopurl", "user", "password"})
    public void removeClient(String driver, String starturl, String stopurl, String user, String password)
            throws Exception {

        // some driver can drop database by magic URL
        source = getRiverSource()
                .driver(driver)
                .url(stopurl)
                .user(user)
                .password(password);
        try {
            logger.debug("connecting to stop URL...");
            // activate stop URL
            source.connectionForWriting();
        } catch (Exception e) {
            // exception is expected, ignore
        }
        // close open write connection
        source.closeWriting();
        logger.debug("stopped");
    }

    protected RiverSettings riverSettings(String resource)
            throws IOException {
        InputStream in = getClass().getResourceAsStream(resource);
        return new RiverSettings(ImmutableSettings.settingsBuilder()
                .build(), XContentHelper.convertToMap(Streams.copyToByteArray(in), false).v2());
    }

    private void sqlScript(Connection connection, String resourceName) throws Exception {
        InputStream in = getClass().getResourceAsStream(resourceName);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        String sql;
        while ((sql = br.readLine()) != null) {
            try {
                logger.debug("executing {}", sql);
                Statement p = connection.createStatement();
                p.execute(sql);
                p.close();
            } catch (SQLException e) {
                // ignore
                logger.error(sql + " failed. Reason: " + e.getMessage());
            }
        }
        br.close();
    }

}
