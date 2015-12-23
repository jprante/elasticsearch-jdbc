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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.xbib.elasticsearch.common.util.LocaleUtil;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.jdbc.strategy.JDBCSource;
import org.xbib.elasticsearch.util.NodeTestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

public abstract class AbstractSinkTest extends NodeTestUtils {

    protected final static Logger logger = LogManager.getLogger("test.target");

    protected static JDBCSource source;

    protected Context context;

    public abstract JDBCSource newSource();

    public abstract Context newContext();

    @BeforeMethod
    @Parameters({"starturl", "user", "password", "create"})
    public void beforeMethod(String starturl, String user, String password, @Optional String resourceName)
            throws Exception {
        startNodes();
        logger.info("nodes started");
        source = newSource()
                .setUrl(starturl)
                .setUser(user)
                .setPassword(password)
                .setLocale(Locale.getDefault())
                .setTimeZone(TimeZone.getDefault());
        logger.info("create table {}", resourceName);
        if (resourceName == null || "".equals(resourceName)) {
            return;
        }
        Connection connection = source.getConnectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        sqlScript(connection, resourceName);
        source.closeWriting();
    }

    @AfterMethod
    @Parameters({"stopurl", "user", "password", "delete"})
    public void afterMethod(String stopurl, String user, String password, @Optional String resourceName)
            throws Exception {
        logger.info("remove table {}", resourceName);
        if (resourceName == null || "".equals(resourceName)) {
            return;
        }
        logger.debug("cleaning...");
        // clean up tables
        Connection connection = source.getConnectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        sqlScript(connection, resourceName);
        // before dropping tables, open read connection must be closed to avoid hangs in mysql/postgresql
        logger.debug("closing reads...");
        source.closeReading();
        // we can drop database by a magic 'stop' URL
        source = newSource()
                .setUrl(stopurl)
                .setUser(user)
                .setPassword(password)
                .setLocale(Locale.getDefault())
                .setTimeZone(TimeZone.getDefault());
        try {
            logger.info("connecting to stop URL...");
            // activate stop URL
            source.getConnectionForWriting();
        } catch (Exception e) {
            // exception is expected, ignore
        }
        source.closeWriting();
        logger.info("stopped");
        // delete test index
        try {
            client("1").admin().indices().prepareDelete(index).execute().actionGet();
            logger.info("index {} deleted", index);
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
        stopNodes();
    }

    protected void perform(String resource) throws Exception {
        // perform a single step
        logger.info("before execution");
        this.context = createContext(resource);
        logger.info("execution");
        context.execute();
        boolean b = waitFor(context, Context.State.IDLE, 5000L);
        logger.info("after execution: {}", b);
    }

    protected Context createContext(String resource) throws Exception {
        InputStream in = getClass().getResourceAsStream(resource);
        Settings settings = Settings.settingsBuilder()
                .put("jdbc.elasticsearch.cluster", "elasticsearch")
                .putArray("jdbc.elasticsearch.host", getHosts())
                .loadFromStream("test", in)
                .build()
                .getAsSettings("jdbc");
        Context context = newContext();
        context.setSettings(settings);
        return context;
    }

    protected void createRandomProducts(String sql, int size)
            throws SQLException {
        Connection connection = source.getConnectionForWriting();
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            add(connection, sql, UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        source.closeWriting();
    }

    protected void createTimestampedLogs(String sql, int size, String locale, String timezone)
            throws SQLException {
        Connection connection = source.getConnectionForWriting();
        Locale l = LocaleUtil.toLocale(locale);
        TimeZone t = TimeZone.getTimeZone(timezone);
        source.setTimeZone(t).setLocale(l);
        Calendar cal = Calendar.getInstance(t, l);
        // half of log in the past, half of it in the future
        cal.add(Calendar.HOUR, -(size / 2));
        for (int i = 0; i < size; i++) {
            Timestamp modified = new Timestamp(cal.getTimeInMillis());
            String message = "Hello world";
            add(connection, sql, modified, message);
            cal.add(Calendar.HOUR, 1);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        source.closeWriting();
    }

    private void add(Connection connection, String sql, final String name, final long amount, final double price)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList<Object>() {{
            add(name);
            add(amount);
            add(price);
        }};
        source.bind(stmt, params);
        stmt.execute();
    }

    private void add(Connection connection, String sql, final Timestamp ts, final String message)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList<Object>() {{
            add(ts);
            add(message);
        }};
        source.bind(stmt, params);
        stmt.execute();
    }

    protected void createRandomProductsJob(String sql, int size)
            throws SQLException {
        Connection connection = source.getConnectionForWriting();
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            long job = 0L;
            add(connection, sql, job, UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        source.closeWriting();
    }

    private void add(Connection connection, String sql, final long job, final String name, final long amount, final double price)
            throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(sql);
        List<Object> params = new ArrayList<Object>() {
            {
                add(job);
                add(name);
                add(amount);
                add(price);
            }
        };
        source.bind(stmt, params);
        stmt.execute();
    }

    private void sqlScript(Connection connection, String resourceName) throws Exception {
        InputStream in = getClass().getResourceAsStream(resourceName);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        String sql;
        while ((sql = br.readLine()) != null) {

            try {
                logger.trace("executing {}", sql);
                Statement p = connection.createStatement();
                p.execute(sql);
                p.close();
            } catch (SQLException e) {
                // ignore
                logger.error(sql + " failed. Reason: " + e.getMessage());
            } finally {
                connection.commit();
            }
        }
        br.close();
    }

    public boolean waitFor(Context context, Context.State state, long millis) throws InterruptedException {
        long t0 = System.currentTimeMillis();
        boolean found;
        do {
            found = state == context.getState();
            if (!found) {
                Thread.sleep(100L);
            }
        } while (!found && System.currentTimeMillis() - t0 < millis);
        return found;
    }
}
