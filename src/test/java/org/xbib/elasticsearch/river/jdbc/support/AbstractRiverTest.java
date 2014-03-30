
package org.xbib.elasticsearch.river.jdbc.support;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.river.RiverSettings;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
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

    private static final ESLogger logger = ESLoggerFactory.getLogger(AbstractRiverTest.class.getName());

    protected static RiverSource source;

    protected static RiverContext context;

    public abstract RiverSource getRiverSource();

    public abstract RiverContext getRiverContext();

    @BeforeMethod
    @Parameters({"driver", "starturl", "user", "password"})
    public void createClient(String driver, String starturl, String user, String password)
            throws Exception {
        source = getRiverSource()
                .url(starturl)
                .user(user)
                .password(password);
        context = getRiverContext()
                .riverSource(source)
                .setRetries(1)
                .setMaxRetryWait(TimeValue.timeValueSeconds(5))
                .setLocale("en");
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
        // some driver can drop database by a magic 'stop' URL
        source = getRiverSource()
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

}
