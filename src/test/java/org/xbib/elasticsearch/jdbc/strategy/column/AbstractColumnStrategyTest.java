package org.xbib.elasticsearch.jdbc.strategy.column;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.xbib.elasticsearch.jdbc.strategy.Context;
import org.xbib.elasticsearch.util.NodeTestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.TimeZone;

public abstract class AbstractColumnStrategyTest extends NodeTestUtils {

    protected static ColumnSource source;

    protected static ColumnContext context;

    public abstract ColumnSource newSource();

    public abstract ColumnContext newContext();

    @BeforeMethod
    @Parameters({"starturl", "user", "password", "create"})
    public void beforeMethod(String starturl, String user, String password, @Optional String resourceName)
            throws Exception {
        startNodes();
        logger.info("nodes started");
        source = newSource();
        source.setUrl(starturl)
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
        // before dropping tables, open read connection must be closed to avoid hangs in mysql/postgresql
        logger.debug("closing reads...");
        source.closeReading();

        logger.debug("connecting for close...");
        Connection connection = source.getConnectionForWriting();
        if (connection == null) {
            throw new IOException("no connection");
        }
        logger.debug("cleaning...");
        // clean up tables
        sqlScript(connection, resourceName);
        logger.debug("closing writes...");
        source.closeWriting();

        // we can drop database by a magic 'stop' URL
        source = newSource();
        source.setUrl(stopurl)
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
        // close open write connection
        source.closeWriting();
        logger.info("stopped");

        // delete test index
        try {
            client("1").admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            logger.info("index {} deleted", index);
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
        stopNodes();
    }

    protected Context createContext(String resource) throws Exception {
        //waitForYellow("1");
        InputStream in = getClass().getResourceAsStream(resource);
        Settings settings = createSettings(resource);
        Context context = newContext();
        context.setSettings(settings);
        //context.getSink().setIngestFactory(createIngestFactory(settings));
        logger.info("created context {} with cluster name {}", context, "elasticsearch");
        return context;
    }

    protected Settings createSettings(String resource)
            throws IOException {
        InputStream in = getClass().getResourceAsStream(resource);
        Settings settings = Settings.settingsBuilder()
                .loadFromStream("test", in)
                .put("jdbc.elasticsearch.cluster", "elasticsearch")
                .putArray("jdbc.elasticsearch.host", getHosts())
                .build()
                .getAsSettings("jdbc");
        in.close();
        return settings;
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
