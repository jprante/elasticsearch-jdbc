package org.xbib.elasticsearch.support.helper;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.river.RiverSettings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.support.river.RiverHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class AbstractRiverNodeTest extends AbstractNodeTestHelper {

    protected static RiverSource source;

    protected static RiverContext context;

    public abstract RiverSource getRiverSource();

    public abstract RiverContext getRiverContext();

    @BeforeMethod
    @Parameters({"starturl", "user", "password"})
    public void createClient(String starturl, String user, String password)
            throws Exception {
        source = getRiverSource()
                .setUrl(starturl)
                .setUser(user)
                .setPassword(password);
        context = getRiverContext()
                .setRiverSource(source)
                .setRetries(1)
                .setMaxRetryWait(TimeValue.timeValueSeconds(5))
                .setLocale("en");
        context.contextualize();
    }

    @BeforeMethod(dependsOnMethods = {"createClient"})
    @Parameters({"create"})
    public void createTable(@Optional String resourceName) throws Exception {
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
    @Parameters({"delete"})
    public void removeTable(@Optional String resourceName) throws Exception {
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
    }

    @AfterMethod(dependsOnMethods = {"removeTable"})
    @Parameters({"starturl", "stopurl", "user", "password"})
    public void removeClient(String starturl, String stopurl, String user, String password)
            throws Exception {
        // some driver can drop database by a magic 'stop' URL
        source = getRiverSource()
                .setUrl(stopurl)
                .setUser(user)
                .setPassword(password);
        try {
            logger.debug("connecting to stop URL...");
            // activate stop URL
            source.getConnectionForWriting();
        } catch (Exception e) {
            // exception is expected, ignore
        }
        // close open write connection
        source.closeWriting();
        logger.debug("stopped");
    }

    @BeforeMethod(dependsOnMethods = {"startNodes"})
    public void createRiver() throws Exception {
        startNode("1");
        logger.info("node started");
        waitForYellow("1");
        try {
            // create river index
            client("1").admin().indices().create(new CreateIndexRequest("_river")).actionGet();
            logger.info("river index created");
        } catch (IndexAlreadyExistsException e) {
            logger.warn(e.getMessage());
        }
        // do not create index, river must do this
        /*try {
            client("1").admin().indices().create(new CreateIndexRequest(index)).actionGet();
            logger.info("index {} created", index);
        } catch (IndexAlreadyExistsException e) {
            logger.warn(e.getMessage());
        }*/
    }

    @AfterMethod
    public void deleteRiver() throws Exception {
        try {
            client("1").admin().indices().deleteMapping(new DeleteMappingRequest()
                    .indices(new String[]{"_river"}).types("my_jdbc_river")).actionGet();
            logger.info("river deleted");
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
        // delete test index
        try {
            client("1").admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            logger.info("index {} deleted", index);
        } catch (IndexMissingException e) {
            logger.warn(e.getMessage());
        }
    }

    protected void createRiver(String resource) {
        try {
            waitForYellow("1");
            Map<String, Object> map = XContentHelper.convertToMap(Streams.copyToByteArray(getClass().getResourceAsStream(resource)), false).v2();
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("type", "jdbc")
                    .field("jdbc", map.get("jdbc"))
                    .endObject();
            logger.debug("river spec = {}", builder.string());
            IndexRequest indexRequest = Requests.indexRequest("_river").type("my_jdbc_river").id("_meta")
                    .source(builder.string());
            client("1").index(indexRequest).actionGet();
            client("1").admin().indices().prepareRefresh("_river").execute().actionGet();
            logger.debug("river created");
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    protected void waitForRiverEnabled() throws InterruptedException, IOException {
        RiverHelper.waitForRiverEnabled(client("1"), "my_jdbc_river", 15);
        logger.info("river enabled");
    }

    protected void waitForInactiveRiver() throws InterruptedException, IOException {
        RiverHelper.waitForInactiveRiver(client("1"), "my_jdbc_river", 30);
        logger.info("river inactive");
    }

    protected RiverSettings riverSettings(String resource)
            throws IOException {
        InputStream in = getClass().getResourceAsStream(resource);
        return new RiverSettings(ImmutableSettings.settingsBuilder().build(),
                XContentHelper.convertToMap(Streams.copyToByteArray(in), false).v2());
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

    protected void createRandomProductsJob(String sql, int size)
            throws SQLException {
        Connection connection = source.getConnectionForWriting();
        for (int i = 0; i < size; i++) {
            long amount = Math.round(Math.random() * 1000);
            double price = (Math.random() * 10000) / 100.00;
            add(connection, sql, "1", UUID.randomUUID().toString().substring(0, 32), amount, price);
        }
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
        source.closeWriting();
    }

    private void add(Connection connection, String sql, final String job, final String name, final long amount, final double price)
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

}
