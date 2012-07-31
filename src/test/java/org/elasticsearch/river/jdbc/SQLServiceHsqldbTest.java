package org.elasticsearch.river.jdbc;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.log4j.Log4jESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SQLServiceHsqldbTest {
    private final ESLogger logger = new Log4jESLogger("ES river", Logger.getLogger("Test"));
    private SQLService sqlService = new SQLService(logger);
    private Server server;
    private String complexQuery = "select 'index' as \"_operation\",car.modification_date as \"_modification_date\", id as _id, id as \"car.id\", label as \"car.label\", o.label as \"car.options[label]\"," +
            "            o.price as \"car.options[price]\"" +
            "            from car left join car_opt_have co on car.id = co.id_car" +
            "            left join opt o on o.id = co.id_opt";

    private String complexQuery2 = "select 'index' as \"_operation\",car.modification_date as \"_modification_date\", id as _id, id as \"id\", label as \"label\", o.label as \"options[label]\"," +
            "            o.price as \"options[price]\"" +
            "            from car left join car_opt_have co on car.id = co.id_car" +
            "            left join opt o on o.id = co.id_opt";

    private final String INDEX_NAME = "shop";

    @BeforeClass
    public void startHsqldbServer()throws ClassNotFoundException,SQLException{
        HsqlProperties p = new HsqlProperties();
        p.setProperty("server.database.0","mem:test");
        p.setProperty("server.dbname.0","test");


        server = new Server();
        server.setProperties(p);
        server.setLogWriter(null); // can use custom writer
        server.setErrWriter(null); // can use custom writer
        server.start();

        // Insert new data
        Connection conn = SQLUtilTest.createConnection();
        conn.createStatement().execute("create table car(id int,label varchar(255),modification_date timestamp )");
        conn.createStatement().execute("create table opt(id int,label varchar(255), price int)");
        conn.createStatement().execute("create table car_opt_have(id_opt int, id_car int )");

        conn.createStatement().execute("insert into opt values(1,'clim',1000)");
        conn.createStatement().execute("insert into opt values(2,'door',500)");
        conn.createStatement().execute("insert into opt values(3,'wheel',200)");
        conn.createStatement().execute("insert into opt values(4,'gearsystem',350)");
        conn.createStatement().execute("insert into opt values(5,'windows',120)");

        SQLUtilTest.addDataTest(conn, 1, "car1", "2012-06-02 10:00:00", new Integer[]{1, 2, 3, 4});
        SQLUtilTest.addDataTest(conn, 2, "car2", "2012-06-02 11:00:00", new Integer[]{1});
        SQLUtilTest.addDataTest(conn, 3, "car3", "2012-06-02 10:00:00", new Integer[]{1});
        SQLUtilTest.addDataTest(conn, 4, "car4", "2012-06-02 14:00:00", new Integer[]{1, 3});
        SQLUtilTest.addDataTest(conn, 5, "car5", "2012-06-02 15:00:00", new Integer[]{1});

        conn.close();
    }



    @AfterClass
    public void stopHsqldbServer()throws Exception{
        /* Drop database */
        Connection conn = SQLUtilTest.createConnection();
        conn.createStatement().execute("drop table car_opt_have");
        conn.createStatement().execute("drop table opt");
        conn.createStatement().execute("drop table car");
        server.stop();
    }


    @BeforeMethod
    public void resetIndex(){
        BulkOperation op = getMemoryBulkOperation(logger).setIndex(INDEX_NAME).setType("car");
        op.getClient().admin().indices().prepareDelete(INDEX_NAME).execute().actionGet();
    }

    @Test
    public void testConnexion()throws Exception{
        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        Assert.assertFalse(connection.isClosed());
    }


    @Test
    public void testMerger()throws Exception{
        String query = "select 'index' as \"_operation\", id as _id, id as \"car.id\", label as \"car.label\", o.label as \"car.options\"" +
                "            from car left join car_opt_have co on car.id = co.id_car" +
                "            left join opt o on o.id = co.id_opt";

        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        PreparedStatement ps = sqlService.prepareStatement(connection,query);
        sqlService.treat(ps,5,"index","_modification_date",getMockBulkOperation(logger).setIndex(INDEX_NAME).setType("car"));
    }

    @Test
    public void testComplexMerger()throws Exception{
        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        PreparedStatement ps = sqlService.prepareStatement(connection,complexQuery);
        sqlService.treat(ps,5,"index","_modification_date",getMockBulkOperation(logger).setIndex(INDEX_NAME).setType("car"));
    }


    @Test
    public void testComplexMergerInMemory()throws Exception{
        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        PreparedStatement ps = sqlService.prepareStatement(connection,complexQuery2);
        BulkOperation op = getMemoryBulkOperation(logger).setIndex(INDEX_NAME).setType("car");

        sqlService.treat(ps,5,"index","_modification_date",op);
        refreshIndex(op.getClient(),INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 5);

        // Add new entry to test if it's indexed
        Connection conn = SQLUtilTest.createConnection();
        SQLUtilTest.addDataTest(conn, 6, "car6", "2012-07-02 14:00:00", new Integer[]{3});
        conn.close();

        sqlService.treat(ps,5,"index","_modification_date",op);
        refreshIndex(op.getClient(),INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 6);
    }


    private void refreshIndex(Client client,String index){
        client.admin().indices().refresh(new RefreshRequest(index)).actionGet();
    }

    private BulkOperation getMockBulkOperation(ESLogger logger){
        return new BulkOperation(null,logger){
            @Override
            public void create(String index, String type, String id, long version, XContentBuilder builder) {
                try{
                    logger.info(" CREATE : " + this.index + "/" + this.type + "/" + id + " => " + builder.string());
                }catch(Exception e){
                    logger.error("Error generating CREATE");
                }
            }

            @Override
            public void index(String index, String type, String id, long version, XContentBuilder builder) {
                try{
                    logger.info(" INDEX: " + this.index + "/" + this.type + "/" + id + " => " + builder.string());
                }catch(Exception e){
                    logger.error("Error generating INDEX");
                }
            }

            @Override
            public void delete(String index, String type, String id) {
                logger.info(" DELETE: " + this.index + "/" + this.type + "/" + id);
            }
        };
    }

    protected static BulkOperation getMemoryBulkOperation(ESLogger logger){
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("gateway.type","none")
                .put("index.gateway.type", "none")
                .put("index.store.type", "memory")
                .put("path.data","target/data")
                .build();

        Node node = NodeBuilder.nodeBuilder().local(true).settings(settings).node();
        final Client client = node.client();


        return new BulkOperation(client,logger){
            @Override
            public BulkOperation setIndex(String index) {
                super.setIndex(index);
                // We create the index in memory if it does'nt exist
                if(!client.admin().indices().prepareExists(index).execute().actionGet().exists()){
                    client.admin().indices().prepareCreate(index).execute().actionGet();
                }
                return this;
            }

            @Override
            public void create(String index, String type, String id, long version, XContentBuilder builder) {
                try{
                    logger.info(" CREATE : " + this.index + "/" + this.type + "/" + id + " => " + builder.string());
                    client.prepareIndex(this.index, this.type).setSource(builder).execute().actionGet();
                }catch(Exception e){
                    logger.error("Error generating CREATE");
                }
            }

            @Override
            public void index(String index, String type, String id, long version, XContentBuilder builder) {
                try{
                    logger.info(" INDEX: " + this.index + "/" + this.type + "/" + id + " => " + builder.string());
                    IndexResponse r = client.prepareIndex(this.index,this.type,id).setSource(builder).execute().actionGet();
                }catch(Exception e){
                    logger.error("Error generating INDEX");
                }
            }

            @Override
            public void delete(String index, String type, String id) {
                logger.info(" DELETE: " + this.index + "/" + this.type + "/" + id);
                client.prepareDelete(this.index, this.type, id).execute().actionGet();
            }
        };
    }

}