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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SQLServiceHsqldbTest {
    private final ESLogger logger = new Log4jESLogger("ES river", Logger.getLogger("Test"));
    private SQLService sqlService = new SQLService(logger);
    private Server server;
    private List<Object> mappingComplexQuery = ESUtilTest.createMapping("_operation","_modification_date","_id","car.id","car.label","car.options[label]","car.options[price]");
    private String complexQuery = "select 'index' as \"_operation\",car.modification_date as \"_modification_date\", id as _id, id, label, o.label, o.price" +
            "            from car left join car_opt_have co on car.id = co.id_car" +
            "            left join opt o on o.id = co.id_opt";
    private List<Object> mappingComplexQuery2 = ESUtilTest.createMapping("_operation","_modification_date","_id","id","label","options[label]","options[price]");
    private String complexQuery2 = "select 'index' as \"_operation\",car.modification_date as \"_modification_date\", id as _id, id, label , o.label,o.price" +
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
        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(INDEX_NAME).setType("car");
        op.getClient().admin().indices().prepareDelete(INDEX_NAME).execute().actionGet();
    }

    @Test
    public void testConnexion()throws Exception{
        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        Assert.assertFalse(connection.isClosed());
    }

    @Test
    public void testMerger()throws Exception{
        String query = "select 'index' as \"_operation\", id as _id, id, label, o.label " +
                "            from car left join car_opt_have co on car.id = co.id_car" +
                "            left join opt o on o.id = co.id_opt";
        List<Object> mapping = ESUtilTest.createMapping("_operation","_id","car.id","car.label","car.options");
        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        PreparedStatement ps = sqlService.prepareStatement(connection,query);
        sqlService.treat(ps,5,"index",ESUtilTest.getMockBulkOperation(logger).setIndex(INDEX_NAME).setType("car"),mapping);
    }

    @Test
    public void testComplexMerger()throws Exception{
        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        PreparedStatement ps = sqlService.prepareStatement(connection,complexQuery);
        
        sqlService.treat(ps,5,"index",ESUtilTest.getMockBulkOperation(logger).setIndex(INDEX_NAME).setType("car"),mappingComplexQuery);
    }


    @Test
    public void testComplexMergerInMemory()throws Exception{
        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        PreparedStatement ps = sqlService.prepareStatement(connection,complexQuery2);
        BulkOperation op = ESUtilTest.getMemoryBulkOperation(logger).setIndex(INDEX_NAME).setType("car");

        sqlService.treat(ps,5,"index",op,mappingComplexQuery2);
        refreshIndex(op.getClient(),INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 5);

        // Add new entry to test if it's indexed
        Connection conn = SQLUtilTest.createConnection();
        SQLUtilTest.addDataTest(conn, 6, "car6", "2012-07-02 14:00:00", new Integer[]{3});
        conn.close();

        sqlService.treat(ps,5,"index",op,mappingComplexQuery2);
        refreshIndex(op.getClient(),INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 6);
    }


    private void refreshIndex(Client client,String index){
        client.admin().indices().refresh(new RefreshRequest(index)).actionGet();
    }

    

}
