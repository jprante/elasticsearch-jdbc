package org.elasticsearch.river.jdbc;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.log4j.Log4jESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/* Acceptance test of JDBCRiver with memory database and memory ES */
public class JDBCRiverTest {
    private final ESLogger logger = new Log4jESLogger("ES river", Logger.getLogger("Test"));
    private SQLService sqlService = new SQLService(logger);
    private Server server;
    private String mandatoryQuery = "select 'index' as \"_operation\", id as _id, id as \"id\"" +
            "            from car ";
    private String complexQuery2 = "select 'index' as \"_operation\", id as \"_id\", id as \"id\", label as \"label\", o.label as \"options[label]\"," +
            "            o.price as \"options[price]\", modification_date as \"_modification_date\"" +
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
        Class.forName("org.hsqldb.jdbcDriver");
        Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:test", "SA", "");
        conn.createStatement().execute("create table car(id int,label varchar(255),modification_date timestamp )");
        conn.createStatement().execute("create table opt(id int,label varchar(255), price int)");
        conn.createStatement().execute("create table car_opt_have(id_opt int, id_car int )");

        conn.createStatement().execute("insert into car values(1,'car1','2012-06-02 10:00:02')");
        conn.createStatement().execute("insert into car values(2,'car2','2012-06-02 11:00:04')");
        conn.createStatement().execute("insert into car values(3,'car3','2012-06-02 14:22:05')");
        conn.createStatement().execute("insert into car values(4,'car4','2012-06-02 10:30:03')");
        conn.createStatement().execute("insert into car values(5,'car5','2012-06-02 09:00:01')");

        conn.createStatement().execute("insert into opt values(1,'clim',1000)");
        conn.createStatement().execute("insert into opt values(2,'door',500)");
        conn.createStatement().execute("insert into opt values(3,'wheel',200)");
        conn.createStatement().execute("insert into opt values(4,'gearsystem',350)");
        conn.createStatement().execute("insert into opt values(5,'windows',120)");

        conn.createStatement().execute("insert into car_opt_have values(1,1)");
        conn.createStatement().execute("insert into car_opt_have values(2,1)");
        conn.createStatement().execute("insert into car_opt_have values(3,1)");
        conn.createStatement().execute("insert into car_opt_have values(4,1)");
        conn.createStatement().execute("insert into car_opt_have values(1,2)");
        conn.createStatement().execute("insert into car_opt_have values(1,3)");
        conn.createStatement().execute("insert into car_opt_have values(1,4)");
        conn.createStatement().execute("insert into car_opt_have values(3,4)");
        conn.createStatement().execute("insert into car_opt_have values(1,5)");

        conn.close();
    }

    @AfterClass
    public void stopHsqldbServer()throws Exception{
        Class.forName("org.hsqldb.jdbcDriver");
        Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:test", "SA", "");
        conn.createStatement().execute("drop table car_opt_have");
        conn.createStatement().execute("drop table opt");
        conn.createStatement().execute("drop table car");
        server.stop();
    }

    @BeforeMethod
    public void resetIndex(){
        BulkOperation op = SQLServiceHsqldbTest.getMemoryBulkOperation(logger).setIndex(INDEX_NAME).setType("car");
        op.getClient().admin().indices().prepareDelete(INDEX_NAME).execute().actionGet();
    }

    @Test
    public void testSettings(){
        BulkOperation op = SQLServiceHsqldbTest.getMemoryBulkOperation(logger).setIndex(INDEX_NAME).setType("car");
        Map<String, Object> map = new HashMap<String, Object>();
        Map<String, Object> jdbc = new HashMap<String, Object>();
        map.put("jdbc",jdbc);
        jdbc.put("user","SA");
        RiverSettings settings = new RiverSettings(ImmutableSettings.settingsBuilder().build(),map);
        JDBCRiver river = new JDBCRiver(new RiverName("type_test","type_test"),settings,"river_test",op.getClient());
        Assert.assertNotNull(river);
    }

    private RiverSettings createSettings(){


        Map<String, Object> map = new HashMap<String, Object>();
        Map<String, Object> jdbc = new HashMap<String, Object>();
        Map<String, Object> index = new HashMap<String, Object>();
        map.put("jdbc",jdbc);
        map.put("index",index);
        jdbc.put("user","SA");
        jdbc.put("password","");
        jdbc.put("driver","org.hsqldb.jdbcDriver");
        jdbc.put("url","jdbc:hsqldb:mem:test");
        jdbc.put("sql",complexQuery2);
        jdbc.put("aliasDateField","_modification_date");
        jdbc.put("fetchsize",4);
        jdbc.put("strategy","timebasis");
        index.put("index","shop");
        index.put("type","car");

        return new RiverSettings(ImmutableSettings.settingsBuilder().build(),map);
    }

    @Test
    public void testMandatoryField(){
        BulkOperation op = SQLServiceHsqldbTest.getMemoryBulkOperation(logger).setIndex(INDEX_NAME).setType("car");

        RiverSettings settings = createSettings();
        ((Map<String,Object>)settings.settings().get("jdbc")).put("sql",mandatoryQuery);
        JDBCRiver river = new JDBCRiver(new RiverName("river_test","river_test"),settings,"_river",op.getClient());
        river.riverStrategy.run();

    }


    @Test
    public void testComplexMergerInMemory()throws Exception{
        BulkOperation op = SQLServiceHsqldbTest.getMemoryBulkOperation(logger).setIndex(INDEX_NAME).setType("car");
        // Creation of working index
        op.getClient().admin().indices().prepareCreate("_river").execute().actionGet();

        RiverSettings settings = createSettings();
        JDBCRiver river = new JDBCRiver(new RiverName("river_test","river_test"),settings,"_river",op.getClient());
        river.delay = false;
        river.riverStrategy.run();
        refreshIndex(op.getClient(),INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 4);

        // TODO : date problem with GMT
        //Assert.assertEquals(((Map<String, Object>)op.getClient().prepareGet("_river","river_test","_custom").execute().actionGet().sourceAsMap().get("jdbc")).get("lastDateModification"),"2012-06-02 11:00:04");

        river.riverStrategy.run();
        refreshIndex(op.getClient(),INDEX_NAME);
        Assert.assertEquals(op.getClient().prepareSearch(INDEX_NAME).execute().actionGet().getHits().getTotalHits(), 5);


    }


    private void refreshIndex(Client client,String index){
        client.admin().indices().refresh(new RefreshRequest(index)).actionGet();
    }

}
