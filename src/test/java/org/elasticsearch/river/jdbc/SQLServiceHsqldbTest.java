package org.elasticsearch.river.jdbc;

import org.apache.log4j.Logger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.log4j.Log4jESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class SQLServiceHsqldbTest {
    private final ESLogger logger = new Log4jESLogger("ES river", Logger.getLogger("Test"));
    private SQLService sqlService = new SQLService(logger);
    private Server server;

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
        conn.createStatement().execute("create table car(id int,label varchar(255) )");
        conn.createStatement().execute("create table opt(id int,label varchar(255), price int)");
        conn.createStatement().execute("create table car_opt_have(id_opt int, id_car int )");

        conn.createStatement().execute("insert into car values(1,'car1')");
        conn.createStatement().execute("insert into car values(2,'car2')");
        conn.createStatement().execute("insert into car values(3,'car3')");
        conn.createStatement().execute("insert into car values(4,'car4')");
        conn.createStatement().execute("insert into car values(5,'car5')");

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
    public void stopHsqldbServer(){
        server.stop();
    }

    @Test
    public void testRequest()throws Exception{
        
    }

    @Test
    public void testConnexion()throws Exception{
        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        Assert.assertFalse(connection.isClosed());
    }

    @Test
    public void testTable()throws Exception{
        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        PreparedStatement ps = sqlService.prepareStatement(connection,"select * from test");
        ResultSet rs = sqlService.execute(ps,100);
        Assert.assertTrue(rs.next());

    }

    @Test
    public void testMerger()throws Exception{
        String query = "select 'index' as \"_operation\", id as _id, id as \"car.id\", label as \"car.label\", o.label as \"car.options\"" +
                "            from car left join car_opt_have co on car.id = co.id_car" +
                "            left join opt o on o.id = co.id_opt";

        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        PreparedStatement ps = sqlService.prepareStatement(connection,query);
        sqlService.treat(ps,5,"index",null);
    }

    @Test
    public void testComplexMerger()throws Exception{
        String query = "select 'index' as \"_operation\", id as _id, id as \"car.id\", label as \"car.label\", o.label as \"car.options[label]\"," +
                "            o.price as \"car.options[price]\"" +
                "            from car left join car_opt_have co on car.id = co.id_car" +
                "            left join opt o on o.id = co.id_opt";

        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        PreparedStatement ps = sqlService.prepareStatement(connection,query);
        sqlService.treat(ps,5,"index",getMockBulkOperation(logger).setIndex("shop").setType("car"));
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

}
