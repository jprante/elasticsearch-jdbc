package org.elasticsearch.river.jdbc;

import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

public class SQLServiceHsqldbTest {
    private SQLService sqlService = new SQLService();
    private Server server;

    @BeforeClass
    public void startHsqldbServer(){
        HsqlProperties p = new HsqlProperties();
        p.setProperty("server.database.0","mem:test");
        p.setProperty("server.dbname.0","test");
        


        server = new Server();
        server.setProperties(p);
        server.setLogWriter(null); // can use custom writer
        server.setErrWriter(null); // can use custom writer
        server.start();
        // Insert new data

    }

    @AfterClass
    public void stopHsqldbServer(){
        server.stop();
    }

    @Test
    public void testConnexion()throws Exception{
        Connection connection = sqlService.getConnection("oracle.jdbc.driver.OracleDriver","jdbc:oracle:thin:@cebdd:1521:CADREMP","CADREMP","CADREMP",true);
        Assert.assertNotNull(connection,"Test connection");
        connection.close();
    }

    @Test
    public void testRequest()throws Exception{
        Connection connection = sqlService.getConnection("oracle.jdbc.driver.OracleDriver","jdbc:oracle:thin:@cebdd:1521:CADREMP","CADREMP","CADREMP",true);

        PreparedStatement statement = sqlService.prepareStatement(connection,"select * from mission");
        ResultSet results = sqlService.execute(statement,100);
        Assert.assertNotNull(results);
        Assert.assertTrue(results.next());

        connection.close();
    }

    @Test
    public void testHsqldbConnexion()throws Exception{
        Connection connection = sqlService.getConnection("org.hsqldb.jdbcDriver","jdbc:hsqldb:mem:test", "SA", "",true);
        Assert.assertFalse(connection.isClosed());
    }


}
