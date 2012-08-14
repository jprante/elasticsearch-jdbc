package org.elasticsearch.river.jdbc;

import org.h2.tools.Server;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

public class SQLServiceH2Test {
    private SQLService sqlService = new SQLService();
    private Server server;

    @BeforeClass
    public void startHsqldbServer()throws SQLException, ClassNotFoundException{
        server = Server.createTcpServer().start();
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem","SA","");

        try{
            conn.createStatement().execute("drop table test");
        }catch(Exception e){}
        conn.createStatement().execute("create table test(id int,label varchar(255) )");
        conn.createStatement().execute("insert into test values(1,'test1')");
        conn.close();

    }

    @AfterClass
    public void stopHsqldbServer(){
        server.stop();
    }


    @Test
    public void testConnexion()throws Exception{
        Connection connection = sqlService.getConnection("org.h2.Driver","jdbc:h2:mem", "SA", "",true);
        Assert.assertFalse(connection.isClosed());
    }

    @Test
    public void testTable()throws Exception{
        Connection connection = sqlService.getConnection("org.h2.Driver","jdbc:h2:mem", "SA", "",true);
        PreparedStatement ps = sqlService.prepareStatement(connection,"select * from test");
        ResultSet rs = sqlService.execute(ps,100);
        Assert.assertTrue(rs.next());
        Assert.assertFalse(rs.next());
        
    }


}
