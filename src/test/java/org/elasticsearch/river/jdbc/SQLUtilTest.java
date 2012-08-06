package org.elasticsearch.river.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Give sql tools for test using SQL (HSQLDB)
 */
public class SQLUtilTest {

    public static Connection createConnection()throws SQLException, ClassNotFoundException{
        Class.forName("org.hsqldb.jdbcDriver");
        return DriverManager.getConnection("jdbc:hsqldb:mem:test", "SA", "");
    }

    public static void addDataTest(Connection conn,int id,String value,String date,Integer[] options)throws SQLException{
        conn.createStatement().execute("insert into car values(" + id + ",'" + value + "','" + date + "')");
        for(Integer opt : options ){
            conn.createStatement().execute("insert into car_opt_have values(" + opt + "," + id + ")");
        }
    }

    public static void addDataTest(Connection conn,int id,String value,String date,Integer[] options,Integer[] colors)throws SQLException{
        conn.createStatement().execute("insert into car values(" + id + ",'" + value + "','" + date + "')");
        for(Integer opt : options ){
            conn.createStatement().execute("insert into car_opt_have values(" + opt + "," + id + ")");
        }

        for(Integer color : colors ){
            conn.createStatement().execute("insert into car_color_have values(" + color + "," + id + ")");
        }
    }

    /* Create random data */
    public static void createRandomData(Connection connection,int idFrom,int size,int year,int month,int day)throws SQLException, ClassNotFoundException{
        boolean createConnection = false;
        if(connection == null){
            connection = createConnection();
            createConnection = true;
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        for(int i = 0 ; i < size ; i++){
            Calendar calendar = Calendar.getInstance();
            calendar.set(year,month,day);
            calendar.set(Calendar.MINUTE,(int)Math.round(Math.random()*10000));
            addDataTest(connection,idFrom + i+1,"car" + i,format.format(calendar.getTime()),new Integer[]{(i%5)+1});
        }
        if(createConnection){
            connection.close();
        }
    }

    /**
     * Truncate datas of table
     * @param conn
     * @param table
     * @throws SQLException
     */
    public static void truncateTable(Connection conn,String table)throws SQLException{
        conn.createStatement().execute("delete from " + table);
    }

}
