package org.xbib.elasticsearch.river.jdbc.strategy.simple.storedprocedure;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class StoredProcedureJavaDBSample {

    public static void showSuppliers(ResultSet[] rs)
            throws SQLException {

        Connection con = DriverManager.getConnection("jdbc:default:connection");
        Statement stmt = null;

        String query =
                "select SUPPLIERS.SUP_NAME, " +
                        "COFFEES.COF_NAME " +
                        "from SUPPLIERS, COFFEES " +
                        "where SUPPLIERS.SUP_ID = " +
                        "COFFEES.SUP_ID " +
                        "order by SUP_NAME";

        stmt = con.createStatement();
        rs[0] = stmt.executeQuery(query);
    }

    public static void getSupplierOfCoffee(String coffeeName, String[] supplierName)
            throws SQLException {

        Connection con = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        String query =
                "select SUPPLIERS.SUP_NAME " +
                        "from SUPPLIERS, COFFEES " +
                        "where " +
                        "SUPPLIERS.SUP_ID = COFFEES.SUP_ID " +
                        "and ? = COFFEES.COF_NAME";

        pstmt = con.prepareStatement(query);
        pstmt.setString(1, coffeeName);
        rs = pstmt.executeQuery();

        if (rs.next()) {
            supplierName[0] = rs.getString(1);
        } else {
            supplierName[0] = null;
        }
    }

}
