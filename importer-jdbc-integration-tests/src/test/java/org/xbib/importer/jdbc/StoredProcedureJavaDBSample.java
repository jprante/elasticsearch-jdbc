package org.xbib.importer.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 */
public class StoredProcedureJavaDBSample {

    public void showSuppliers() throws SQLException {
        String query =
                "select SUPPLIERS.SUP_NAME, " +
                        "COFFEES.COF_NAME " +
                        "from SUPPLIERS, COFFEES " +
                        "where SUPPLIERS.SUP_ID = " +
                        "COFFEES.SUP_ID " +
                        "order by SUP_NAME";
        try (Connection con = DriverManager.getConnection("jdbc:default:connection");
            Statement stmt = con.createStatement();) {
            ResultSet rs = stmt.executeQuery(query);
        }
    }

    public void getSupplierOfCoffee(String coffeeName, String[] supplierName)
            throws Exception {

        String query =
                "select SUPPLIERS.SUP_NAME " +
                        "from SUPPLIERS, COFFEES " +
                        "where " +
                        "SUPPLIERS.SUP_ID = COFFEES.SUP_ID " +
                        "and ? = COFFEES.COF_NAME";

        try (Connection con = DriverManager.getConnection("jdbc:default:connection");
             PreparedStatement pstmt = con.prepareStatement(query);
        ) {
            pstmt.setString(1, coffeeName);
            ResultSet rs = pstmt.executeQuery();
            supplierName[0]  = rs.next() ? rs.getString(1) : null;
            rs.close();
        }
    }

}
