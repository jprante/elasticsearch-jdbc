/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.jdbc.strategy.standard.storedprocedure;

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
        String query =
                "select SUPPLIERS.SUP_NAME, " +
                        "COFFEES.COF_NAME " +
                        "from SUPPLIERS, COFFEES " +
                        "where SUPPLIERS.SUP_ID = " +
                        "COFFEES.SUP_ID " +
                        "order by SUP_NAME";

        Statement stmt = con.createStatement();
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
