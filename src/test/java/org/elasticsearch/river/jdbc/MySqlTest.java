/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.jdbc;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MySqlTest {

	String driverClassName = "com.mysql.jdbc.Driver";
	String url = "jdbc:mysql://localhost:3306/test_jdbc_river";
	String username = "";
	String password = "";

	SQLService service;
    Connection connection;

	@BeforeMethod
	public void setup() throws ClassNotFoundException, SQLException {
		service = new SQLService();
		connection = service.getConnection(driverClassName, url, username, password, true);
	}

	@Test
	public void testStarQuery() {
        try {
            String sql = "select * from orders";
            List<Object> params = new ArrayList<Object>();
            int fetchsize = 0;
            Action listener = new DefaultAction() {

                @Override
                public void index(String index, String type, String id, String parent, long version, XContentBuilder builder) throws IOException {
                    System.err.println("index=" + index + " type=" + type + " id=" + id + " parent=" + parent + " builder=" + builder.string());
                }
            };
            PreparedStatement statement = service.prepareStatement(connection, sql);
            service.bind(statement, params);
            ResultSet results = service.execute(statement, fetchsize);
            Merger merger = new Merger(listener, 1L);
            long rows = 0L;
            while (service.nextRow(results, merger)) {
                rows++;
            }
            merger.close();
            System.err.println("rows = " + rows);
            service.close(results);
            service.close(statement);
            service.close(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBill() throws SQLException, NoSuchAlgorithmException, IOException
    {
        String sql = "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product ";
        List<Object> params = new ArrayList<Object>();
        int fetchsize = 0;
        Action listener = new DefaultAction() {

            @Override
            public void index(String index, String type, String id, String parent, long version, XContentBuilder builder) throws IOException {
                System.err.println("index=" + index + " type=" + type + " id=" + id + " parent=" + parent + " builder=" + builder.string());
            }
        };
        PreparedStatement statement = service.prepareStatement(connection, sql);
        service.bind(statement, params);
        ResultSet results = service.execute(statement, fetchsize);
        Merger merger = new Merger(listener, 1L);
        long rows = 0L;
        while (service.nextRow(results, merger)) {
            rows++;
        }
        merger.close();
        System.err.println("rows = " + rows);
        service.close(results);
        service.close(statement);
        service.close(connection);
    }    
    
    @Test
    public void testRelations() throws SQLException, IOException, NoSuchAlgorithmException
    {
        String sql = "select \"relations\" as \"_index\", orders.customer as \"_id\", orders.customer as \"contact.customer\", employees.name as \"contact.employee\" from orders left join employees on employees.department = orders.department";
        List<Object> params = new ArrayList<Object>();
        int fetchsize = 0;
        Action listener = new DefaultAction() {

            @Override
            public void index(String index, String type, String id, String parent, long version, XContentBuilder builder) throws IOException {
                System.err.println("index=" + index + " type=" + type + " id=" + id + " parent=" + parent + " builder=" + builder.string());
            }
        };
        PreparedStatement statement = service.prepareStatement(connection, sql);
        service.bind(statement, params);
        ResultSet results = service.execute(statement, fetchsize);
        Merger merger = new Merger(listener, 1L);
        long rows = 0L;
        while (service.nextRow(results, merger)) {
            rows++;
        }
        merger.close();
        System.err.println("rows = " + rows);
        service.close(results);
        service.close(statement);
        service.close(connection);
    }
    
    @Test
    public void testHighBills() {
        try {
            String sql = "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product and orders.quantity * products.price > ?";
            List<Object> params = new ArrayList<Object>();
            params.add(5.0);
            int fetchsize = 0;
            Action listener = new DefaultAction() {

                @Override
                public void index(String index, String type, String id, String parent, long version, XContentBuilder builder) throws IOException {
                    System.err.println("index=" + index + " type=" + type + " id=" + id + " parent=" + parent + " builder=" + builder.string());
                }
            };
            PreparedStatement statement = service.prepareStatement(connection, sql);
            service.bind(statement, params);
            ResultSet results = service.execute(statement, fetchsize);
            Merger merger = new Merger(listener, 1L);
            long rows = 0L;
            while (service.nextRow(results, merger)) {
                rows++;
            }
            merger.close();
            System.err.println("rows = " + rows);
            service.close(results);
            service.close(statement);
            service.close(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }    

    @Test
    public void testTimePeriod() {
        try {
            String sql = "select products.name as \"product.name\", orders.customer as \"product.customer.name\", orders.quantity * products.price as \"product.customer.bill\" from products, orders where products.name = orders.product and orders.created between ? - 14 and ?";
            List<Object> params = new ArrayList<Object>();
            params.add("2012-06-01");
            params.add("$now");
            int fetchsize = 0;
            Action listener = new DefaultAction() {

                @Override
                public void index(String index, String type, String id, String parent, long version, XContentBuilder builder) throws IOException {
                    System.err.println("index=" + index + " type=" + type + " id=" + id + " parent=" + parent + " builder=" + builder.string());
                }
            };
            PreparedStatement statement = service.prepareStatement(connection, sql);
            service.bind(statement, params);
            ResultSet results = service.execute(statement, fetchsize);
            Merger merger = new Merger(listener, 1L);
            long rows = 0L;
            while (service.nextRow(results, merger)) {
                rows++;
            }
            merger.close();
            System.err.println("rows = " + rows);
            service.close(results);
            service.close(statement);
            service.close(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }    
    
}
