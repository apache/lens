/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.jdbc;

import static org.testng.Assert.*;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.ResultRow;
import org.apache.lens.server.api.driver.InMemoryResultSet;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.driver.LensResultSet;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.query.QueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.ColumnDescriptor;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The Class TestJDBCFinal.
 */
public class TestJDBCFinal {

  /** The base conf. */
  Configuration baseConf;

  /** The driver. */
  JDBCDriver driver;

  /**
   * Collection of drivers
   */
  Collection<LensDriver> drivers;

  /**
   * Test create jdbc driver.
   *
   * @throws Exception the exception
   */
  @BeforeTest
  public void testCreateJdbcDriver() throws Exception {
    baseConf = new Configuration();
    baseConf.set(JDBCDriverConfConstants.JDBC_DRIVER_CLASS, "org.hsqldb.jdbc.JDBCDriver");
    baseConf.set(JDBCDriverConfConstants.JDBC_DB_URI, "jdbc:hsqldb:mem:jdbcTestDB;MODE=MYSQL");
    baseConf.set(JDBCDriverConfConstants.JDBC_USER, "sa");
    baseConf.set(JDBCDriverConfConstants.JDBC_PASSWORD, "");
    baseConf.set(JDBCDriverConfConstants.JDBC_QUERY_REWRITER_CLASS, ColumnarSQLRewriter.class.getName());
    baseConf.set(JDBCDriverConfConstants.JDBC_EXPLAIN_KEYWORD_PARAM, "explain plan for ");

    driver = new JDBCDriver();
    driver.configure(baseConf, "jdbc", "jdbc1");
    assertNotNull(driver);
    assertTrue(driver.configured);
    System.out.println("Driver configured!");
    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    drivers = new ArrayList<LensDriver>() {
      {
        add(driver);
      }
    };
  }

  /**
   * Close.
   *
   * @throws Exception the exception
   */
  @AfterTest
  public void close() throws Exception {
    driver.close();
    System.out.println("Driver closed!");
  }

  // create table and insert data

  /**
   * Creates the tables.
   *
   * @throws Exception the exception
   */
  synchronized void createTables() throws Exception {
    Connection conn = null;
    Statement stmt = null;

    String createFact = "create table  sales_fact (time_key integer, item_key integer, branch_key integer, "
      + "location_key integer, dollars_sold double, units_sold integer)";

    String createDim1 = "create table  time_dim ( time_key integer, day date, day_of_week integer, "
      + "month integer, quarter integer, year integer )";

    String createDim2 = "create table item_dim ( item_key integer, item_name varchar(500))";

    String createDim3 = "create table branch_dim ( branch_key integer, branch_name varchar(100))";

    String createDim4 = "create table location_dim (location_key integer,location_name varchar(100))";

    String insertFact = "insert into sales_fact values "
      + "(1001,234,119,223,3000.58,56), (1002,235,120,224,3456.26,62), (1003,236,121,225,6745.23,97),"
      + "(1004,237,122,226,8753.49,106)";

    String insertDim1 = "insert into time_dim values "
      + "(1001,'1900-01-01',1,1,1,1900),(1002,'1900-01-02',2,1,1,1900),(1003,'1900-01-03',3,1,1,1900),"
      + "(1004,'1900-01-04',4,1,1,1900)";

    String insertDim2 = "insert into item_dim values " + "(234,'item1'),(235,'item2'),(236,'item3'),(237,'item4')";

    String insertDim3 = "insert into branch_dim values "
      + "(119,'branch1'),(120,'branch2'),(121,'branch3'),(122,'branch4') ";

    String insertDim4 = "insert into location_dim values " + "(223,'loc1'),(224,'loc2'),(225,'loc4'),(226,'loc4')";
    try {
      conn = driver.getConnection();
      stmt = conn.createStatement();
      // stmt.execute(dropTables);
      stmt.execute(createFact);
      stmt.execute(createDim1);
      stmt.execute(createDim2);
      stmt.execute(createDim3);
      stmt.execute(createDim4);
      stmt.execute(insertFact);
      stmt.execute(insertDim1);
      stmt.execute(insertDim2);
      stmt.execute(insertDim3);
      stmt.execute(insertDim4);

    } finally {
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
  }

  /**
   * Creates the schema.
   *
   * @throws Exception the exception
   */
  @Test
  public void createSchema() throws Exception {
    createTables();
  }

  /**
   * Test execute1.
   *
   * @throws Exception the exception
   */
  @Test
  public void testExecute1() throws Exception {
    testCreateJdbcDriver();
    final String query =

      "select fact.time_key,time_dim.day_of_week,time_dim.day," + "sum(fact.dollars_sold) "
        + "from sales_fact fact " + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "where time_dim.day between '1900-01-01' and '1900-01-03' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day " + "order by fact.time_key desc";

    QueryContext context = new QueryContext(query, "SA", new LensConf(), baseConf, drivers);

    LensResultSet resultSet = driver.execute(context);
    assertNotNull(resultSet);

    if (resultSet instanceof InMemoryResultSet) {
      InMemoryResultSet rs = (InMemoryResultSet) resultSet;
      LensResultSetMetadata rsMeta = rs.getMetadata();
      assertEquals(rsMeta.getColumns().size(), 4);

      ColumnDescriptor col1 = rsMeta.getColumns().get(0);
      assertEquals(col1.getTypeName().toLowerCase(), "int");
      assertEquals(col1.getName(), "time_key".toUpperCase());

      ColumnDescriptor col2 = rsMeta.getColumns().get(1);
      assertEquals(col2.getTypeName().toLowerCase(), "int");
      assertEquals(col2.getName(), "day_of_week".toUpperCase());

      ColumnDescriptor col3 = rsMeta.getColumns().get(2);
      assertEquals(col3.getTypeName().toLowerCase(), "date");
      assertEquals(col3.getName(), "day".toUpperCase());

      ColumnDescriptor col4 = rsMeta.getColumns().get(3);
      assertEquals(col4.getTypeName().toLowerCase(), "double");
      assertEquals(col4.getName(), "c4".toUpperCase());

      while (rs.hasNext()) {
        ResultRow row = rs.next();
        List<Object> rowObjects = row.getValues();
        System.out.println(rowObjects);
      }

      if (rs instanceof JDBCResultSet) {
        ((JDBCResultSet) rs).close();
      }
    }
  }

  /**
   * Test execute2.
   *
   * @throws Exception the exception
   */
  @Test
  public void testExecute2() throws Exception {
    testCreateJdbcDriver();
    final String query =

      "select fact.time_key,time_dim.day_of_week,time_dim.day, " + "sum(fact.dollars_sold)  "
        + "from sales_fact fact " + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join item_dim item_dim on fact.item_key = item_dim.item_key and item_dim.item_name = 'item2' "
        + "inner join branch_dim branch_dim on fact.branch_key = branch_dim.branch_key "
        + "and branch_dim.branch_name = 'branch2' "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "where time_dim.day between '1900-01-01' and '1900-01-04' " + "and location_dim.location_name = 'loc2' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day " + "order by fact.time_key  desc ";

    QueryContext context = new QueryContext(query, "SA", new LensConf(), baseConf, drivers);
    LensResultSet resultSet = driver.execute(context);
    assertNotNull(resultSet);

    if (resultSet instanceof InMemoryResultSet) {
      InMemoryResultSet rs = (InMemoryResultSet) resultSet;
      LensResultSetMetadata rsMeta = rs.getMetadata();
      assertEquals(rsMeta.getColumns().size(), 4);

      ColumnDescriptor col1 = rsMeta.getColumns().get(0);
      assertEquals(col1.getTypeName().toLowerCase(), "int");
      assertEquals(col1.getName(), "time_key".toUpperCase());

      ColumnDescriptor col2 = rsMeta.getColumns().get(1);
      assertEquals(col2.getTypeName().toLowerCase(), "int");
      assertEquals(col2.getName(), "day_of_week".toUpperCase());

      ColumnDescriptor col3 = rsMeta.getColumns().get(2);
      assertEquals(col3.getTypeName().toLowerCase(), "date");
      assertEquals(col3.getName(), "day".toUpperCase());

      ColumnDescriptor col4 = rsMeta.getColumns().get(3);
      assertEquals(col4.getTypeName().toLowerCase(), "double");
      assertEquals(col4.getName(), "c4".toUpperCase());

      while (rs.hasNext()) {
        ResultRow row = rs.next();
        List<Object> rowObjects = row.getValues();
        System.out.println(rowObjects);
      }

      if (rs instanceof JDBCResultSet) {
        ((JDBCResultSet) rs).close();
      }
    }
  }

}
