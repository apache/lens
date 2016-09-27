/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.jdbc;

import static org.testng.Assert.*;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.cube.parse.TestQuery;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestDruidSQLRewriter {

  HiveConf hconf = new HiveConf();
  Configuration conf = new Configuration();
  DruidSQLRewriter qtest = new DruidSQLRewriter();

  /*
  * Star schema used for the queries below
  *
  * create table sales_fact (time_key varchar,item_key varchar, dollars_sold double, units_sold int);
  * /
  *
 /**
  * Compare queries.
  *
  * @param expected the expected
  * @param actual   the actual
  */
  private void compareQueries(String actual, String expected) {
    assertEquals(new TestQuery(actual), new TestQuery(expected));
  }

  /**
   * Setup.
   *
   * @throws Exception the exception
   */
  @BeforeTest
  public void setup() throws Exception {
    conf.addResource("jdbcdriver-default.xml");
    conf.addResource("drivers/jdbc/jdbc1/jdbcdriver-site.xml");
    conf.addResource("drivers/jdbc/druid/jdbcdriver-site.xml");
    qtest.init(conf);
    hconf.addResource(conf);
    SessionState.start(hconf);
    List<FieldSchema> factColumns = new ArrayList<>();
    factColumns.add(new FieldSchema("time_key", "string", ""));
    factColumns.add(new FieldSchema("item_key", "int", ""));
    factColumns.add(new FieldSchema("dollars_sold", "double", ""));
    factColumns.add(new FieldSchema("units_sold", "int", ""));

    try {
      createHiveTable("default", "sales_fact", factColumns);
    } catch (HiveException e) {
      log.error("Encountered hive exception.", e);
    }
  }

  /**
   * Creates the hive table.
   *
   * @param db      the db
   * @param table   the table
   * @param columns the columns
   * @throws Exception the exception
   */
  void createHiveTable(String db, String table, List<FieldSchema> columns) throws Exception {
    Table tbl1 = new Table(db, table);
    tbl1.setFields(columns);

    Hive.get().createTable(tbl1);
    System.out.println("Created table : " + table);
  }

  /**
   * Clean.
   *
   * @throws HiveException the hive exception
   */
  @AfterTest
  public void clean() throws HiveException {
    try {
      Hive.get().dropTable("default.sales_fact");
    } catch (HiveException e) {
      log.error("Encountered hive exception", e);
    }
  }

  @Test
  // Testing multiple queries in one instance
  public void testNoRewrite() throws LensException {

    SessionState.start(hconf);

    String query = "select count(distinct time_key) from sales_fact";
    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select count( distinct  time_key ) from sales_fact ";
    compareQueries(actual, expected);

    String query2 = "select count(distinct time_key) from sales_fact sales_fact";
    String actual2 = qtest.rewrite(query2, conf, hconf);
    String expected2 = "select count( distinct  time_key ) from sales_fact sales_fact___sales_fact";
    compareQueries(expected2, actual2);

    String query3 = "select count(distinct sales_fact.time_key) from db.sales_fact sales_fact";
    String actual3 = qtest.rewrite(query3, conf, hconf);
    String expected3 = "select count( distinct ( sales_fact__db_sales_fact_sales_fact . time_key )) "
      + "from db.sales_fact sales_fact__db_sales_fact_sales_fact";
    compareQueries(expected3, actual3);
  }


  @Test
  public void testRewrittenQuery() throws LensException {

    String query =
      "select fact.time_key as `Time Key`, sum(fact.dollars_sold) from sales_fact fact group by fact.time_key order"
        + " by dollars_sold  ";

    SessionState.start(hconf);
    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select ( fact . time_key ) as \"Time Key\" , sum(( fact . dollars_sold )) from sales_fact "
      + "fact group by ( fact . time_key ) order by dollars_sold asc";
    compareQueries(actual, expected);
  }

  @Test
  public void testJoinQueryFail() {
    String query =
      "select time_dim.day_of_week, sum(fact.dollars_sold) as dollars_sold from sales_fact fact  "
        + "inner join item_dim item_dim on fact.item_key = item_dim.item_key "
        + "where fact.item_key in (select item_key from test.item_dim idim where idim.item_name = 'item_1') ";

    SessionState.start(hconf);
    try {
      qtest.rewrite(query, conf, hconf);
      Assert.fail("The Join query did NOT suffer any exception");
    } catch (LensException e) {
      System.out.println("Exception as expected in Join testcase");
    }
  }

  @Test
  public void testWhereSubQueryFail() {
    String query =
      "select time_dim.day_of_week, sum(fact.dollars_sold) as dollars_sold from sales_fact fact  "
        + "where fact.item_key in (select item_key from test.item_dim idim where idim.item_name = 'item_1') "
        + "and fact.location_key in (select location_key from test.location_dim ldim where "
        + "ldim.location_name = 'loc_1') "
        + "group by time_dim.day_of_week "
        + "order by dollars_sold";

    SessionState.start(hconf);

    try {
      qtest.rewrite(query, conf, hconf);
      Assert.fail("The Where Sub query did NOT suffer any exception");
    } catch (LensException e) {
      System.out.println("Exception as expected in where sub query..");
    }
  }

  @Test
  public void testUnionQueryFail() {
    String query = "select a,sum(b)as b from ( select a,b from tabl1 where a<=10  union all select a,b from tabl2 where"
      + " a>10 and a<=20 union all select a,b from tabl3 where a>20 )unionResult group by a order by b desc limit 10";

    SessionState.start(hconf);
    try {
      qtest.rewrite(query, conf, hconf);
      Assert.fail("The invalid query did NOT suffer any exception");
    } catch (LensException e) {
      System.out.println("Exception as expected in Union query..");
    }
  }

  /**
   * Test replace db name.
   *
   * @throws Exception the exception
   */
  @Test
  public void testReplaceDBName() throws Exception {
    File jarDir = new File("target/testjars");
    File testJarFile = new File(jarDir, "test.jar");
    File serdeJarFile = new File(jarDir, "serde.jar");

    URL[] serdeUrls = new URL[2];
    serdeUrls[0] = new URL("file:" + testJarFile.getAbsolutePath());
    serdeUrls[1] = new URL("file:" + serdeJarFile.getAbsolutePath());

    URLClassLoader createTableClassLoader = new URLClassLoader(serdeUrls, hconf.getClassLoader());
    hconf.setClassLoader(createTableClassLoader);
    SessionState.start(hconf);

    // Create test table
    Database database = new Database();
    database.setName("mydb");

    Hive.get(hconf).createDatabase(database);
    SessionState.get().setCurrentDatabase("mydb");
    createTable(hconf, "mydb", "mytable", "testDB", "testTable_1");

    String query = "SELECT * FROM mydb.mytable t1 WHERE A = 100";

    DruidSQLRewriter rewriter = new DruidSQLRewriter();
    rewriter.init(conf);
    rewriter.ast = HQLParser.parseHQL(query, hconf);
    rewriter.query = query;
    rewriter.analyzeInternal(conf, hconf);

    String joinTreeBeforeRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println(joinTreeBeforeRewrite);

    // Rewrite
    rewriter.replaceWithUnderlyingStorage(hconf);
    String joinTreeAfterRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println("joinTreeAfterRewrite:" + joinTreeAfterRewrite);

    // Tests
    assertTrue(joinTreeBeforeRewrite.contains("mydb"));
    assertTrue(joinTreeBeforeRewrite.contains("mytable"));

    assertFalse(joinTreeAfterRewrite.contains("mydb"));
    assertFalse(joinTreeAfterRewrite.contains("mytable"));

    assertTrue(joinTreeAfterRewrite.contains("testdb"));
    assertTrue(joinTreeAfterRewrite.contains("testtable_1"));

    // Rewrite one more query where table and db name is not set
    createTable(hconf, "mydb", "mytable_2", null, null);
    String query2 = "SELECT * FROM mydb.mytable_2 WHERE a = 100";
    rewriter.ast = HQLParser.parseHQL(query2, hconf);
    rewriter.query = query2;
    rewriter.analyzeInternal(conf, hconf);

    joinTreeBeforeRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println(joinTreeBeforeRewrite);

    // Rewrite
    rewriter.replaceWithUnderlyingStorage(hconf);
    joinTreeAfterRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println(joinTreeAfterRewrite);

    // Rewrite should not replace db and table name since its not set
    assertEquals(joinTreeAfterRewrite, joinTreeBeforeRewrite);

    // Test a query with default db
    Hive.get().dropTable("mydb", "mytable");
    database = new Database();
    database.setName("examples");
    Hive.get().createDatabase(database);
    createTable(hconf, "examples", "mytable", "default", null);

    String defaultQuery = "SELECT * FROM examples.mytable t1 WHERE A = 100";
    rewriter.ast = HQLParser.parseHQL(defaultQuery, hconf);
    rewriter.query = defaultQuery;
    rewriter.analyzeInternal(conf, hconf);
    joinTreeBeforeRewrite = HQLParser.getString(rewriter.fromAST);
    rewriter.replaceWithUnderlyingStorage(hconf);
    joinTreeAfterRewrite = HQLParser.getString(rewriter.fromAST);
    assertTrue(joinTreeBeforeRewrite.contains("examples"), joinTreeBeforeRewrite);
    assertFalse(joinTreeAfterRewrite.contains("examples"), joinTreeAfterRewrite);
    System.out.println("default case: " + joinTreeAfterRewrite);

    Hive.get().dropTable("mydb", "mytable");
    Hive.get().dropTable("mydb", "mytable_2");
    Hive.get().dropTable("examples", "mytable");

    Hive.get().dropDatabase("mydb", true, true, true);
    Hive.get().dropDatabase("examples", true, true, true);
    SessionState.get().setCurrentDatabase("default");
  }

  void createTable(HiveConf conf, String db, String table, String udb, String utable) throws Exception {
    createTable(conf, db, table, udb, utable, true, null);
  }

  /**
   * Test replace column mapping.
   *
   * @throws Exception the exception
   */
  @Test
  public void testReplaceColumnMapping() throws Exception {
    SessionState.start(hconf);
    String testDB = "testrcm";
    Hive.get().dropDatabase(testDB, true, true, true);

    // Create test table
    Database database = new Database();
    database.setName(testDB);

    Hive.get(hconf).createDatabase(database);
    try {
      SessionState.get().setCurrentDatabase(testDB);
      Map<String, String> columnMap = new HashMap<>();
      columnMap.put("id", "id1");
      columnMap.put("name", "name1");
      columnMap.put("dollars_sold", "Dollars_Sold");
      columnMap.put("units_sold", "Units_Sold");

      createTable(hconf, testDB, "mytable", "testDB", "testTable_1", false, columnMap);

      String query = "SELECT t1.id, t1.name, sum(t1.dollars_sold), sum(t1.units_sold) FROM " + testDB
        + ".mytable t1 WHERE t1.id = 100 GROUP BY t1.id HAVING count(t1.id) > 2 ORDER BY t1.id";

      DruidSQLRewriter rewriter = new DruidSQLRewriter();
      rewriter.init(conf);
      rewriter.ast = HQLParser.parseHQL(query, hconf);
      rewriter.query = query;
      rewriter.analyzeInternal(conf, hconf);

      String actual = rewriter.rewrite(query, conf, hconf);
      System.out.println("Actual : " + actual);
      String expected =
        "select (t1.id1), (t1.name1), sum((t1.Dollars_Sold)), sum((t1.Units_Sold)) from testDB.testTable_1 t1 where ("
          + "(t1.id1) = 100) group by (t1.id1) having (count((t1.id1)) > 2) order by t1.id1 asc";

      compareQueries(actual, expected);

    } finally {
      Hive.get().dropTable(testDB, "mytable", true, true);
      Hive.get().dropDatabase(testDB, true, true, true);
      SessionState.get().setCurrentDatabase("default");
    }
  }

  /**
   * Creates the table.
   *
   * @param db     the db
   * @param table  the table
   * @param udb    the udb
   * @param utable the utable
   * @param setCustomSerde whether to set custom serde or not
   * @param columnMapping columnmapping for the table
   *
   * @throws Exception the exception
   */
  void createTable(
    HiveConf conf, String db, String table, String udb, String utable, boolean setCustomSerde,
    Map<String, String> columnMapping) throws Exception {
    Table tbl1 = new Table(db, table);

    if (StringUtils.isNotBlank(udb)) {
      tbl1.setProperty(LensConfConstants.NATIVE_DB_NAME, udb);
    }
    if (StringUtils.isNotBlank(utable)) {
      tbl1.setProperty(LensConfConstants.NATIVE_TABLE_NAME, utable);
    }
    if (columnMapping != null && !columnMapping.isEmpty()) {
      tbl1.setProperty(LensConfConstants.NATIVE_TABLE_COLUMN_MAPPING, StringUtils.join(columnMapping.entrySet(), ","));
      log.info("columnMapping property:{}", tbl1.getProperty(LensConfConstants.NATIVE_TABLE_COLUMN_MAPPING));
    }

    List<FieldSchema> columns = new ArrayList<FieldSchema>();
    columns.add(new FieldSchema("id", "int", "col1"));
    columns.add(new FieldSchema("name", "string", "col2"));
    columns.add(new FieldSchema("dollars_sold", "double", "col3"));
    columns.add(new FieldSchema("units_sold", "int", "col4"));

    tbl1.setFields(columns);

    Hive.get(conf).createTable(tbl1);
    System.out.println("Created table " + table);
  }
}
