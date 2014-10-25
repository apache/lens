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

import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.lens.api.LensException;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.MetastoreConstants;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.driver.jdbc.ColumnarSQLRewriter;
import org.apache.lens.server.api.LensConfConstants;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * The Class TestColumnarSQLRewriter.
 */
public class TestColumnarSQLRewriter {

  /**
   * Sets the of.
   *
   * @param args
   *          the args
   * @return the sets the
   */
  private Set<String> setOf(String... args) {
    Set<String> result = new HashSet<String>();
    for (String s : args) {
      result.add(s.replaceAll("\\s+", ""));
    }
    return result;
  }

  /**
   * Sets the of.
   *
   * @param collection
   *          the collection
   * @return the sets the
   */
  private Set<String> setOf(Collection<String> collection) {
    Set<String> result = new HashSet<String>();
    for (String s : collection) {
      result.add(s.replaceAll("\\s+", ""));
    }
    return result;
  }

  /**
   * Compare queries.
   *
   * @param expected
   *          the expected
   * @param actual
   *          the actual
   */
  private void compareQueries(String expected, String actual) {
    if (expected == null && actual == null) {
      return;
    } else if (expected == null) {
      Assert.fail();
    } else if (actual == null) {
      Assert.fail("Rewritten query is null");
    }
    String expectedTrimmed = expected.replaceAll("\\W", "");
    String actualTrimmed = actual.replaceAll("\\W", "");

    if (!expectedTrimmed.equalsIgnoreCase(actualTrimmed)) {
      String method = null;
      for (StackTraceElement trace : Thread.currentThread().getStackTrace()) {
        if (trace.getMethodName().startsWith("test")) {
          method = trace.getMethodName() + ":" + trace.getLineNumber();
        }
      }

      System.err.println("__FAILED__ " + method + "\n\tExpected: " + expected + "\n\t---------\n\tActual: " + actual);
    }
    Assert.assertTrue(expectedTrimmed.equalsIgnoreCase(actualTrimmed));
  }

  /*
   * Star schema used for the queries below
   * 
   * create table sales_fact (time_key integer, item_key integer, branch_key integer, location_key integer, dollars_sold
   * double, units_sold integer);
   * 
   * create table time_dim ( time_key integer, day date);
   * 
   * create table item_dim ( item_key integer, item_name varchar(500) );
   * 
   * create table branch_dim ( branch_key integer, branch_key varchar(100));
   * 
   * create table location_dim (location_key integer,location_name varchar(100));
   */

  /**
   * Creates the hive table.
   *
   * @param db
   *          the db
   * @param table
   *          the table
   * @param columns
   *          the columns
   * @throws Exception
   *           the exception
   */
  void createHiveTable(String db, String table, List<FieldSchema> columns) throws Exception {
    Table tbl1 = new Table(db, table);
    tbl1.setFields(columns);

    Hive.get().createTable(tbl1);
    System.out.println("Created table : " + table);
  }

  /**
   * Setup.
   *
   * @throws Exception
   *           the exception
   */
  @BeforeTest
  public void setup() throws Exception {

    List<FieldSchema> factColumns = new ArrayList<FieldSchema>();
    factColumns.add(new FieldSchema("item_key", "int", ""));
    factColumns.add(new FieldSchema("branch_key", "int", ""));
    factColumns.add(new FieldSchema("location_key", "int", ""));
    factColumns.add(new FieldSchema("dollars_sold", "double", ""));
    factColumns.add(new FieldSchema("units_sold", "int", ""));

    List<FieldSchema> factPartColumns = new ArrayList<FieldSchema>();
    factPartColumns.add(new FieldSchema("time_key", "int", ""));

    List<FieldSchema> timedimColumns = new ArrayList<FieldSchema>();
    timedimColumns.add(new FieldSchema("time_key", "int", ""));
    timedimColumns.add(new FieldSchema("day", "date", ""));

    List<FieldSchema> itemdimColumns = new ArrayList<FieldSchema>();
    itemdimColumns.add(new FieldSchema("item_key", "int", ""));
    itemdimColumns.add(new FieldSchema("item_name", "string", ""));

    List<FieldSchema> branchdimColumns = new ArrayList<FieldSchema>();
    branchdimColumns.add(new FieldSchema("branch_key", "int", ""));
    branchdimColumns.add(new FieldSchema("branch_name", "string", ""));

    List<FieldSchema> locationdimColumns = new ArrayList<FieldSchema>();
    locationdimColumns.add(new FieldSchema("location_key", "int", ""));
    locationdimColumns.add(new FieldSchema("location_name", "string", ""));

    try {
      createHiveTable("default", "sales_fact", factColumns);
      createHiveTable("default", "time_dim", timedimColumns);
      createHiveTable("default", "item_dim", itemdimColumns);
      createHiveTable("default", "branch_dim", branchdimColumns);
      createHiveTable("default", "location_dim", locationdimColumns);
    } catch (HiveException e) {
      e.printStackTrace();
    }
  }

  /**
   * Clean.
   *
   * @throws HiveException
   *           the hive exception
   */
  @AfterTest
  public void clean() throws HiveException {
    try {
      Hive.get().dropTable("default.sales_fact");
      Hive.get().dropTable("default.time_dim");
      Hive.get().dropTable("default.item_dim");
      Hive.get().dropTable("default.branch_dim");
      Hive.get().dropTable("default.location_dim");
    } catch (HiveException e) {
      e.printStackTrace();
    }
  }

  /**
   * Test no rewrite.
   *
   * @throws ParseException
   *           the parse exception
   * @throws SemanticException
   *           the semantic exception
   * @throws LensException
   *           the lens exception
   */
  @Test
  // Testing multiple queries in one instance
  public void testNoRewrite() throws ParseException, SemanticException, LensException {

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));
    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String query = "select count(distinct id) from location_dim";
    String actual = qtest.rewrite(conf, query);
    String expected = "select count( distinct  id ) from location_dim ";
    compareQueries(expected, actual);

    String query2 = "select count(distinct id) from location_dim  location_dim";
    String actual2 = qtest.rewrite(conf, query2);
    String expected2 = "select count( distinct  id ) from location_dim location_dim";
    compareQueries(expected2, actual2);

    String query3 = "select count(distinct location_dim.id) from  global_dw.location_dim location_dim";
    String actual3 = qtest.rewrite(conf, query3);
    String expected3 = "select count( distinct ( location_dim . id )) from global_dw.location_dim location_dim";
    compareQueries(expected3, actual3);

    String query4 = "select count(distinct location_dim.id) from  global_dw.location_dim location_dim "
        + "left outer join global_dw.item_dim item_dim on location_dim.id = item_dim.id "
        + "right outer join time_dim time_dim on location_dim.id = time_dim.id ";
    String actual4 = qtest.rewrite(conf, query4);
    String expected4 = "select count( distinct ( location_dim . id )) from global_dw.location_dim location_dim  "
        + "right outer join time_dim time_dim on (( location_dim . id ) = ( time_dim . id ))  "
        + "left outer join global_dw.item_dim item_dim on (( location_dim . id ) = ( item_dim . id ))";
    compareQueries(expected4, actual4);
  }

  /**
   * Test join cond.
   *
   * @throws ParseException
   *           the parse exception
   * @throws SemanticException
   *           the semantic exception
   * @throws LensException
   *           the lens exception
   */
  @Test
  public void testJoinCond() throws ParseException, SemanticException, LensException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
        + "from sales_fact fact " + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day " + "order by dollars_sold desc ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String rwq = qtest.rewrite(conf, query);
    String expected = "inner join location_dim  location_dim  on "
        + "((( fact  .  location_key ) = ( location_dim  .  location_key )) "
        + "and (( location_dim  .  location_name ) =  'test123' )) "
        + "inner join time_dim  time_dim  on (( fact  .  time_key ) = ( time_dim  .  time_key ))";
    String actual = qtest.joinCondition.toString();

    compareQueries(expected, actual);
  }

  /**
   * Test all filter cond.
   *
   * @throws ParseException
   *           the parse exception
   * @throws SemanticException
   *           the semantic exception
   * @throws LensException
   *           the lens exception
   */
  @Test
  public void testAllFilterCond() throws ParseException, SemanticException, LensException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
        + "from sales_fact fact " + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day " + "order by dollars_sold desc ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String rwq = qtest.rewrite(conf, query);
    Set<String> actual = setOf(qtest.rightFilter);
    Assert.assertEquals(
        actual,
        setOf("(( location_dim  .  location_name ) =  'test123' )",
            "( time_dim  .  time_key ) between  '2013-01-01'  and  '2013-01-31'", ""));
  }

  /**
   * Test all agg column.
   *
   * @throws ParseException
   *           the parse exception
   * @throws SemanticException
   *           the semantic exception
   * @throws LensException
   *           the lens exception
   */
  @Test
  public void testAllAggColumn() throws ParseException, SemanticException, LensException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold, "
        + "sum(fact.units_sold),avg(fact.dollars_sold),min(fact.dollars_sold),max(fact.dollars_sold)"
        + "from sales_fact fact " + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day " + "order by dollars_sold desc ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String rwq = qtest.rewrite(conf, query);
    Set<String> aggrActual = setOf(qtest.aggColumn);
    Set<String> expectedAggr = setOf("sum(( fact  .  units_sold )) as sum_fact_units_sold",
        "min(( fact  .  dollars_sold )) as min_fact_dollars_sold",
        "avg(( fact  .  dollars_sold )) as avg_fact_dollars_sold",
        "sum(( fact  .  dollars_sold )) as sum_fact_dollars_sold",
        "max(( fact  .  dollars_sold )) as max_fact_dollars_sold");
    Assert.assertEquals(aggrActual, expectedAggr);
  }

  /**
   * Test all fact keys.
   *
   * @throws ParseException
   *           the parse exception
   * @throws SemanticException
   *           the semantic exception
   * @throws LensException
   *           the lens exception
   */
  @Test
  public void testAllFactKeys() throws ParseException, SemanticException, LensException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key, "
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold, "
        + "sum(fact.units_sold),avg(fact.dollars_sold),min(fact.dollars_sold),max(fact.dollars_sold)"
        + "from sales_fact fact " + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "inner join item_dim item_dim on fact.item_key = item_dim.item_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key " + "order by dollars_sold desc ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String rwq = qtest.rewrite(conf, query);
    String expected = "fact.time_key,fact.location_key,fact.item_key,";
    String actual = qtest.factKeys.toString();
    compareQueries(expected, actual);
  }

  /**
   * Test fact sub queries.
   *
   * @throws ParseException
   *           the parse exception
   * @throws SemanticException
   *           the semantic exception
   * @throws LensException
   *           the lens exception
   */
  @Test
  public void testFactSubQueries() throws ParseException, SemanticException, LensException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key, "
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold, "
        + "sum(fact.units_sold),avg(fact.dollars_sold),min(fact.dollars_sold),max(fact.dollars_sold)"
        + "from sales_fact fact " + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "inner join item_dim item_dim on fact.item_key = item_dim.item_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' " + "and item_dim.item_name = 'item_1' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key " + "order by dollars_sold desc ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String rwq = qtest.rewrite(conf, query);
    String expected = "fact.time_key in  (  select time_dim.time_key from time_dim where ( time_dim  .  time_key ) "
        + "between  '2013-01-01'  and  '2013-01-31'  ) and fact.location_key in  (  select location_dim.location_key "
        + "from location_dim where (( location_dim  .  location_name ) =  'test123' ) ) and "
        + "fact.item_key in  (  select item_dim.item_key from item_dim "
        + "where (( item_dim  .  item_name ) =  'item_1' ) ) and ";
    String actual = qtest.allSubQueries.toString();
    compareQueries(expected, actual);
  }

  /**
   * Test rewritten query.
   *
   * @throws ParseException
   *           the parse exception
   * @throws SemanticException
   *           the semantic exception
   * @throws LensException
   *           the lens exception
   */
  @Test
  public void testRewrittenQuery() throws ParseException, SemanticException, LensException {

    String query =

    "select fact.time_key,time_dim.day_of_week,to_date(time_dim.day),item_dim.item_key, "
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold, "
        + "format_number(sum(fact.units_sold),4),format_number(avg(fact.dollars_sold),'##################.###'),"
        + "min(fact.dollars_sold),max(fact.dollars_sold)" + "from sales_fact fact "
        + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "inner join item_dim item_dim on fact.item_key = item_dim.item_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between date_add('2013-01-01', 1) and date_sub('2013-01-31',3) "
        + "and item_dim.item_name = 'item_1' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key " + "order by dollars_sold  ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String actual = qtest.rewrite(conf, query);

    String expected = "select ( fact . time_key ), ( time_dim . day_of_week ), date(( time_dim . day )), "
        + "( item_dim . item_key ),  case  when (sum(sum_fact_dollars_sold) =  0 ) then  0.0  "
        + "else sum(sum_fact_dollars_sold) end  dollars_sold , format(sum(sum_fact_units_sold),  4 ), "
        + "format(avg(avg_fact_dollars_sold),  '##################.###' ), min(min_fact_dollars_sold), "
        + "max(max_fact_dollars_sold) from  (select fact.time_key,fact.location_key,fact.item_key,"
        + "sum(( fact . dollars_sold )) as sum_fact_dollars_sold, sum(( fact . units_sold )) as sum_fact_units_sold, "
        + "avg(( fact . dollars_sold )) as avg_fact_dollars_sold, min(( fact . dollars_sold )) as min_fact_dollars_sold, "
        + "max(( fact . dollars_sold )) as max_fact_dollars_sold from sales_fact fact where fact.time_key "
        + "in  (  select time_dim.time_key from time_dim where ( time_dim . time_key ) between date_add( '2013-01-01' , interval 1  day) "
        + "and date_sub( '2013-01-31' , interval 3  day) ) and fact.location_key in  (  select location_dim.location_key from location_dim "
        + "where (( location_dim . location_name ) =  'test123' ) ) and fact.item_key in  "
        + "(  select item_dim.item_key from item_dim where (( item_dim . item_name ) =  'item_1' ) )  "
        + "group by fact.time_key,fact.location_key,fact.item_key) fact inner join item_dim  item_dim  "
        + "on ((( fact . item_key ) = ( item_dim . item_key )) and (( location_dim . location_name ) =  'test123' )) "
        + "inner join location_dim  location_dim  on (( fact . location_key ) = ( location_dim . location_key )) "
        + "inner join time_dim  time_dim  on (( fact . time_key ) = ( time_dim . time_key )) "
        + "where (( time_dim . time_key ) between date_add( '2013-01-01' , interval 1  day) and "
        + "date_sub( '2013-01-31' , interval 3  day) and (( item_dim . item_name ) =  'item_1' )) "
        + "group by ( fact . time_key ), ( time_dim . day_of_week ), ( time_dim . day ), "
        + "( item_dim . item_key ) order by dollars_sold  asc";

    compareQueries(expected, actual);
  }

  /**
   * Test union query.
   *
   * @throws ParseException
   *           the parse exception
   * @throws SemanticException
   *           the semantic exception
   * @throws LensException
   *           the lens exception
   */
  @Test
  public void testUnionQuery() throws ParseException, SemanticException, LensException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
        + "from sales_fact fact " + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-05' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day " + "order by dollars_sold  " + "union all "
        + "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
        + "from sales_fact fact " + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-02-01' and '2013-02-05' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day " + "order by dollars_sold " + "union all "
        + "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
        + "from sales_fact fact " + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-03-01' and '2013-03-05' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day " + "order by dollars_sold ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String actual = qtest.rewrite(conf, query);
    String expected = "select ( fact  .  time_key ), ( time_dim  .  day_of_week ), ( time_dim  .  day ),  "
        + "case  when (sum(sum_fact_dollars_sold) =  0 ) then  0.0  else sum(sum_fact_dollars_sold) end dollars_sold "
        + "from  (select fact.time_key,fact.location_key,sum(( fact  .  dollars_sold )) as sum_fact_dollars_sold "
        + "from sales_fact fact where fact.time_key in  (  select time_dim.time_key from time_dim where "
        + "( time_dim  .  time_key ) between  '2013-01-01'  and  '2013-01-05'  ) and fact.location_key in "
        + " (  select location_dim.location_key from location_dim where (( location_dim  .  location_name ) =  'test123' ) ) "
        + " group by fact.time_key,fact.location_key) fact inner join location_dim  location_dim  "
        + "on ((( fact  .  location_key ) = ( location_dim  .  location_key )) "
        + "and (( location_dim  .  location_name ) =  'test123' )) inner join time_dim  "
        + "time_dim  on (( fact  .  time_key ) = ( time_dim  .  time_key )) where ( time_dim  .  time_key ) "
        + "between  '2013-01-01'  and  '2013-01-05'  group by ( fact  .  time_key ), ( time_dim  .  day_of_week ), "
        + "( time_dim  .  day ) order by  dollars_sold asc union all select ( fact  .  time_key ), ( time_dim  .  day_of_week ), "
        + "( time_dim  .  day ),  case  when (sum(sum_fact_dollars_sold) =  0 ) then  0.0  else sum(sum_fact_dollars_sold) "
        + "end dollars_sold from  (select fact.time_key,fact.location_key,sum(( fact  .  dollars_sold )) as sum_fact_dollars_sold "
        + "from sales_fact fact where fact.time_key in  (  select time_dim.time_key from time_dim where ( time_dim  .  time_key ) "
        + "between  '2013-02-01'  and  '2013-02-05'  ) and fact.location_key in  "
        + "(  select location_dim.location_key from location_dim where (( location_dim  .  location_name ) =  'test123' ) )  "
        + "group by fact.time_key,fact.location_key) fact inner join location_dim  "
        + "location_dim  on ((( fact  .  location_key ) = ( location_dim  .  location_key )) and "
        + "(( location_dim  .  location_name ) =  'test123' )) inner join time_dim  time_dim  on (( fact  .  time_key ) = "
        + "( time_dim  .  time_key )) where ( time_dim  .  time_key ) between  '2013-02-01'  and  '2013-02-05'  group by "
        + "( fact  .  time_key ), ( time_dim  .  day_of_week ), ( time_dim  .  day ) order by dollars_sold asc "
        + "union all select ( fact  .  time_key ), ( time_dim  .  day_of_week ), ( time_dim  .  day ),  "
        + "case  when (sum(sum_fact_dollars_sold) =  0 ) then  0.0  else sum(sum_fact_dollars_sold) end dollars_sold "
        + "from  (select fact.time_key,fact.location_key,sum(( fact  .  dollars_sold )) as sum_fact_dollars_sold "
        + "from sales_fact fact where fact.time_key in  (  select time_dim.time_key from time_dim where "
        + "( time_dim  .  time_key ) between  '2013-03-01'  and  '2013-03-05'  ) and fact.location_key in  "
        + "(  select location_dim.location_key from location_dim where (( location_dim  .  location_name ) =  'test123' ) )  "
        + "group by fact.time_key,fact.location_key) fact inner join location_dim  location_dim  on "
        + "((( fact  .  location_key ) = ( location_dim  .  location_key )) and (( location_dim  .  location_name ) =  'test123' )) "
        + "inner join time_dim  time_dim  on (( fact  .  time_key ) = ( time_dim  .  time_key )) "
        + "where ( time_dim  .  time_key ) between  '2013-03-01'  and  '2013-03-05'  group by ( fact  .  time_key ), "
        + "( time_dim  .  day_of_week ), ( time_dim  .  day ) order by  dollars_sold asc";

    compareQueries(expected, actual);
  }

  /**
   * Test replace db name.
   *
   * @throws Exception
   *           the exception
   */
  @Test
  public void testReplaceDBName() throws Exception {
    HiveConf conf = new HiveConf(ColumnarSQLRewriter.class);
    conf.setBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, false);
    SessionState.start(conf);

    // Create test table
    createTable("default", "mytable", "testDB", "testTable_1");
    createTable("default", "mytable_2", "testDB", "testTable_2");
    createTable("default", "mytable_3", "testDB", "testTable_3");

    String query = "SELECT * FROM mydb.mytable t1 JOIN mydb.mytable_2 t2 ON t1.t2id = t2.id "
        + " left outer join mytable_3 t3 on t2.t3id = t3.id " + "WHERE A = 100";

    ColumnarSQLRewriter rewriter = new ColumnarSQLRewriter();
    rewriter.ast = HQLParser.parseHQL(query);
    rewriter.query = query;
    rewriter.analyzeInternal();

    String joinTreeBeforeRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println(joinTreeBeforeRewrite);

    // Rewrite
    CubeMetastoreClient client = CubeMetastoreClient.getInstance(conf);
    rewriter.replaceWithUnderlyingStorage(rewriter.fromAST, client);
    String joinTreeAfterRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println(joinTreeAfterRewrite);

    // Tests
    assertTrue(joinTreeBeforeRewrite.contains("mydb"));
    assertTrue(joinTreeBeforeRewrite.contains("mytable") && joinTreeBeforeRewrite.contains("mytable_2")
        && joinTreeBeforeRewrite.contains("mytable_3"));

    assertFalse(joinTreeAfterRewrite.contains("mydb"));
    assertFalse(joinTreeAfterRewrite.contains("mytable") && joinTreeAfterRewrite.contains("mytable_2")
        && joinTreeAfterRewrite.contains("mytable_3"));

    assertTrue(joinTreeAfterRewrite.contains("testdb"));
    assertTrue(joinTreeAfterRewrite.contains("testtable_1") && joinTreeAfterRewrite.contains("testtable_2")
        && joinTreeAfterRewrite.contains("testtable_3"));

    // Rewrite one more query where table and db name is not set
    createTable("default", "mytable_4", null, null);
    String query2 = "SELECT * FROM mydb.mytable_4 WHERE a = 100";
    rewriter = new ColumnarSQLRewriter();
    rewriter.ast = HQLParser.parseHQL(query2);
    rewriter.query = query2;
    rewriter.analyzeInternal();

    joinTreeBeforeRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println(joinTreeBeforeRewrite);

    // Rewrite
    rewriter.replaceWithUnderlyingStorage(rewriter.fromAST, client);
    joinTreeAfterRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println(joinTreeAfterRewrite);

    // Rewrite should not replace db and table name since its not set
    assertEquals(joinTreeAfterRewrite, joinTreeBeforeRewrite);

    // Test a query with default db
    Hive.get().dropTable("default", "mytable");
    createTable("default", "mytable", "default", null);

    String defaultQuery = "SELECT * FROM examples.mytable t1 WHERE A = 100";
    rewriter = new ColumnarSQLRewriter();
    rewriter.ast = HQLParser.parseHQL(defaultQuery);
    rewriter.query = defaultQuery;
    rewriter.analyzeInternal();
    joinTreeBeforeRewrite = HQLParser.getString(rewriter.fromAST);
    rewriter.replaceWithUnderlyingStorage(rewriter.fromAST, CubeMetastoreClient.getInstance(conf));
    joinTreeAfterRewrite = HQLParser.getString(rewriter.fromAST);
    assertTrue(joinTreeBeforeRewrite.contains("examples"), joinTreeBeforeRewrite);
    assertFalse(joinTreeAfterRewrite.contains("examples"), joinTreeAfterRewrite);
    System.out.println("default case: " + joinTreeAfterRewrite);

    Hive.get().dropTable("default", "mytable");
    Hive.get().dropTable("default", "mytable_2");
    Hive.get().dropTable("default", "mytable_3");
    Hive.get().dropTable("default", "mytable_4");
  }

  /**
   * Creates the table.
   *
   * @param db
   *          the db
   * @param table
   *          the table
   * @param udb
   *          the udb
   * @param utable
   *          the utable
   * @throws Exception
   *           the exception
   */
  void createTable(String db, String table, String udb, String utable) throws Exception {
    Table tbl1 = new Table(db, table);

    if (StringUtils.isNotBlank(udb)) {
      tbl1.setProperty(LensConfConstants.NATIVE_DB_NAME, udb);
    }
    if (StringUtils.isNotBlank(utable)) {
      tbl1.setProperty(LensConfConstants.NATIVE_TABLE_NAME, utable);
    }

    List<FieldSchema> columns = new ArrayList<FieldSchema>();
    columns.add(new FieldSchema("id", "int", "col1"));
    columns.add(new FieldSchema("name", "string", "col2"));
    tbl1.setFields(columns);

    Hive.get().createTable(tbl1);
    System.out.println("Created table " + table);
  }
}
