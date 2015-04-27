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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import org.apache.lens.api.LensException;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.LensConfConstants;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * The Class TestColumnarSQLRewriter.
 */
public class TestColumnarSQLRewriter {

  HiveConf hconf = new HiveConf();
  Configuration conf = new Configuration();
  ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

  /**
   * Sets the of.
   *
   * @param args the args
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
   * @param collection the collection
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
   * @param expected the expected
   * @param actual   the actual
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
   * create table sales_fact (time_key integer, item_key integer, branch_key integer, location_key integer,
   * dollars_sold double, units_sold integer);
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
   * Setup.
   *
   * @throws Exception the exception
   */
  @BeforeTest
  public void setup() throws Exception {
    qtest.init(conf);

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
   * @throws HiveException the hive exception
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
   * @throws ParseException    the parse exception
   * @throws SemanticException the semantic exception
   * @throws LensException     the lens exception
   */
  @Test
  // Testing multiple queries in one instance
  public void testNoRewrite() throws ParseException, SemanticException, LensException {

    SessionState.start(hconf);

    String query = "select count(distinct id) from location_dim";
    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select count( distinct  id ) from location_dim ";
    compareQueries(expected, actual);

    String query2 = "select count(distinct id) from location_dim  location_dim";
    String actual2 = qtest.rewrite(query2, conf, hconf);
    String expected2 = "select count( distinct  id ) from location_dim location_dim___location_dim";
    compareQueries(expected2, actual2);

    String query3 = "select count(distinct location_dim.id) from  db.location_dim location_dim";
    String actual3 = qtest.rewrite(query3, conf, hconf);
    String expected3 = "select count( distinct ( location_dim__db_location_dim_location_dim . id )) "
      + "from db.location_dim location_dim__db_location_dim_location_dim";
    compareQueries(expected3, actual3);

    String query4 = "select count(distinct location_dim.id) from  db.location_dim location_dim "
      + "left outer join db.item_dim item_dim on location_dim.id = item_dim.id "
      + "right outer join time_dim time_dim on location_dim.id = time_dim.id ";
    String actual4 = qtest.rewrite(query4, conf, hconf);
    String expected4 = "select count( distinct ( location_dim__db_location_dim_location_dim . id )) "
      + "from db.location_dim location_dim__db_location_dim_location_dim  left outer join db.item_dim "
      + "item_dim__db_item_dim_item_dim on (( location_dim__db_location_dim_location_dim . id ) = "
      + "( item_dim__db_item_dim_item_dim . id ))  right outer join time_dim time_dim___time_dim on "
      + "(( location_dim__db_location_dim_location_dim . id ) = ( time_dim___time_dim . id ))";
    compareQueries(expected4, actual4);

  }

  /**
   * Test join cond.
   *
   * @throws ParseException    the parse exception
   * @throws SemanticException the semantic exception
   * @throws LensException     the lens exception
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

    SessionState.start(hconf);

    String rwq = qtest.rewrite(query, conf, hconf);
    String expected = "inner join time_dim time_dim___time_dim on (( sales_fact___fact . time_key ) = "
      + "( time_dim___time_dim . time_key ))  inner join location_dim location_dim___location_dim on "
      + "((( sales_fact___fact . location_key ) = ( location_dim___location_dim . location_key )) "
      + "and (( location_dim___location_dim . location_name ) =  'test123' ))";
    String actual = qtest.joinCondition.toString();

    compareQueries(expected, actual);
  }

  /**
   * Test all filter cond.
   *
   * @throws ParseException    the parse exception
   * @throws SemanticException the semantic exception
   * @throws LensException     the lens exception
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

    SessionState.start(hconf);

    String rwq = qtest.rewrite(query, conf, hconf);
    String expected = "[(( location_dim___location_dim . location_name ) =  "
      + "'test123' ), , , ( time_dim___time_dim . time_key ) between  '2013-01-01'  and  '2013-01-31' "
      + ", , ( time_dim___time_dim . time_key ) between  '2013-01-01'  and  '2013-01-31' ]";
    String actual = qtest.rightFilter.toString();

    compareQueries(expected, actual);

  }

  /**
   * Test all agg column.
   *
   * @throws ParseException    the parse exception
   * @throws SemanticException the semantic exception
   * @throws LensException     the lens exception
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

    SessionState.start(hconf);

    String rwq = qtest.rewrite(query, conf, hconf);
    String actual = qtest.aggColumn.toString();
    String expected = "[sum(( sales_fact___fact . dollars_sold )) as sum_sales_fact___fact_dollars_sold, "
      + "sum(( sales_fact___fact . units_sold )) as sum_sales_fact___fact_units_sold, "
      + "avg(( sales_fact___fact . dollars_sold )) as avg_sales_fact___fact_dollars_sold, "
      + "min(( sales_fact___fact . dollars_sold )) as min_sales_fact___fact_dollars_sold, "
      + "max(( sales_fact___fact . dollars_sold )) as max_sales_fact___fact_dollars_sold]";
    Assert.assertEquals(expected, actual);
  }

  /**
   * Test all fact keys.
   *
   * @throws ParseException    the parse exception
   * @throws SemanticException the semantic exception
   * @throws LensException     the lens exception
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

    SessionState.start(hconf);

    String rwq = qtest.rewrite(query, conf, hconf);
    String expected = "sales_fact___fact.time_key,sales_fact___fact.location_key,sales_fact___fact.item_key,";
    String actual = qtest.factKeys.toString();
    compareQueries(expected, actual);
  }

  /**
   * Test fact sub queries.
   *
   * @throws ParseException    the parse exception
   * @throws SemanticException the semantic exception
   * @throws LensException     the lens exception
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

    SessionState.start(hconf);

    String rwq = qtest.rewrite(query, conf, hconf);
    String expected = "sales_fact___fact.time_key in  (  select time_dim .time_key from time_dim where "
      + "( time_dim. time_key ) between  '2013-01-01'  and  '2013-01-31'  ) and sales_fact___fact.location_key in  "
      + "(  select location_dim .location_key from location_dim "
      + "where (( location_dim. location_name ) =  'test123' ) ) "
      + "and sales_fact___fact.item_key in  (  select item_dim .item_key from "
      + "item_dim where (( item_dim. item_name ) =  'item_1' ) ) and";
    String actual = qtest.allSubQueries.toString();
    compareQueries(expected, actual);
  }

  /**
   * Test rewritten query.
   *
   * @throws ParseException    the parse exception
   * @throws SemanticException the semantic exception
   * @throws LensException     the lens exception
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

    SessionState.start(hconf);

    String actual = qtest.rewrite(query, conf, hconf);

    String expected = "select ( sales_fact___fact . time_key ), ( time_dim___time_dim . day_of_week ), "
      + "date(( time_dim___time_dim . day )), ( item_dim___item_dim . item_key ),  case  when "
      + "(sum(sum_sales_fact___fact_dollars_sold) =  0 ) then  0.0  else sum(sum_sales_fact___fact_dollars_sold) "
      + "end  dollars_sold , format(sum(sum_sales_fact___fact_units_sold),  4 ), "
      + "format(avg(avg_sales_fact___fact_dollars_sold),  "
      + "'##################.###' ), min(min_sales_fact___fact_dollars_sold), max(max_sales_fact___fact_dollars_sold) "
      + "from  (select sales_fact___fact.time_key,sales_fact___fact.location_key,sales_fact___fact.item_key,"
      + "sum(( sales_fact___fact . dollars_sold )) as sum_sales_fact___fact_dollars_sold, "
      + "sum(( sales_fact___fact . units_sold )) as sum_sales_fact___fact_units_sold, "
      + "avg(( sales_fact___fact . dollars_sold )) as avg_sales_fact___fact_dollars_sold, "
      + "min(( sales_fact___fact . dollars_sold )) as min_sales_fact___fact_dollars_sold, "
      + "max(( sales_fact___fact . dollars_sold )) as max_sales_fact___fact_dollars_sold "
      + "from sales_fact sales_fact___fact where sales_fact___fact.time_key in  (  select time_dim .time_key "
      + "from time_dim where ( time_dim. time_key ) between date_add( '2013-01-01' , interval 1  day) "
      + "and date_sub( '2013-01-31' , interval 3  day) ) and sales_fact___fact.location_key in  "
      + "(  select location_dim .location_key from location_dim where "
      + "(( location_dim. location_name ) =  'test123' ) ) "
      + "and sales_fact___fact.item_key in  (  select item_dim .item_key from item_dim where "
      + "(( item_dim. item_name ) =  'item_1' ) )  group by sales_fact___fact.time_key,"
      + "sales_fact___fact.location_key,sales_fact___fact.item_key) sales_fact___fact  "
      + "inner join time_dim time_dim___time_dim on (( sales_fact___fact . time_key ) = "
      + "( time_dim___time_dim . time_key ))  inner join location_dim location_dim___location_dim "
      + "on (( sales_fact___fact . location_key ) = ( location_dim___location_dim . location_key ))  "
      + "inner join item_dim item_dim___item_dim on ((( sales_fact___fact . item_key ) = "
      + "( item_dim___item_dim . item_key )) and (( location_dim___location_dim . location_name ) = "
      + " 'test123' ))  where (( time_dim___time_dim . time_key ) between date_add( '2013-01-01' , interval 1  day) "
      + "and date_sub( '2013-01-31' , interval 3  day) and (( item_dim___item_dim . item_name ) =  'item_1' )) "
      + "group by ( sales_fact___fact . time_key ), ( time_dim___time_dim . day_of_week ), "
      + "( time_dim___time_dim . day ), ( item_dim___item_dim . item_key ) order by dollars_sold  asc";

    compareQueries(expected, actual);
  }

  /**
   * Test union query.
   *
   * @throws ParseException    the parse exception
   * @throws SemanticException the semantic exception
   * @throws LensException     the lens exception
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

    SessionState.start(hconf);

    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select ( sales_fact___fact . time_key ), ( time_dim___time_dim . day_of_week ), "
      + "( time_dim___time_dim . day ),  case  when (sum(sum_sales_fact___fact_dollars_sold) =  0 ) then  "
      + "0.0  else sum(sum_sales_fact___fact_dollars_sold) end  dollars_sold  "
      + "from  (select sales_fact___fact.time_key,"
      + "sales_fact___fact.location_key,sum(( sales_fact___fact . dollars_sold )) "
      + "as sum_sales_fact___fact_dollars_sold "
      + "from sales_fact sales_fact___fact where sales_fact___fact.time_key in  "
      + "(  select time_dim .time_key from time_dim "
      + "where ( time_dim. time_key ) between  '2013-01-01'  and  '2013-01-05'  ) "
      + "and sales_fact___fact.location_key in  "
      + "(  select location_dim .location_key from location_dim where "
      + "(( location_dim. location_name ) =  'test123' ) )  "
      + "group by sales_fact___fact.time_key,sales_fact___fact.location_key) sales_fact___fact  inner join time_dim "
      + "time_dim___time_dim on (( sales_fact___fact . time_key ) = ( time_dim___time_dim . time_key ))  "
      + "inner join location_dim location_dim___location_dim on ((( sales_fact___fact . location_key ) = "
      + "( location_dim___location_dim . location_key )) "
      + "and (( location_dim___location_dim . location_name ) =  'test123' )) "
      + " where ( time_dim___time_dim . time_key ) between  '2013-01-01'  and  '2013-01-05'  group by "
      + "( sales_fact___fact . time_key ), ( time_dim___time_dim . day_of_week ), ( time_dim___time_dim . day ) "
      + "order by dollars_sold  asc  union all select ( sales_fact___fact . time_key ), ( time_dim___time_dim . "
      + "day_of_week ), ( time_dim___time_dim . day ),  case  when (sum(sum_sales_fact___fact_dollars_sold) =  0 ) "
      + "then  0.0  else sum(sum_sales_fact___fact_dollars_sold) end  dollars_sold  "
      + "from  (select sales_fact___fact.time_key,"
      + "sales_fact___fact.location_key,sum(( sales_fact___fact . dollars_sold ))"
      + " as sum_sales_fact___fact_dollars_sold "
      + "from sales_fact sales_fact___fact where sales_fact___fact.time_key in  (  select time_dim .time_key "
      + "from time_dim where ( time_dim. time_key ) between  '2013-02-01'  and  '2013-02-05'  ) and "
      + "sales_fact___fact.location_key in  (  select location_dim .location_key from location_dim where "
      + "(( location_dim. location_name ) =  'test123' ) )  "
      + "group by sales_fact___fact.time_key,sales_fact___fact.location_key) "
      + "sales_fact___fact  inner join time_dim time_dim___time_dim on (( sales_fact___fact . time_key ) = "
      + "( time_dim___time_dim . time_key ))  inner join location_dim location_dim___location_dim "
      + "on ((( sales_fact___fact . "
      + "location_key ) = ( location_dim___location_dim . location_key )) "
      + "and (( location_dim___location_dim . location_name ) =  "
      + "'test123' ))  where ( time_dim___time_dim . time_key ) between  '2013-02-01'  and  '2013-02-05'  group by "
      + "( sales_fact___fact . time_key ), ( time_dim___time_dim . day_of_week ), ( time_dim___time_dim . day ) "
      + "order by dollars_sold  asc  union all "
      + "select ( sales_fact___fact . time_key ), ( time_dim___time_dim . day_of_week ), "
      + "( time_dim___time_dim . day ),  case  when (sum(sum_sales_fact___fact_dollars_sold) =  0 ) then  0.0  else "
      + "sum(sum_sales_fact___fact_dollars_sold) end  dollars_sold  from  (select sales_fact___fact.time_key,"
      + "sales_fact___fact.location_key,sum(( sales_fact___fact . dollars_sold )) "
      + "as sum_sales_fact___fact_dollars_sold "
      + "from sales_fact sales_fact___fact where sales_fact___fact.time_key in  "
      + "(  select time_dim .time_key from time_dim "
      + "where ( time_dim. time_key ) between  '2013-03-01'  and  '2013-03-05'  ) "
      + "and sales_fact___fact.location_key in  "
      + "(  select location_dim .location_key from location_dim where "
      + "(( location_dim. location_name ) =  'test123' ) ) "
      + " group by sales_fact___fact.time_key,sales_fact___fact.location_key) sales_fact___fact  "
      + "inner join time_dim time_dim___time_dim "
      + "on (( sales_fact___fact . time_key ) = ( time_dim___time_dim . time_key ))  "
      + "inner join location_dim location_dim___location_dim on ((( sales_fact___fact . location_key ) = "
      + "( location_dim___location_dim . location_key )) "
      + "and (( location_dim___location_dim . location_name ) =  'test123' ))  "
      + "where ( time_dim___time_dim . time_key ) between  '2013-03-01'  and  '2013-03-05'  "
      + "group by ( sales_fact___fact . time_key ), "
      + "( time_dim___time_dim . day_of_week ), ( time_dim___time_dim . day ) order by dollars_sold  asc";
    compareQueries(expected, actual);
  }

  @Test
  public void testNoAggCol() throws ParseException, SemanticException, LensException {

    String query = "SELECT  distinct ( location_dim . id ) FROM location_dim "
      + "location_dim join time_dim time_dim on location_dim.time_id = time_dim.id "
      + "WHERE ( time_dim . full_date ) between  '2013-01-01 00:00:00'  and  '2013-01-04 00:00:00'  LIMIT 10 ";

    SessionState.start(hconf);

    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select  distinct ( location_dim___location_dim . id ) "
      + "from location_dim location_dim___location_dim  "
      + "inner join time_dim time_dim___time_dim on "
      + "(( location_dim___location_dim . time_id ) = ( time_dim___time_dim . id ))  "
      + "where ( time_dim___time_dim . full_date ) "
      + "between  '2013-01-01 00:00:00'  and  '2013-01-04 00:00:00'  limit 10";
    compareQueries(expected, actual);

  }

  @Test
  public void testCountReplace() throws ParseException, SemanticException, LensException {

    String query = "SELECT  count(location_dim.name) FROM location_dim "
      + "location_dim join time_dim time_dim on location_dim.time_id = time_dim.id "
      + "WHERE ( time_dim . full_date ) between  '2013-01-01 00:00:00'  and  '2013-01-04 00:00:00'  LIMIT 10 ";

    SessionState.start(hconf);

    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select sum(count_location_dim___location_dim_name) from  "
      + "(select location_dim___location_dim.time_id,count(( location_dim___location_dim . name )) as "
      + "count_location_dim___location_dim_name from location_dim location_dim___location_dim where "
      + "location_dim___location_dim.time_id in  (  select time_dim .id from time_dim where "
      + "( time_dim. full_date ) between  '2013-01-01 00:00:00'  and  '2013-01-04 00:00:00'  )  "
      + "group by location_dim___location_dim.time_id) location_dim___location_dim  inner join "
      + "time_dim time_dim___time_dim on (( location_dim___location_dim . time_id ) = "
      + "( time_dim___time_dim . id ))  where ( time_dim___time_dim . full_date ) "
      + "between  '2013-01-01 00:00:00'  and  '2013-01-04 00:00:00'  limit 10";
    compareQueries(expected, actual);
  }

  @Test
  public void testReplaceAlias() throws ParseException, SemanticException, LensException {

    String query = "select fact.time_key,time_dim.day_of_week,time_dim.day,"
      + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
      + "from db.sales_fact as fact " + "inner join time_dim as time_dim on fact.time_key = time_dim.time_key "
      + "inner join db.location_dim ld on fact.location_key = ld.location_key " + "and ld.location_name = 'test123' "
      + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
      + "group by fact.time_key,time_dim.day_of_week,time_dim.day " + "order by dollars_sold desc ";

    SessionState.start(hconf);

    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select ( sales_fact__db_sales_fact_fact . time_key ), ( time_dim___time_dim . day_of_week ), "
      + "( time_dim___time_dim . day ),  case  when (sum(sum_sales_fact__db_sales_fact_fact_dollars_sold) =  0 ) "
      + "then  0.0  else sum(sum_sales_fact__db_sales_fact_fact_dollars_sold) end  dollars_sold  from  "
      + "(select sales_fact__db_sales_fact_fact.location_key, sales_fact__db_sales_fact_fact.time_key,"
      + "sum(( sales_fact__db_sales_fact_fact . dollars_sold )) as sum_sales_fact__db_sales_fact_fact_dollars_sold "
      + "from db.sales_fact sales_fact__db_sales_fact_fact where sales_fact__db_sales_fact_fact.time_key in  "
      + "(  select time_dim .time_key from time_dim "
      + "where ( time_dim. time_key ) between  '2013-01-01'  and  '2013-01-31'  ) "
      + "and sales_fact__db_sales_fact_fact.location_key in  (  select location_dim .location_key from "
      + "location_dim where (( location_dim. location_name ) =  'test123' ) )  group by "
      + "sales_fact__db_sales_fact_fact.location_key, sales_fact__db_sales_fact_fact.time_key) "
      + "sales_fact__db_sales_fact_fact  inner join time_dim time_dim___time_dim on "
      + "(( sales_fact__db_sales_fact_fact . time_key ) = ( time_dim___time_dim . time_key ))  "
      + "inner join db.location_dim location_dim__db_location_dim_ld "
      + "on ((( sales_fact__db_sales_fact_fact . location_key ) "
      + "= ( location_dim__db_location_dim_ld . location_key )) and "
      + "(( location_dim__db_location_dim_ld . location_name ) =  'test123' )) "
      + " where ( time_dim___time_dim . time_key ) "
      + "between  '2013-01-01'  and  '2013-01-31'  group by ( sales_fact__db_sales_fact_fact . time_key ), "
      + "( time_dim___time_dim . day_of_week ), ( time_dim___time_dim . day ) order by dollars_sold  desc";

    compareQueries(expected, actual);
  }


  @Test
  public void testSkipSnowflakeJoinFact() throws ParseException, SemanticException, LensException {

    String query = "SELECT (dim1 . date) date , sum((f . msr1)) msr1 , (dim2 . name) dim2_name, "
      + "(dim3 . name) dim3_name , (dim4 . name) dim4_name " + "FROM fact f "
      + "INNER JOIN dim1 dim1 ON f.dim1_id = dim1.id " + "INNER JOIN dim2 dim2 ON f.dim2_id = dim2.id "
      + "INNER JOIN dim3 dim3 ON f.dim3_id = dim3.id " + "INNER JOIN dim4 dim4 ON  dim2.id_2 = dim4.id_2 "
      + "WHERE ((dim1 . date) = '2014-11-25 00:00:00') "
      + "GROUP BY (dim1 . date),  (dim2 . name), (dim3 . name) , (dim4 . name) ";

    SessionState.start(hconf);

    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select ( dim1___dim1 . date ) date , sum(sum_fact___f_msr1) msr1 , "
      + "( dim2___dim2 . name ) dim2_name , "
      + "( dim3___dim3 . name ) dim3_name , ( dim4___dim4 . name ) dim4_name "
      + "from  (select fact___f.dim2_id, fact___f.dim1_id, fact___f.dim3_id,"
      + "sum(( fact___f . msr1 )) as sum_fact___f_msr1 from fact fact___f where fact___f.dim1_id in  "
      + "(  select dim1 .id from dim1 where (( dim1. date ) =  '2014-11-25 00:00:00' ) )  "
      + "group by fact___f.dim2_id, fact___f.dim1_id, fact___f.dim3_id) fact___f  inner join dim1 dim1___dim1 on "
      + "(( fact___f . dim1_id ) = ( dim1___dim1 . id ))  inner join dim2 dim2___dim2 on (( fact___f . dim2_id ) = "
      + "( dim2___dim2 . id ))  inner join dim3 dim3___dim3 on (( fact___f . dim3_id ) = ( dim3___dim3 . id ))  "
      + "inner join dim4 dim4___dim4 on (( dim2___dim2 . id_2 ) = ( dim4___dim4 . id_2 ))  "
      + "where (( dim1___dim1 . date ) =  '2014-11-25 00:00:00' ) "
      + "group by ( dim1___dim1 . date ), ( dim2___dim2 . name ), "
      + "( dim3___dim3 . name ), ( dim4___dim4 . name ))";

    compareQueries(expected, actual);
  }


  @Test
  public void testFactFilterPushDown() throws ParseException, SemanticException, LensException {

    String query = "SELECT (dim1 . date) date , sum((f . msr1)) msr1 , (dim2 . name) dim2_name  "
      + "FROM fact f  INNER JOIN dim1 dim1 ON f.dim1_id = dim1.id  and f.m2 = '1234' "
      + "INNER JOIN dim2 dim2 ON f.dim2_id = dim2.id  and f.m3 > 3000 "
      + "WHERE ((dim1 . date) = '2014-11-25 00:00:00')  and f.m4  is not null "
      + "GROUP BY (dim1 . date),  (dim2 . name)";

    SessionState.start(hconf);

    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select ( dim1___dim1 . date ) date , sum(sum_fact___f_msr1) msr1 , "
      + "( dim2___dim2 . name ) dim2_name  from  "
      + "(select fact___f.dim2_id, fact___f.dim1_id, fact___f.m4, fact___f.m3, fact___f.m2,sum(( fact___f . msr1 )) as "
      + "sum_fact___f_msr1 from fact fact___f where ( fact___f . m4 ) is not null  and (( fact___f . m2 ) =  '1234' ) "
      + "and (( fact___f . m3 ) >  3000 ) and fact___f.dim1_id in  (  select dim1 .id from dim1 "
      + "where (( dim1. date ) =  '2014-11-25 00:00:00' ) )  "
      + "group by fact___f.dim2_id, fact___f.dim1_id, fact___f.m4, fact___f.m3, fact___f.m2) "
      + "fact___f  inner join dim1 dim1___dim1 on "
      + "((( fact___f . dim1_id ) = ( dim1___dim1 . id )) and (( fact___f . m2 ) =  '1234' ))  "
      + "inner join dim2 dim2___dim2 "
      + "on ((( fact___f . dim2_id ) = ( dim2___dim2 . id )) and (( fact___f . m3 ) >  3000 ))  "
      + "where ((( dim1___dim1 . date ) =  '2014-11-25 00:00:00' ) and ( fact___f . m4 ) is not null ) "
      + "group by ( dim1___dim1 . date ), ( dim2___dim2 . name )";

    compareQueries(expected, actual);
  }

  @Test
  public void testOrderByAlias() throws ParseException, SemanticException, LensException {

    String query = "SELECT (dim1 . date) dim1_date , sum((f . msr1)) msr1 , (dim2 . name) dim2_name  "
      + "FROM fact f  INNER JOIN dim1 dim1 ON f.dim1_id = dim1.id  and f.m2 = '1234' "
      + "INNER JOIN dim2 dim2 ON f.dim2_id = dim2.id  and f.m3 > 3000 "
      + "WHERE ((dim1 . date) = '2014-11-25 00:00:00')  and f.m4  is not null "
      + "GROUP BY (dim1 . date),  (dim2 . name) ORDER BY dim1_date";

    SessionState.start(hconf);

    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select ( dim1___dim1 . date ) dim1_date , sum(sum_fact___f_msr1) msr1 , "
      + "( dim2___dim2 . name ) dim2_name  from  (select fact___f.dim2_id, "
      + "fact___f.dim1_id, fact___f.m4, fact___f.m3, fact___f.m2,sum(( fact___f . msr1 )) "
      + "as sum_fact___f_msr1 from fact fact___f "
      + "where ( fact___f . m4 ) is not null  and (( fact___f . m2 ) =  '1234' ) "
      + "and (( fact___f . m3 ) >  3000 ) and fact___f.dim1_id in  (  select dim1 .id from dim1 "
      + "where (( dim1. date ) =  '2014-11-25 00:00:00' ) )  group by fact___f.dim2_id, fact___f.dim1_id, "
      + "fact___f.m4, fact___f.m3, fact___f.m2) fact___f  inner join dim1 dim1___dim1 on ((( fact___f . dim1_id ) = "
      + "( dim1___dim1 . id )) and (( fact___f . m2 ) =  '1234' ))  "
      + "inner join dim2 dim2___dim2 on ((( fact___f . dim2_id ) = "
      + "( dim2___dim2 . id )) and (( fact___f . m3 ) >  3000 )) "
      + "where ((( dim1___dim1 . date ) =  '2014-11-25 00:00:00' ) and "
      + "( fact___f . m4 ) is not null ) "
      + "group by ( dim1___dim1 . date ), ( dim2___dim2 . name ) order by dim1_date  asc";

    compareQueries(expected, actual);
  }

  @Test
  public void testExcludeJoinFilterFromFactQuery() throws ParseException, SemanticException, LensException {

    String query = "SELECT (dim1 . date) dim1_date , sum((f . msr1)) msr1 , (dim2 . name) dim2_name  "
      + "FROM fact f  INNER JOIN dim1 dim1 ON f.dim1_id = dim1.id  and f.m2 = '1234' "
      + "INNER JOIN dim2 dim2 ON f.dim2_id = dim2.id  and f.dim3_id = dim2.id "
      + "WHERE ((dim1 . date) = '2014-11-25 00:00:00')  and f.m4  is not null "
      + "GROUP BY (dim1 . date),  (dim2 . name) ORDER BY dim1_date";

    SessionState.start(hconf);

    String actual = qtest.rewrite(query, conf, hconf);
    String expected = "select ( dim1___dim1 . date ) dim1_date , sum(sum_fact___f_msr1) msr1 , "
      + "( dim2___dim2 . name ) dim2_name  from  "
      + "(select fact___f.dim2_id, fact___f.dim1_id, fact___f.dim3_id, fact___f.m4, fact___f.m2,sum((fact___f.msr1)) "
      + "as sum_fact___f_msr1 from fact fact___f where ( fact___f . m4 ) is not null  "
      + "and (( fact___f.m2 ) =  '1234' ) and "
      + "fact___f.dim1_id in  (  select dim1 .id from dim1 where (( dim1. date ) =  '2014-11-25 00:00:00' ) )  "
      + "group by fact___f.dim2_id, fact___f.dim1_id, fact___f.dim3_id, fact___f.m4, fact___f.m2) fact___f  "
      + "inner join dim1 dim1___dim1 "
      + "on ((( fact___f . dim1_id ) = ( dim1___dim1 . id )) and (( fact___f . m2 ) =  '1234' ))  inner join "
      + "dim2 dim2___dim2 "
      + "on ((( fact___f . dim2_id ) = ( dim2___dim2 . id )) and (( fact___f . dim3_id ) = ( dim2___dim2 . id ))) "
      + " where ((( dim1___dim1 . date ) =  '2014-11-25 00:00:00' ) and ( fact___f . m4 ) is not null ) "
      + "group by ( dim1___dim1 . date ), "
      + "( dim2___dim2 . name ) order by dim1_date  asc";

    compareQueries(expected, actual);
  }

  /**
   * Test replace db name.
   *
   * @throws Exception the exception
   */
  @Test
  public void testReplaceDBName() throws Exception {
    File jarDir = new File("testdata");
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
    createTable(hconf, "mydb", "mytable_2", "testDB", "testTable_2");
    createTable(hconf, "default", "mytable_3", "testDB", "testTable_3");

    String query = "SELECT * FROM mydb.mytable t1 JOIN mytable_2 t2 ON t1.t2id = t2.id "
      + " left outer join default.mytable_3 t3 on t2.t3id = t3.id " + "WHERE A = 100";

    ColumnarSQLRewriter rewriter = new ColumnarSQLRewriter();
    rewriter.init(conf);
    rewriter.ast = HQLParser.parseHQL(query, hconf);
    rewriter.query = query;
    rewriter.analyzeInternal(conf, hconf);

    String joinTreeBeforeRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println(joinTreeBeforeRewrite);

    // Rewrite
    rewriter.replaceWithUnderlyingStorage(hconf, rewriter.fromAST);
    String joinTreeAfterRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println("joinTreeAfterRewrite:" + joinTreeAfterRewrite);

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
    createTable(hconf, "mydb", "mytable_4", null, null);
    String query2 = "SELECT * FROM mydb.mytable_4 WHERE a = 100";
    rewriter.ast = HQLParser.parseHQL(query2, hconf);
    rewriter.query = query2;
    rewriter.analyzeInternal(conf, hconf);

    joinTreeBeforeRewrite = HQLParser.getString(rewriter.fromAST);
    System.out.println(joinTreeBeforeRewrite);

    // Rewrite
    rewriter.replaceWithUnderlyingStorage(hconf, rewriter.fromAST);
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
    rewriter.replaceWithUnderlyingStorage(hconf, rewriter.fromAST);
    joinTreeAfterRewrite = HQLParser.getString(rewriter.fromAST);
    assertTrue(joinTreeBeforeRewrite.contains("examples"), joinTreeBeforeRewrite);
    assertFalse(joinTreeAfterRewrite.contains("examples"), joinTreeAfterRewrite);
    System.out.println("default case: " + joinTreeAfterRewrite);

    Hive.get().dropTable("examples", "mytable");
    Hive.get().dropTable("mydb", "mytable_2");
    Hive.get().dropTable("default", "mytable_3");
    Hive.get().dropTable("mydb", "mytable_4");
    Hive.get().dropDatabase("mydb", true, true, true);
    Hive.get().dropDatabase("examples", true, true, true);
    SessionState.get().setCurrentDatabase("default");
  }

  /**
   * Creates the table.
   *
   * @param db     the db
   * @param table  the table
   * @param udb    the udb
   * @param utable the utable
   * @throws Exception the exception
   */
  void createTable(HiveConf conf, String db, String table, String udb, String utable) throws Exception {
    Table tbl1 = new Table(db, table);
    tbl1.setSerializationLib("DatabaseJarSerde");
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

    Hive.get(conf).createTable(tbl1);
    System.out.println("Created table " + table);
  }
}
