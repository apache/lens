package com.inmobi.grill.driver.jdbc;

/*
 * #%L
 * Grill Driver for JDBC
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
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
 * #L%
 */

import java.util.*;

import com.inmobi.grill.server.api.GrillConfConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreConstants;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.testng.Assert;
//import org.junit.AfterClass;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestColumnarSQLRewriter {

  private Set<String> setOf(String ... args) {
    Set<String> result = new HashSet<String>();
    for (String s : args) {
      result.add(s.replaceAll("\\s+", ""));
    }
    return result;
  }

  private Set<String> setOf(Collection<String> collection) {
    Set<String> result = new HashSet<String>();
    for (String s : collection) {
      result.add(s.replaceAll("\\s+", ""));
    }
    return result;
  }

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

      System.err.println("__FAILED__ " + method + "\n\tExpected: " + expected
          + "\n\t---------\n\tActual: " + actual);
    }
    Assert.assertTrue(expectedTrimmed.equalsIgnoreCase(actualTrimmed));
  }

  /*
   * Star schema used for the queries below
   * 
   * create table sales_fact (time_key integer, item_key integer, branch_key
   * integer, location_key integer, dollars_sold double, units_sold integer);
   * 
   * create table time_dim ( time_key integer, day datetime, day_of_week
   * integer, month integer, quarter integer, year integer );
   * 
   * create table item_dim ( item_key integer, item_name varchar(500) );
   * 
   * create table branch_dim ( branch_key integer, branch_name varchar(100));
   * 
   * create table location_dim (location_key integer,location_name
   * varchar(100));
   */

  @Test
  public void testJoinCond() throws ParseException, SemanticException,
      GrillException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
        + "from sales_fact fact "
        + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day "
        + "order by dollars_sold desc ";

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

  @Test
  public void testAllFilterCond() throws ParseException, SemanticException,
      GrillException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
        + "from sales_fact fact "
        + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day "
        + "order by dollars_sold desc ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String rwq = qtest.rewrite(conf, query);
    Set<String> actual = setOf(qtest.rightFilter);
    Assert.assertEquals(actual, setOf("(( location_dim  .  location_name ) =  'test123' )",
      "( time_dim  .  time_key ) between  '2013-01-01'  and  '2013-01-31'", ""));
  }

  @Test
  public void testAllAggColumn() throws ParseException, SemanticException,
      GrillException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold, "
        + "sum(fact.units_sold),avg(fact.dollars_sold),min(fact.dollars_sold),max(fact.dollars_sold)"
        + "from sales_fact fact "
        + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day "
        + "order by dollars_sold desc ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String rwq = qtest.rewrite(conf, query);
    Set<String> aggrActual = setOf(qtest.aggColumn);
    Set<String> expectedAggr = setOf("sum(( fact  .  units_sold )) as sum_fact_units_sold",
      "min(( fact  .  dollars_sold )) as min_fact_dollars_sold",
      "avg(( fact  .  dollars_sold )) as avg_fact_dollars_sold",
      "sum(( fact  .  dollars_sold )) as sum_fact_dollars_sold",
      "max(( fact  .  dollars_sold )) as max_fact_dollars_sold"
      );
    Assert.assertEquals(aggrActual, expectedAggr);
  }

  @Test
  public void testAllFactKeys() throws ParseException, SemanticException,
      GrillException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key, "
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold, "
        + "sum(fact.units_sold),avg(fact.dollars_sold),min(fact.dollars_sold),max(fact.dollars_sold)"
        + "from sales_fact fact "
        + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "inner join item_dim item_dim on fact.item_key = item_dim.item_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key "
        + "order by dollars_sold desc ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String rwq = qtest.rewrite(conf, query);
    String expected = "fact.time_key,fact.location_key,fact.item_key,";
    String actual = qtest.factKeys.toString();
    compareQueries(expected, actual);
  }

  @Test
  public void testFactSubQueries() throws ParseException, SemanticException,
      GrillException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key, "
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold, "
        + "sum(fact.units_sold),avg(fact.dollars_sold),min(fact.dollars_sold),max(fact.dollars_sold)"
        + "from sales_fact fact "
        + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "inner join item_dim item_dim on fact.item_key = item_dim.item_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
        + "and item_dim.item_name = 'item_1' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key "
        + "order by dollars_sold desc ";

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

  @Test
  public void testRewrittenQuery() throws ParseException, SemanticException,
      GrillException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key, "
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold, "
        + "sum(fact.units_sold),avg(fact.dollars_sold),min(fact.dollars_sold),max(fact.dollars_sold)"
        + "from sales_fact fact "
        + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "inner join item_dim item_dim on fact.item_key = item_dim.item_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-31' "
        + "and item_dim.item_name = 'item_1' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day,item_dim.item_key "
        + "order by dollars_sold  ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String rwq = qtest.rewrite(conf, query);
    String expected = "select ( fact  .  time_key ), ( time_dim  .  day_of_week ), ( time_dim  .  day ), "
        + "( item_dim  .  item_key ),  case  when (sum(sum_fact_dollars_sold) =  0 ) then  0.0  "
        + "else sum(sum_fact_dollars_sold) end dollars_sold , sum(sum_fact_units_sold), avg(avg_fact_dollars_sold), "
        + "min(min_fact_dollars_sold), max(max_fact_dollars_sold) "
        + "from  (select fact.time_key,fact.location_key,fact.item_key,"
        + "sum(( fact . dollars_sold )) as sum_fact_dollars_sold, sum(( fact . units_sold )) as sum_fact_units_sold," +
      " avg(( fact . dollars_sold )) as avg_fact_dollars_sold, " +
      "min(( fact . dollars_sold )) as min_fact_dollars_sold, max(( fact . dollars_sold )) as max_fact_dollars_sold"
        + "from sales_fact fact where fact.time_key in  (  select time_dim.time_key from time_dim "
        + "where ( time_dim  .  time_key ) between  '2013-01-01'  and  '2013-01-31'  ) and "
        + "fact.location_key in  (  select location_dim.location_key from location_dim "
        + "where (( location_dim  .  location_name ) =  'test123' ) ) and fact.item_key "
        + "in  (  select item_dim.item_key from item_dim where (( item_dim  .  item_name ) =  'item_1' ) )  "
        + "group by fact.time_key,fact.location_key,fact.item_key) fact inner join item_dim  item_dim  "
        + "on ((( fact  .  item_key ) = ( item_dim  .  item_key )) and (( location_dim  .  location_name ) =  'test123' )) "
        + "inner join location_dim  location_dim  on (( fact  .  location_key ) = ( location_dim  .  location_key )) "
        + "inner join time_dim  time_dim  on (( fact  .  time_key ) = ( time_dim  .  time_key )) "
        + "where (( time_dim  .  time_key ) between  '2013-01-01'  and  '2013-01-31'  "
        + "and (( item_dim  .  item_name ) =  'item_1' )) "
        + "group by ( fact  .  time_key ), ( time_dim  .  day_of_week ), ( time_dim  .  day ), ( item_dim  .  item_key ) "
        + "order by  dollars_sold asc ";
    String actual = qtest.finalRewrittenQuery;
    compareQueries(expected, actual);
  }

  @Test
  public void testUnionQuery() throws ParseException, SemanticException,
      GrillException {

    String query =

    "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
        + "from sales_fact fact "
        + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-01-01' and '2013-01-05' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day "
        + "order by dollars_sold  "
        + "union all"
        + "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
        + "from sales_fact fact "
        + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-02-01' and '2013-02-05' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day "
        + "order by dollars_sold "
        + "union all"
        + "select fact.time_key,time_dim.day_of_week,time_dim.day,"
        + "case when sum(fact.dollars_sold) = 0 then 0.0 else sum(fact.dollars_sold) end dollars_sold "
        + "from sales_fact fact "
        + "inner join time_dim time_dim on fact.time_key = time_dim.time_key "
        + "inner join location_dim location_dim on fact.location_key = location_dim.location_key "
        + "and location_dim.location_name = 'test123' "
        + "where time_dim.time_key between '2013-03-01' and '2013-03-05' "
        + "group by fact.time_key,time_dim.day_of_week,time_dim.day "
        + "order by dollars_sold ";

    SessionState.start(new HiveConf(ColumnarSQLRewriter.class));

    HiveConf conf = new HiveConf();
    ColumnarSQLRewriter qtest = new ColumnarSQLRewriter();

    String rwq = qtest.rewrite(conf, query);
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
    String actual = qtest.finalRewrittenQuery.toString();
    compareQueries(expected, actual);
  }

  @Test
  public void testReplaceDBName() throws Exception {
    HiveConf conf = new HiveConf(ColumnarSQLRewriter.class);
    conf.setBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, false);
    SessionState.start(conf);

    // Create test table
    createTable("default", "mytable", "testDB", "testTable_1");
    createTable("default", "mytable_2", "testDB", "testTable_2");
    createTable("default", "mytable_3", "testDB", "testTable_3");

    String query = "SELECT * FROM mydb.mytable t1 JOIN mydb.mytable_2 t2 ON t1.t2id = t2.id " +
      " left outer join mytable_3 t3 on t2.t3id = t3.id " +
      "WHERE A = 100";


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
    assertTrue(joinTreeBeforeRewrite.contains("mytable")
        && joinTreeBeforeRewrite.contains("mytable_2")
        && joinTreeBeforeRewrite.contains("mytable_3")
    );

    assertFalse(joinTreeAfterRewrite.contains("mydb"));
    assertFalse(joinTreeAfterRewrite.contains("mytable")
        && joinTreeAfterRewrite.contains("mytable_2")
        && joinTreeAfterRewrite.contains("mytable_3")
    );

    assertTrue(joinTreeAfterRewrite.contains("testdb"));
    assertTrue(joinTreeAfterRewrite.contains("testtable_1")
        && joinTreeAfterRewrite.contains("testtable_2")
        && joinTreeAfterRewrite.contains("testtable_3")
    );

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

    //  Test a query with default db
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

  void createTable(String db, String table, String udb, String utable) throws Exception {
    Table tbl1 = new Table(db, table);

    if (StringUtils.isNotBlank(udb)) {
      tbl1.setProperty(GrillConfConstants.GRILL_NATIVE_DB_NAME, udb);
    }
    if (StringUtils.isNotBlank(utable)) {
      tbl1.setProperty(GrillConfConstants.GRILL_NATIVE_TABLE_NAME, utable);
    }

    List<FieldSchema> columns = new ArrayList<FieldSchema>();
    columns.add(new FieldSchema("id", "int", "col1"));
    columns.add(new FieldSchema("name", "string", "col2"));
    tbl1.setFields(columns);

    Hive.get().createTable(tbl1);
    System.out.println("Created table " + table);
  }
}
