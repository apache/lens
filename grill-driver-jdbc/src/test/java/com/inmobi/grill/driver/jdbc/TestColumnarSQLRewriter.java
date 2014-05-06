package com.inmobi.grill.driver.jdbc;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.parse.HQLParser;
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
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillException;

public class TestColumnarSQLRewriter {

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
      // System.err.println("\t__AGGR_EXPRS:" +
      // rewrittenQuery.getAggregateExprs());
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
    String expected = "[, (( location_dim  .  location_name ) =  'test123' ), "
        + "( time_dim  .  time_key ) between  '2013-01-01'  and  '2013-01-31' ]";
    Set<String> setAllFilters = new HashSet<String>(qtest.rightFilter);
    String actual = setAllFilters.toString();
    compareQueries(expected, actual);
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
    String expected = "[sum(( fact  .  units_sold )) as sum_fact_units_sold, min(( fact  .  dollars_sold )) "
        + "as min_fact_dollars_sold, avg(( fact  .  dollars_sold )) as avg_fact_dollars_sold, "
        + "sum(( fact  .  dollars_sold )) as sum_fact_dollars_sold, "
        + "max(( fact  .  dollars_sold )) as max_fact_dollars_sold]";
    String actual = qtest.aggColumn.toString();
    compareQueries(expected, actual);
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
        + "sum(( fact  .  units_sold )) as sum_fact_units_sold, min(( fact  .  dollars_sold )) as min_fact_dollars_sold,"
        + " avg(( fact  .  dollars_sold )) as avg_fact_dollars_sold, sum(( fact  .  dollars_sold )) "
        + "as sum_fact_dollars_sold, max(( fact  .  dollars_sold )) as max_fact_dollars_sold "
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
}
