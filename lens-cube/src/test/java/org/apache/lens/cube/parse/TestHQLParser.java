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

package org.apache.lens.cube.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestHQLParser {

  HiveConf conf = new HiveConf();
  @Test
  public void testGroupByOrderByGetString() throws Exception {
    String query = "SELECT a,b, sum(c) FROM tab GROUP BY a,f(b), d+e ORDER BY a, g(b), e/100";
    ASTNode node = HQLParser.parseHQL(query, conf);

    ASTNode groupby = HQLParser.findNodeByPath(node, TOK_INSERT, TOK_GROUPBY);
    String expected = "a , f( b ), ( d + e )";
    Assert.assertEquals(expected, HQLParser.getString(groupby).trim());

    ASTNode orderby = HQLParser.findNodeByPath(node, TOK_INSERT, HiveParser.TOK_ORDERBY);
    String expectedOrderBy = "a asc , g( b ) asc , e / 100 asc";
    System.out.println("###Actual order by:" + HQLParser.getString(orderby).trim());
    Assert.assertEquals(expectedOrderBy, HQLParser.getString(orderby).trim());
  }

  @Test
  public void testLiteralCaseIsPreserved() throws Exception {
    String literalQuery = "SELECT 'abc' AS col1, 'DEF' AS col2 FROM foo where col3='GHI' " + "AND col4 = 'JKLmno'";

    ASTNode tree = HQLParser.parseHQL(literalQuery, conf);

    ASTNode select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    String selectStr = HQLParser.getString(select).trim();
    String expectedSelect = "'abc' as `col1` , 'DEF' as `col2`";
    Assert.assertEquals(expectedSelect, selectStr);

    ASTNode where = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where).trim();
    String expectedWhere = "(( col3 = 'GHI' ) and ( col4 = 'JKLmno' ))";
    Assert.assertEquals(expectedWhere, whereStr);
  }

  @Test
  public void testCaseStatementGetString() throws Exception {
    String query = "SELECT  " + "CASE (col1 * 100)/200 + 5 " + "WHEN 'ABC' THEN 'def' " + "WHEN 'EFG' THEN 'hij' "
      + "ELSE 'XyZ' " + "END AS ComplexCaseStatement FROM FOO";

    ASTNode tree = HQLParser.parseHQL(query, conf);
    ASTNode select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    String selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause ");
    System.out.println(selectStr);
    Assert.assertEquals(selectStr.trim(), "case ((( col1 * 100 ) / 200 ) + 5 ) when 'ABC' then 'def' when 'EFG' "
      + "then 'hij' else 'XyZ' end as `ComplexCaseStatement`");

    String q2 = "SELECT " + "CASE WHEN col1 = 'abc' then 'def' " + "when col1 = 'ghi' then 'jkl' "
      + "else 'none' END AS Complex_Case_Statement_2" + " from FOO";

    tree = HQLParser.parseHQL(q2, conf);
    select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause 2");
    System.out.println(selectStr);
    Assert.assertEquals(selectStr.trim(), "case when ( col1 = 'abc' ) then 'def' when ( col1 = 'ghi' ) then 'jkl' "
      + "else 'none' end as `Complex_Case_Statement_2`");

    String q3 = "SELECT  " + "CASE (col1 * 100)/200 + 5 " + "WHEN 'ABC' THEN 'def' " + "WHEN 'EFG' THEN 'hij' "
      + "END AS ComplexCaseStatement FROM FOO";

    tree = HQLParser.parseHQL(q3, conf);
    select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause ");
    System.out.println(selectStr);
    Assert.assertEquals(selectStr.trim(), "case ((( col1 * 100 ) / 200 ) + 5 ) when 'ABC' then 'def' when 'EFG' "
      + "then 'hij' end as `ComplexCaseStatement`");

    String q4 = "SELECT " + "CASE WHEN col1 = 'abc' then 'def' " + "when col1 = 'ghi' then 'jkl' "
      + "END AS Complex_Case_Statement_2" + " from FOO";

    tree = HQLParser.parseHQL(q4, conf);
    select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause 2");
    System.out.println(selectStr);
    Assert.assertEquals(selectStr.trim(), "case when ( col1 = 'abc' ) then 'def' when ( col1 = 'ghi' ) then 'jkl' end "
      + "as `Complex_Case_Statement_2`");

  }

  @Test
  public void testIsNullCondition() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE col1 IS NULL";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1, conf), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("col1 is null", whereStr.trim());
  }

  @Test
  public void testIsNotNullCondition() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE col1 IS NOT NULL";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1, conf), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("col1 is not null", whereStr.trim());
  }

  @Test
  public void testBetweenCondition() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE col1 BETWEEN 10 AND 100";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1, conf), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("col1 between 10 and 100", whereStr.trim());
  }

  @Test
  public void testNotBetweenCondition() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE col1 NOT BETWEEN 10 AND 100";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1, conf), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("col1 not between 10 and 100", whereStr.trim());
  }

  @Test
  public void testBinaryOperators() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE " + "(A <=> 10) AND (B & C = 10) AND (D | E = 10) "
      + "AND (F ^ G = 10) AND (H % 2 = 1) AND  (~I = 10)" + "AND (!J) AND (NOT K) AND TRUE AND FALSE";

    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1, conf), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    String expected = "(( a <=> 10 ) and (( b & c ) = 10 ) and (( d | e ) = 10 ) and (( f ^ g ) = 10 ) and "
      + "(( h % 2 ) = 1 ) and ( ~ i = 10 ) and not j and not k and true and false )";
    Assert.assertEquals(whereStr.trim(), expected);
  }

  @Test
  public void testCompelxTypeOperators() throws Exception {
    String q1 = "SELECT A[2], B['key'], C.D FROM FOO";

    ASTNode select = HQLParser.findNodeByPath(HQLParser.parseHQL(q1, conf), TOK_INSERT, TOK_SELECT);
    String selectStr = HQLParser.getString(select);
    System.out.println(selectStr);
    Assert.assertEquals(selectStr.trim(), "a [ 2 ], b [ 'key' ], ( c . d )");
  }

  @Test
  public void testInAndNotInOperator() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE A IN ('B', 'C', 'D', 'E', 'F')";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1, conf), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals(whereStr.trim(), "a in ( 'B' , 'C' , 'D' , 'E' , 'F' )");

    q1 = "SELECT * FROM FOO WHERE A NOT IN ('B', 'C', 'D', 'E', 'F')";
    where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1, conf), TOK_INSERT, TOK_WHERE);
    whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals(whereStr.trim(), "a not in ( 'B' , 'C' , 'D' , 'E' , 'F' )");
  }

  @Test
  public void testOrderbyBrackets() throws Exception {
    String query = "SELECT id from citytable order by ((citytable.id) asc)";
    // String hql = rewrite(driver, query);
    ASTNode tree = HQLParser.parseHQL(query, conf);
    ASTNode orderByTree = HQLParser.findNodeByPath(tree, TOK_INSERT, HiveParser.TOK_ORDERBY);
    String reconstructed = HQLParser.getString(orderByTree);
    System.out.println("RECONSTRUCTED0:" + reconstructed);
    // Assert.assertEquals("(( citytable  .  id ) asc )", reconstructed);
    HQLParser.parseHQL("SELECT citytable.id FROM citytable ORDER BY " + reconstructed, conf);

    String query2 = "SELECT id from citytable order by (citytable.id asc)";
    tree = HQLParser.parseHQL(query2, conf);
    orderByTree = HQLParser.findNodeByPath(tree, TOK_INSERT, HiveParser.TOK_ORDERBY);
    reconstructed = HQLParser.getString(orderByTree);
    System.out.println("RECONSTRUCTED1:" + reconstructed);
    HQLParser.parseHQL("SELECT citytable.id FROM citytable ORDER BY " + reconstructed, conf);

    String query3 = "SELECT id, name from citytable order by citytable.id asc, citytable.name desc";
    tree = HQLParser.parseHQL(query3, conf);
    orderByTree = HQLParser.findNodeByPath(tree, TOK_INSERT, HiveParser.TOK_ORDERBY);
    reconstructed = HQLParser.getString(orderByTree);
    System.out.println("RECONSTRUCTED2:" + reconstructed);
    HQLParser.parseHQL("SELECT id, name FROM citytable ORDER BY " + reconstructed, conf);

    String query4 = "SELECT id from citytable order by citytable.id";
    tree = HQLParser.parseHQL(query4, conf);
    orderByTree = HQLParser.findNodeByPath(tree, TOK_INSERT, HiveParser.TOK_ORDERBY);
    reconstructed = HQLParser.getString(orderByTree);
    System.out.println("RECONSTRUCTED3:" + reconstructed);
    HQLParser.parseHQL("SELECT citytable.id FROM citytable ORDER BY " + reconstructed, conf);
  }

  @Test
  public void testInnerJoin() throws Exception {
    String query
      = "select tab1.a, tab2.b from table1 tab1 inner join table tab2 on tab1.id = tab2.id where tab1.a > 123";
    ASTNode node = HQLParser.parseHQL(query, conf);
    ASTNode temp = HQLParser.findNodeByPath(node, TOK_FROM, TOK_JOIN);
    String expected = "table1 tab1 table tab2 (( tab1 . id ) = ( tab2 . id ))";
    Assert.assertEquals(HQLParser.getString(temp), expected);
  }

  @Test
  public void testAliasWithSpaces() throws Exception {
    String query = "select id as `an id` from sample_dim";
    try {
      ASTNode tree = HQLParser.parseHQL(query, conf);
      Assert.assertNotNull(tree);
    } catch (NullPointerException exc) {
      log.error("should not have thrown npe", exc);
      Assert.fail("should not have thrown npe");
    }
  }

  @Test
  public void testAliasShouldBeQuoted() throws Exception {
    Assert.assertEquals(getSelectStrForQuery("select id as identity from sample_dim"), "id as `identity`");
    Assert.assertEquals(getSelectStrForQuery("select id as `column identity` from sample_dim"),
        "id as `column identity`");
    Assert.assertEquals(getSelectStrForQuery("select id identity from sample_dim"), "id as `identity`");
    Assert.assertEquals(getSelectStrForQuery("select id `column identity` from sample_dim"),
        "id as `column identity`");
  }

  private String getSelectStrForQuery(String query) throws Exception {
    ASTNode tree = HQLParser.parseHQL(query, conf);
    ASTNode select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    return HQLParser.getString(select).trim();
  }

  @Test
  public void testAllColumns() throws Exception {
    String query = "select * from tab";
    ASTNode select = HQLParser.findNodeByPath(HQLParser.parseHQL(query, conf), TOK_INSERT, TOK_SELECT);
    String selectStr = HQLParser.getString(select);
    System.out.println(selectStr);
    Assert.assertEquals(selectStr, "*");

    query = "select tab.*, tab2.a, tab2.b from tab";
    ASTNode ast = HQLParser.parseHQL(query, conf);
    select = HQLParser.findNodeByPath(ast, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println(selectStr);
    Assert.assertEquals(selectStr, "tab . * , ( tab2 . a ), ( tab2 . b )");

    query = "select count(*) from tab";
    ast = HQLParser.parseHQL(query, conf);
    select = HQLParser.findNodeByPath(ast, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println(selectStr);
    Assert.assertEquals("count(*)", selectStr);

    query = "select count(tab.*) from tab";
    ast = HQLParser.parseHQL(query, conf);
    select = HQLParser.findNodeByPath(ast, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println(selectStr);
    Assert.assertEquals("count( tab . * )", selectStr);
  }

  @Test
  public void testNegativeLiteral() throws Exception {
    String query1 = "select 2-1 as col1,col2 from table1";
    ASTNode tree = HQLParser.parseHQL(query1, conf);
    ASTNode selectAST = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    HQLParser.printAST(selectAST);
    String genQuery = HQLParser.getString(selectAST);
    System.out.println("genQuery1: " + genQuery);

    String query2 = "select -1 as col1,col2 from table1";
    tree = HQLParser.parseHQL(query2, conf);
    selectAST = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    HQLParser.printAST(selectAST);
    String genQuery2 = HQLParser.getString(selectAST);
    System.out.println("genQuery2: " + genQuery2);

    Assert.assertFalse(genQuery2.contains("1 -"));
    Assert.assertTrue(genQuery2.contains("- 1"));

    // Validate returned string is parseable
    HQLParser.printAST(HQLParser.findNodeByPath(HQLParser.parseHQL("SELECT " + genQuery2 + " FROM table1", conf),
      TOK_INSERT,
      TOK_SELECT));
  }

  @Test
  public void testEqualsAST() throws Exception {
    ASTNode expr1 = HQLParser.parseExpr("T1.a + T2.b - T2.c");
    ASTNode expr2 = HQLParser.parseExpr("t1.A + t2.B - t2.C");

    Assert.assertTrue(HQLParser.equalsAST(expr1, expr2));

    ASTNode literalExpr1 = HQLParser.parseExpr("A = 'FooBar'");
    ASTNode literalExpr2 = HQLParser.parseExpr("a = 'FooBar'");
    Assert.assertTrue(HQLParser.equalsAST(literalExpr1, literalExpr2));

    ASTNode literalExpr3 = HQLParser.parseExpr("A = 'fOObAR'");
    Assert.assertFalse(HQLParser.equalsAST(literalExpr1, literalExpr3));

    ASTNode literalExpr4 = HQLParser.parseExpr("A <> 'FooBar'");
    Assert.assertFalse(HQLParser.equalsAST(literalExpr1, literalExpr4));
  }

  @Test
  public void testCastStatement() throws ParseException, LensException {
    String castSelect = "cast(( a  +  b ) as tinyint), cast(( a  +  b ) as smallint), cast(( a  +  b ) as int),"
      + " cast(( a  +  b ) as bigint), cast(( a  +  b ) as float), cast(( a  +  b ) as double),"
      + " cast(( a  +  b ) as boolean), cast( a  as date), cast( b  as datetime), cast( a  as timestamp),"
      + " cast(( a  +  b ) as string), cast(( a  +  b ) as binary), cast(( a  +  b ) as decimal(3,6)),"
      + " cast(( a  +  b ) as decimal(5)), cast(( a  +  b ) as varchar(10)), cast(( a  +  b ) as char(20)),"
      + " cast( '17.29'  as decimal(4,2))";
    castSelect = "3.1415926BD";
    String query = "select " + castSelect + " from table limit 1";
    ASTNode tree = HQLParser.parseHQL(query, conf);
    System.out.println(tree.dump());
    ASTNode selectAST = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    String genQuery = HQLParser.getString(selectAST);
    Assert.assertEquals(genQuery, castSelect);
  }

  @Test
  public void testOtherStatements() throws ParseException, LensException {
    String select = "3.1415926BD";
    String query = "select " + select + " from table limit 1";
    ASTNode tree = HQLParser.parseHQL(query, conf);
    ASTNode selectAST = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    String genQuery = HQLParser.getString(selectAST);
    Assert.assertEquals(genQuery, select);
  }

  @DataProvider
  public Object[][] nAryFlatteningDataProvider() {
    return new Object[][] {
      {"a", "a"},
      {"a or b", "a or b"},
      {"a or b or c or d", "a or b or c or d"},
      {"a and b and c and d", "a and b and c and d"},
      {"a and (b or c)", "a and ( b or c )"},
      {"a and (b or c or d) and (e or f) and (g and h)", "a and ( b or c or d ) and ( e or f ) and g and h"},
      // ambiguous, but uniquely understood, or > and.
      {"a and b or c or d and e or f and g and h", "( a and b ) or c or ( d and e ) or ( f and g and h )"},
    };
  }

  @Test(dataProvider = "nAryFlatteningDataProvider")
  public void testNAryOperatorFlattening(String input, String expected) throws LensException {
    ASTNode tree = HQLParser.parseExpr(input);
    String infixString = HQLParser.getString(tree);
    Assert.assertEquals(infixString, expected);
  }
}
