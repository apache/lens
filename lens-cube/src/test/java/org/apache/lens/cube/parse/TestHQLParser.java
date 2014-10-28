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

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.lens.cube.parse.HQLParser;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHQLParser {
  @Test
  public void testGroupByOrderByGetString() throws Exception {
    String query = "SELECT a,b, sum(c) FROM tab GROUP BY a,f(b), d+e ORDER BY a, g(b), e/100";
    ASTNode node = HQLParser.parseHQL(query);

    ASTNode groupby = HQLParser.findNodeByPath(node, TOK_INSERT, TOK_GROUPBY);
    String expected = "a , f( b ), ( d  +  e )";
    Assert.assertEquals(expected, HQLParser.getString(groupby).trim());

    ASTNode orderby = HQLParser.findNodeByPath(node, TOK_INSERT, HiveParser.TOK_ORDERBY);
    String expectedOrderBy = "a  asc , g( b )  asc ,  e  /  100   asc";
    System.out.println("###Actual order by:" + HQLParser.getString(orderby).trim());
    Assert.assertEquals(expectedOrderBy, HQLParser.getString(orderby).trim());
  }

  @Test
  public void testLiteralCaseIsPreserved() throws Exception {
    String literalQuery = "SELECT 'abc' AS col1, 'DEF' AS col2 FROM foo where col3='GHI' " + "AND col4 = 'JKLmno'";

    ASTNode tree = HQLParser.parseHQL(literalQuery);

    ASTNode select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    String selectStr = HQLParser.getString(select).trim();
    String expectedSelect = "'abc'  col1 ,  'DEF'  col2";
    Assert.assertEquals(expectedSelect, selectStr);

    ASTNode where = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where).trim();
    String expectedWhere = "(( col3  =  'GHI' ) and ( col4  =  'JKLmno' ))";
    Assert.assertEquals(expectedWhere, whereStr);
  }

  @Test
  public void testCaseStatementGetString() throws Exception {
    String query =
        "SELECT  " + "CASE (col1 * 100)/200 + 5 " + "WHEN 'ABC' THEN 'def' " + "WHEN 'EFG' THEN 'hij' " + "ELSE 'XyZ' "
            + "END AS ComplexCaseStatement FROM FOO";

    ASTNode tree = HQLParser.parseHQL(query);
    ASTNode select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    String selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause ");
    System.out.println(selectStr);
    Assert.assertEquals("case ((( col1  *  100 ) /  200 ) +  5 ) "
        + "when  'ABC'  then  'def'  when  'EFG'  then  'hij'  " + "else  'XyZ'  end  complexcasestatement",
        selectStr.trim());

    String q2 =
        "SELECT " + "CASE WHEN col1 = 'abc' then 'def' " + "when col1 = 'ghi' then 'jkl' "
            + "else 'none' END AS Complex_Case_Statement_2" + " from FOO";

    tree = HQLParser.parseHQL(q2);
    select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause 2");
    System.out.println(selectStr);
    Assert.assertEquals("case  when ( col1  =  'abc' ) then  'def'  " + "when ( col1  =  'ghi' ) then  'jkl'  "
        + "else  'none'  end  complex_case_statement_2", selectStr.trim());

    String q3 =
        "SELECT  " + "CASE (col1 * 100)/200 + 5 " + "WHEN 'ABC' THEN 'def' " + "WHEN 'EFG' THEN 'hij' "
            + "END AS ComplexCaseStatement FROM FOO";

    tree = HQLParser.parseHQL(q3);
    select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause ");
    System.out.println(selectStr);
    Assert.assertEquals("case ((( col1  *  100 ) /  200 ) +  5 ) " + "when  'ABC'  then  'def'  when  'EFG'  "
        + "then  'hij'  end  complexcasestatement", selectStr.trim());

    String q4 =
        "SELECT " + "CASE WHEN col1 = 'abc' then 'def' " + "when col1 = 'ghi' then 'jkl' "
            + "END AS Complex_Case_Statement_2" + " from FOO";

    tree = HQLParser.parseHQL(q4);
    select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause 2");
    System.out.println(selectStr);
    Assert.assertEquals("case  when ( col1  =  'abc' ) then  " + "'def'  when ( col1  =  'ghi' ) then  'jkl'  "
        + "end  complex_case_statement_2", selectStr.trim());

  }

  @Test
  public void testIsNullCondition() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE col1 IS NULL";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("col1  is null", whereStr.trim());
  }

  @Test
  public void testIsNotNullCondition() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE col1 IS NOT NULL";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("col1  is not null", whereStr.trim());
  }

  @Test
  public void testBetweenCondition() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE col1 BETWEEN 10 AND 100";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("col1  between  10  and  100", whereStr.trim());
  }

  @Test
  public void testNotBetweenCondition() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE col1 NOT BETWEEN 10 AND 100";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("col1  not between  10  and  100", whereStr.trim());
  }

  @Test
  public void testBinaryOperators() throws Exception {
    String q1 =
        "SELECT * FROM FOO WHERE " + "(A <=> 10) AND (B & C = 10) AND (D | E = 10) "
            + "AND (F ^ G = 10) AND (H % 2 = 1) AND  (~I = 10)" + "AND (!J) AND (NOT K) AND TRUE AND FALSE";

    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    String expected =
        "(((((((((( a  <=>  10 ) and (( b  &  c ) =  10 )) " + "and (( d  |  e ) =  10 )) and (( f  ^  g ) =  10 )) "
            + "and (( h  %  2 ) =  1 )) and ( ~  i  =  10 )) and  not  j ) " + "and  not  k ) and  true ) and  false )";
    System.out.println(whereStr);
    Assert.assertEquals(expected, whereStr.trim());
  }

  @Test
  public void testCompelxTypeOperators() throws Exception {
    String q1 = "SELECT A[2], B['key'], C.D FROM FOO";

    ASTNode select = HQLParser.findNodeByPath(HQLParser.parseHQL(q1), TOK_INSERT, TOK_SELECT);
    String selectStr = HQLParser.getString(select);
    System.out.println(selectStr);
    Assert.assertEquals("a [ 2 ],  b [ 'key' ], ( c . d )", selectStr.trim());
  }

  @Test
  public void testInAndNotInOperator() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE A IN ('B', 'C', 'D', 'E', 'F')";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("a  in ( 'B'  ,  'C'  ,  'D'  ,  'E'  ,  'F' )", whereStr.trim());

    q1 = "SELECT * FROM FOO WHERE A NOT IN ('B', 'C', 'D', 'E', 'F')";
    where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1), TOK_INSERT, TOK_WHERE);
    whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("a  not  in ( 'B'  ,  'C'  ,  'D'  ,  'E'  ,  'F' )", whereStr.trim());
  }

  @Test
  public void testOrderbyBrackets() throws Exception {
    String query = "SELECT id from citytable order by ((citytable.id) asc)";
    // String hql = rewrite(driver, query);
    ASTNode tree = HQLParser.parseHQL(query);
    ASTNode orderByTree = HQLParser.findNodeByPath(tree, TOK_INSERT, HiveParser.TOK_ORDERBY);
    String reconstructed = HQLParser.getString(orderByTree);
    System.out.println("RECONSTRUCTED0:" + reconstructed);
    // Assert.assertEquals("(( citytable  .  id ) asc )", reconstructed);
    HQLParser.parseHQL("SELECT citytable.id FROM citytable ORDER BY " + reconstructed);

    String query2 = "SELECT id from citytable order by (citytable.id asc)";
    tree = HQLParser.parseHQL(query2);
    orderByTree = HQLParser.findNodeByPath(tree, TOK_INSERT, HiveParser.TOK_ORDERBY);
    reconstructed = HQLParser.getString(orderByTree);
    System.out.println("RECONSTRUCTED1:" + reconstructed);
    HQLParser.parseHQL("SELECT citytable.id FROM citytable ORDER BY " + reconstructed);

    String query3 = "SELECT id, name from citytable order by citytable.id asc, citytable.name desc";
    tree = HQLParser.parseHQL(query3);
    orderByTree = HQLParser.findNodeByPath(tree, TOK_INSERT, HiveParser.TOK_ORDERBY);
    reconstructed = HQLParser.getString(orderByTree);
    System.out.println("RECONSTRUCTED2:" + reconstructed);
    HQLParser.parseHQL("SELECT id, name FROM citytable ORDER BY " + reconstructed);

    String query4 = "SELECT id from citytable order by citytable.id";
    tree = HQLParser.parseHQL(query4);
    orderByTree = HQLParser.findNodeByPath(tree, TOK_INSERT, HiveParser.TOK_ORDERBY);
    reconstructed = HQLParser.getString(orderByTree);
    System.out.println("RECONSTRUCTED3:" + reconstructed);
    HQLParser.parseHQL("SELECT citytable.id FROM citytable ORDER BY " + reconstructed);
  }

  @Test
  public void testInnerJoin() throws Exception {
    String query =
        "select tab1.a, tab2.b from table1 tab1 inner join table tab2 on tab1.id = tab2.id where tab1.a > 123";
    ASTNode node = HQLParser.parseHQL(query);
    ASTNode temp = HQLParser.findNodeByPath(node, TOK_FROM, TOK_JOIN);
    String expected = " table1  tab1  table  tab2 (( tab1 . id ) = ( tab2 . id ))";
    Assert.assertEquals(expected, HQLParser.getString(temp));
  }

  @Test
  public void testAliasWithSpaces() throws Exception {
    String query = "select id as `an id` from sample_dim";
    try {
      ASTNode tree = HQLParser.parseHQL(query);
    } catch (NullPointerException exc) {
      exc.printStackTrace();
      Assert.fail("should not have thrown npe");
    }
  }

  @Test
  public void testAllColumns() throws Exception {
    String query = "select * from tab";
    ASTNode select = HQLParser.findNodeByPath(HQLParser.parseHQL(query), TOK_INSERT, TOK_SELECT);
    String selectStr = HQLParser.getString(select);
    System.out.println(selectStr);
    Assert.assertEquals(" * ", selectStr);

    query = "select tab.*, tab2.a, tab2.b from tab";
    ASTNode ast = HQLParser.parseHQL(query);
    select = HQLParser.findNodeByPath(ast, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println(selectStr);
    Assert.assertEquals(" tab . * , ( tab2 . a ), ( tab2 . b )", selectStr);

    query = "select count(*) from tab";
    ast = HQLParser.parseHQL(query);
    select = HQLParser.findNodeByPath(ast, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println(selectStr);
    Assert.assertEquals(" count(*) ", selectStr);

    query = "select count(tab.*) from tab";
    ast = HQLParser.parseHQL(query);
    select = HQLParser.findNodeByPath(ast, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println(selectStr);
    Assert.assertEquals("count( tab . * )", selectStr);
  }

}
