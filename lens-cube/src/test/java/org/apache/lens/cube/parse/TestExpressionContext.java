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

import static org.apache.lens.cube.metadata.DateFactory.TWO_DAYS_RANGE;

import java.util.HashSet;
import java.util.Set;

import org.apache.lens.cube.parse.ExpressionResolver.ExprSpecContext;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestExpressionContext extends TestQueryRewrite {

  private Configuration conf;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Test
  public void testNestedExpressions() throws Exception {
    CubeQueryContext nestedExprQL = rewriteCtx("select nestedexpr from testCube where " + TWO_DAYS_RANGE, conf);
    Assert.assertNotNull(nestedExprQL.getExprCtx());
    Set<String> expectedExpressions = Sets.newHashSet(
      "avg(( testcube . roundedmsr2 ))",
      "avg(( testcube . equalsums ))",
      "case when (( testcube . substrexpr ) = 'xyz' ) then avg(( testcube . msr5 ))"
        + " when (( testcube . substrexpr ) = 'abc' ) then (avg(( testcube . msr4 )) / 100 ) end",
      "avg(round((( testcube . msr2 ) / 1000 )))",
      "avg((( testcube . msr3 ) + ( testcube . msr4 )))",
      "avg(((( testcube . msr3 ) + ( testcube . msr2 )) / 100 ))",
      "case when (substr(( testcube . dim1 ), 3 ) = 'xyz' ) then avg(( testcube . msr5 ))"
        + " when (substr(( testcube . dim1 ), 3 ) = 'abc' ) then (avg(( testcube . msr4 )) / 100 ) end",
      "case when (substr(ascii(( dim2chain . name )), 3 ) = 'xyz' ) then"
        + " avg(( testcube . msr5 )) when (substr(ascii(( dim2chain . name )), 3 ) = 'abc' ) then"
        + " (avg(( testcube . msr4 )) / 100 ) end",
      "case when (substr(( testcube . dim1 ), 3 ) = 'xyz' ) then avg((( testcube . msr2 )"
        + " + ( testcube . msr3 ))) when (substr(( testcube . dim1 ), 3 ) = 'abc' ) then"
        + " (avg(( testcube . msr4 )) / 100 ) end",
      "case when (substr(ascii(( dim2chain . name )), 3 ) = 'xyz' ) then"
        + " avg((( testcube . msr2 ) + ( testcube . msr3 ))) when (substr(ascii(( dim2chain . name )), 3 ) = 'abc' )"
        + " then (avg(( testcube . msr4 )) / 100 ) end",
      "case when (( testcube . substrexpr ) = 'xyz' ) then avg((( testcube . msr2 )"
        + " + ( testcube . msr3 ))) when (( testcube . substrexpr ) = 'abc' ) then (avg(( testcube . msr4 )) / 100 )"
        + " end",
      "case when (substr(( testcube . dim1 ), 3 ) = 'xyz' ) then avg((( testcube . msr2 )"
        + " + ( testcube . msr3 ))) when (substr(( testcube . dim1 ), 3 ) = 'abc' ) then"
        + " (avg(( testcube . msr4 )) / 100 ) end",
      "case when (substr(ascii(( dim2chain . name )), 3 ) = 'xyz' ) then"
        + " avg((( testcube . msr2 ) + ( testcube . msr3 ))) when (substr(ascii(( dim2chain . name )), 3 ) = 'abc' )"
        + " then (avg(( testcube . msr4 )) / 100 ) end"
    );

    Set<String> actualExpressions = new HashSet<>();
    for (ExprSpecContext esc : nestedExprQL.getExprCtx().getExpressionContext("nestedexpr", "testcube").getAllExprs()) {
      actualExpressions.add(HQLParser.getString(esc.getFinalAST()));
    }
    Assert.assertEquals(actualExpressions, expectedExpressions);
  }

  @Test
  public void testNestedExpressionsWithTimes() throws Exception {
    CubeQueryContext nestedExprQL = rewriteCtx("select nestedExprWithTimes from testCube where " + TWO_DAYS_RANGE,
      conf);
    Assert.assertNotNull(nestedExprQL.getExprCtx());
    Set<String> expectedExpressions = Sets.newHashSet(
      "avg(( testcube . roundedmsr2 ))",
      "avg(( testcube . equalsums ))",
      "avg(round((( testcube . msr2 ) / 1000 )))",
      "avg((( testcube . msr3 ) + ( testcube . msr4 )))",
      "avg(((( testcube . msr3 ) + ( testcube . msr2 )) / 100 ))"
    );

    Set<String> actualExpressions = new HashSet<>();
    for (ExprSpecContext esc : nestedExprQL.getExprCtx()
      .getExpressionContext("nestedexprwithtimes", "testcube").getAllExprs()) {
      actualExpressions.add(HQLParser.getString(esc.getFinalAST()));
    }
    Assert.assertEquals(actualExpressions, expectedExpressions);
  }
}
