/*
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

import static org.testng.Assert.assertEquals;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import org.apache.lens.cube.parse.join.JoinClause;

import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

/**
 * Created on 28/08/17.
 */
public class CubeQueryContextTest {
  private JoinClause jc;

  @BeforeClass
  public void intClass() {
    jc = Mockito.mock(JoinClause.class);
    Mockito.when(jc.getStarJoin(Mockito.anyString())).thenReturn(null);
  }

  @DataProvider
  public Object[][] testCases() {
    return new Object[][]{
      {"testcube.x=1 and (testcube.dt=yesterday or (testcube.dt=today and testcube.pt=yesterday))",
        newArrayList("((testcube.x) = 1)",
          "(((testcube.dt) = yesterday) or (((testcube.dt) = today) and ((testcube.pt) = yesterday)))"), },
      {"testcube.x=1 and (testcube.dt=yesterday and (testcube.dt=today and testcube.pt=yesterday))",
        newArrayList("((testcube.x) = 1)", "((testcube.dt) = yesterday)",
          "((testcube.dt) = today)", "((testcube.pt) = yesterday)"), },
      {"testcube.x=1 and (testcube.dt = yesterday or "
        + "(case when true and false then 1 else 0 end))",
        newArrayList("((testcube.x) = 1)",
          "(((testcube.dt) = yesterday) or case  when ( true  and  false ) then 1 else 0 end)"), },
    };
  }

  @Test(dataProvider = "testCases")
  public void testGetAllFilters(String expr, List<String> expected) throws Exception {
    List<String> allFilters = newArrayList();
    CubeQueryContext.getAllFilters(HQLParser.parseExpr(expr), "testcube", allFilters, jc, Maps.newHashMap());
    assertEquals(allFilters, expected);
  }
}
