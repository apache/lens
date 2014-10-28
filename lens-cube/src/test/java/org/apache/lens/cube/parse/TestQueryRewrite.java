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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;

public abstract class TestQueryRewrite {

  private static CubeTestSetup setup;
  private static HiveConf hconf = new HiveConf(TestJoinResolver.class);

  @BeforeSuite
  public static void setup() throws Exception {
    SessionState.start(hconf);
    setup = new CubeTestSetup();
    setup.createSources(hconf, TestQueryRewrite.class.getSimpleName());
  }

  @AfterSuite
  public static void tearDown() throws Exception {
    setup.dropSources(hconf);
  }

  @BeforeClass
  public static void setupSession() throws Exception {
    SessionState.start(hconf);
    SessionState.get().setCurrentDatabase(TestQueryRewrite.class.getSimpleName());
  }

  protected String rewrite(String query, Configuration conf) throws SemanticException, ParseException {
    System.out.println("User query:" + query);
    CubeQueryRewriter driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    CubeQueryContext rewrittenQuery = rewriteCtx(query, conf);
    return rewrittenQuery.toHQL();
  }

  protected CubeQueryContext rewriteCtx(String query, Configuration conf) throws SemanticException, ParseException {
    System.out.println("User query:" + query);
    CubeQueryRewriter driver = new CubeQueryRewriter(new HiveConf(conf, HiveConf.class));
    return driver.rewrite(query);
  }

}
