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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class TestQueryRewrite {

  private static CubeTestSetup setup;
  private static HiveConf hconf = new HiveConf(TestQueryRewrite.class);

  @BeforeSuite
  public static void setup() throws Exception {
    SessionState.start(hconf);
    setup = new CubeTestSetup();
    setup.createSources(hconf, TestQueryRewrite.class.getSimpleName());
  }

  @AfterSuite
  public static void tearDown() throws Exception {
    setup.dropSources(hconf, TestQueryRewrite.class.getSimpleName());
  }

  @BeforeClass
  public static void setupSession() throws Exception {
    SessionState.start(hconf);
    SessionState.get().setCurrentDatabase(TestQueryRewrite.class.getSimpleName());
  }

  protected String rewrite(String query, Configuration conf) throws SemanticException, ParseException {
    String rewrittenQuery = rewriteCtx(query, conf).toHQL();
    log.info("Rewritten query: {}", rewrittenQuery);
    return rewrittenQuery;
  }

  protected CubeQueryContext rewriteCtx(String query, Configuration conf) throws SemanticException, ParseException {
    log.info("User query: {}", query);
    CubeQueryRewriter driver = new CubeQueryRewriter(conf, hconf);
    return driver.rewrite(query);
  }

  static PruneCauses.BriefAndDetailedError extractPruneCause(SemanticException e) {
    try {
      return new ObjectMapper().readValue(
        e.getMessage().substring(e.getMessage().indexOf("{"), e.getMessage().length()),
        new TypeReference<PruneCauses.BriefAndDetailedError>() {});
    } catch (IOException e1) {
      throw new RuntimeException("!!!");
    }
  }

  protected SemanticException getSemanticExceptionInRewrite(String query, Configuration conf) throws ParseException {
    try {
      String hql = rewrite(query, conf);
      Assert.fail("Should have thrown exception. But rewrote the query : " + hql);
      // unreachable
      return null;
    } catch (SemanticException e) {
      e.printStackTrace();
      return e;
    }
  }
}
