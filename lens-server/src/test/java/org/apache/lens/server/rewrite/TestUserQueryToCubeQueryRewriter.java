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
/*
 *
 */
package org.apache.lens.server.rewrite;

import static org.testng.Assert.assertEquals;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.rewrite.Phase1Rewriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestUserQueryToCubeQueryRewriter {
  public static class Rewriter1 implements Phase1Rewriter {
    public static final String PREFIX = "a";
    public static final String SUFFIX = "b";

    @Override
    public String rewrite(String query, Configuration queryConf, HiveConf metastoreConf) throws LensException {
      return PREFIX + query + SUFFIX;
    }

    @Override
    public void init(Configuration rewriteConf) {

    }
  }

  public static class Rewriter2 implements Phase1Rewriter {
    public static final String PREFIX = "c";
    public static final String SUFFIX = "d";

    @Override
    public String rewrite(String query, Configuration queryConf, HiveConf metastoreConf) throws LensException {
      return PREFIX + query + SUFFIX;
    }

    @Override
    public void init(Configuration rewriteConf) {

    }
  }

  UserQueryToCubeQueryRewriter rewriter;

  @BeforeClass
  public void setup() throws LensException {
    Configuration conf = new Configuration();
    conf.set(LensConfConstants.QUERY_PHASE1_REWRITERS, Rewriter1.class.getName() + "," + Rewriter2.class.getName());
    rewriter = new UserQueryToCubeQueryRewriter(conf);
  }

  @Test
  public void testRewriter() throws LensException {
    String query = "user query";
    assertEquals(rewriter.rewriteToCubeQuery(query, null, null),
      Rewriter2.PREFIX + Rewriter1.PREFIX + query + Rewriter1.SUFFIX + Rewriter2.SUFFIX);
  }
}
