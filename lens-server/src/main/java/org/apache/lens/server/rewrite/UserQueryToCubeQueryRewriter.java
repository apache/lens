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

import java.util.List;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.rewrite.Phase1Rewriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.collect.Lists;

/**
 * Rewrite user query to cube query using phase 1 rewriters specified in the server installation
 * @see Phase1Rewriter
 * @see LensConfConstants#QUERY_PHASE1_REWRITERS
 */
public class UserQueryToCubeQueryRewriter {
  List<Phase1Rewriter> phase1RewriterList = Lists.<Phase1Rewriter>newArrayList(new CubeKeywordRemover());

  public UserQueryToCubeQueryRewriter(Configuration conf) throws LensException {
    try {
      for (Class clazz : conf.getClasses(LensConfConstants.QUERY_PHASE1_REWRITERS)) {
        if (!Phase1Rewriter.class.isAssignableFrom(clazz)) {
          throw new LensException("Class " + clazz.getCanonicalName() + " is not Phase 1 Rewriter");
        }
        Phase1Rewriter rewriter = (Phase1Rewriter) clazz.newInstance();
        rewriter.init(conf);
        phase1RewriterList.add(rewriter);
      }
    } catch (Exception e) {
      throw LensException.wrap(e);
    }
  }

  public String rewriteToCubeQuery(String userQuery, Configuration conf, HiveConf hiveConf) throws LensException {
    for (Phase1Rewriter rewriter : phase1RewriterList) {
      userQuery = rewriter.rewrite(userQuery, conf, hiveConf);
    }
    return userQuery;
  }

  public void rewrite(AbstractQueryContext ctx) throws LensException {
    ctx.setPhase1RewrittenQuery(rewriteToCubeQuery(ctx.getUserQuery(), ctx.getConf(), ctx.getHiveConf()));
  }
}
