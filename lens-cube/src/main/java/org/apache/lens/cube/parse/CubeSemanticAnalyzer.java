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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.parse.*;

import lombok.Getter;

/**
 * Accepts cube query AST and rewrites into storage table query
 */
public class CubeSemanticAnalyzer extends SemanticAnalyzer {
  private final Configuration queryConf;
  private final HiveConf hiveConf;
  private final List<ValidationRule> validationRules = new ArrayList<ValidationRule>();
  @Getter
  private QB cubeQB;

  public CubeSemanticAnalyzer(Configuration queryConf, HiveConf hiveConf) throws SemanticException {
    super(new QueryState(hiveConf));
    this.queryConf = queryConf;
    this.hiveConf = hiveConf;
    setupRules();
  }

  private void setupRules() {
    validationRules.add(new CheckTableNames(conf));
    validationRules.add(new CheckColumnMapping(conf));
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    reset(true);
    cubeQB = new QB(null, null, false);

    if (ast.getToken().getType() == HiveParser.TOK_QUERY) {
      if (((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.KW_CUBE) {
        // remove cube child from AST
        for (int i = 0; i < ast.getChildCount() - 1; i++) {
          ast.setChild(i, ast.getChild(i + 1));
        }
        ast.deleteChild(ast.getChildCount() - 1);
      }
    }
    // analyzing from the ASTNode.
    if (!doPhase1(ast, cubeQB, initPhase1Ctx(), null)) {
      // if phase1Result false return
      return;
    }
  }
}
