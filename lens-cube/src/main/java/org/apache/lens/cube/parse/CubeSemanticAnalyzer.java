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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Accepts cube query AST and rewrites into storage table query
 */
public class CubeSemanticAnalyzer extends SemanticAnalyzer {
  private final HiveConf conf;
  private final List<ValidationRule> validationRules = new ArrayList<ValidationRule>();
  private CubeQueryContext cubeQl;

  public CubeSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
    this.conf = conf;
    setupRules();
  }

  private void setupRules() {
    validationRules.add(new CheckTableNames(conf));
    validationRules.add(new CheckColumnMapping(conf));
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    reset();
    QB qb = new QB(null, null, false);
    // do not allow create table/view commands
    // TODO Move this to a validation rule
    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE || ast.getToken().getType() == HiveParser.TOK_CREATEVIEW) {
      throw new SemanticException(ErrorMsg.CREATE_NOT_ALLOWED);
    }

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
    if (!doPhase1(ast, qb, initPhase1Ctx())) {
      // if phase1Result false return
      return;
    }
    cubeQl = new CubeQueryContext(ast, qb, conf);
    // cubeQl.init();
    // validate();

    // TODO Move this to a validation Rule
    // QBParseInfo qbp = qb.getParseInfo();
    // TreeSet<String> ks = new TreeSet<String>(qbp.getClauseNames());
    // if (ks.size() > 1) {
    // throw new SemanticException("nested/sub queries not allowed yet");
    // }
    // Operator sinkOp = genPlan(qb);
    // System.out.println(sinkOp.toString());
  }

  @Override
  public void validate() throws SemanticException {
    for (ValidationRule rule : validationRules) {
      if (!rule.validate(cubeQl)) {
        break;
      }
    }
  }

  public CubeQueryContext getQueryContext() {
    return cubeQl;
  }
}
