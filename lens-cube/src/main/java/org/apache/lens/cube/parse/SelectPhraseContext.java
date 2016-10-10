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

import org.apache.hadoop.hive.ql.parse.ASTNode;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
class SelectPhraseContext extends QueriedPhraseContext {
  private String actualAlias;
  private String selectAlias;
  private String finalAlias;
  private String exprWithoutAlias;

  public SelectPhraseContext(ASTNode selectExpr) {
    super(selectExpr);
  }

  String getExprWithoutAlias() {
    if (exprWithoutAlias == null) {
      //Order of Children of select expression AST Node => Index 0: Select Expression Without Alias, Index 1: Alias */
      exprWithoutAlias = HQLParser.getString((ASTNode) getExprAST().getChild(0)).trim();
    }
    return exprWithoutAlias;
  }

  void updateExprs() {
    super.updateExprs();
    exprWithoutAlias = HQLParser.getString((ASTNode) getExprAST().getChild(0)).trim();
  }

}
