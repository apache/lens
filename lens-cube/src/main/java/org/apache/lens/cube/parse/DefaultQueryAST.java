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

import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DefaultQueryAST implements QueryAST {
  private ASTNode selectAST, whereAST, groupByAST, havingAST, joinAST, orderByAST;
  private Integer limitValue;

  public String getSelectTree() {
    return HQLParser.getString(selectAST);
  }

  public String getWhereTree() {
    if (whereAST != null) {
      return HQLParser.getString(whereAST);
    }
    return null;
  }

  public String getGroupByTree() {
    if (groupByAST != null) {
      return HQLParser.getString(groupByAST);
    }
    return null;
  }


  public String getHavingTree() {
    if (havingAST != null) {
      return HQLParser.getString(havingAST);
    }
    return null;
  }

  @Override
  public String getOrderByTree() {
    if (orderByAST != null) {
      return HQLParser.getString(orderByAST);
    }
    return null;
  }

  public static DefaultQueryAST fromCandidateFact(CandidateFact fact, String storageTable, QueryAST ast) throws
    LensException {
    return new DefaultQueryAST(ast.getSelectAST(),
      fact.getStorageWhereClause(storageTable.substring(storageTable.indexOf(".") + 1)),
      ast.getGroupByAST(), ast.getHavingAST(), ast.getJoinAST(), ast.getOrderByAST(), ast.getLimitValue());
  }
}
