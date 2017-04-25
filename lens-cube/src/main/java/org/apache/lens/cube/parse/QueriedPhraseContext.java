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

import java.util.*;

import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Data
@EqualsAndHashCode(callSuper = true)
@Slf4j
class QueriedPhraseContext extends TracksQueriedColumns implements TrackQueriedCubeFields {
  // position in org.apache.lens.cube.parse.CubeQueryContext.queriedPhrases
  private int position;
  private final ASTNode exprAST;
  private Boolean aggregate;
  private String expr;
  private final Set<String> queriedDimAttrs = new HashSet<>();
  private final Set<String> queriedMsrs = new HashSet<>();
  private final Set<String> queriedExprColumns = new HashSet<>();
  private final Set<String> columns = new HashSet<>();

  void setNotAggregate() {
    this.aggregate = false;
  }

  boolean isAggregate() {
    if (aggregate == null) {
      aggregate = HQLParser.hasAggregate(exprAST);
    }
    return aggregate;
  }

  String getExpr() {
    if (expr == null) {
      expr = HQLParser.getString(getExprAST()).trim();
    }
    return expr;
  }

  void updateExprs() {
    expr = HQLParser.getString(getExprAST()).trim();
  }

  @Override
  public void addQueriedDimAttr(String attrName) {
    queriedDimAttrs.add(attrName);
    columns.add(attrName);
  }

  @Override
  public void addQueriedMsr(String msrName) {
    queriedMsrs.add(msrName);
    columns.add(msrName);
  }

  @Override
  public void addQueriedExprColumn(String exprCol) {
    queriedExprColumns.add(exprCol);
    columns.add(exprCol);
  }

  public boolean hasMeasures(CubeQueryContext cubeQl) {
    if (!queriedMsrs.isEmpty()) {
      return true;
    }
    if (!queriedExprColumns.isEmpty()) {
      for (String exprCol : queriedExprColumns) {
        if (cubeQl.getQueriedExprsWithMeasures().contains(exprCol)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * @param candidate
   * @return
   * @throws LensException
   */
  public boolean isEvaluable(StorageCandidate candidate) throws LensException {
    // all measures of the queried phrase should be present
    for (String msr : queriedMsrs) {
      if (!candidate.isColumnPresentAndValidForRange(msr)) {
        return false;
      }
    }
    // all expression columns should be evaluable
    for (String exprCol : queriedExprColumns) {
      if (!candidate.isExpressionEvaluable(exprCol)) {
        log.info("expression {} is not evaluable in fact table:{}", expr, candidate);
        return false;
      }
    }
    // all dim-attributes should be present.
    for (String col : queriedDimAttrs) {
      if (!candidate.getColumns().contains(col.toLowerCase())) {
        // check if it available as reference
        if (!candidate.isDimAttributeEvaluable(col)) {
          log.info("column {} is not available in fact table:{} ", col, candidate);
          return false;
        }
      } else if (!candidate.isColumnValidForRange(col)) {
        log.info("column {} is not available in range queried in fact {}", col, candidate);
        return false;
      }
    }
    return true;
  }
}
