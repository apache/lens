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

import org.apache.lens.cube.metadata.MetastoreConstants;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
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
   * @param cubeQl
   * @param sc
   * @return
   * @throws LensException
   */
  public boolean isEvaluable(CubeQueryContext cubeQl, StorageCandidate sc) throws LensException {
    // all measures of the queried phrase should be present
    for (String msr : queriedMsrs) {
      if (!checkForColumnExistsAndValidForRange(sc, msr, cubeQl)) {
        return false;
      }
    }
    // all expression columns should be evaluable
    for (String exprCol : queriedExprColumns) {
      if (!cubeQl.getExprCtx().isEvaluable(exprCol, sc)) {
        log.info("expression {} is not evaluable in fact table:{}", expr, sc);
        return false;
      }
    }
    // all dim-attributes should be present.
    for (String col : queriedDimAttrs) {
      if (!sc.getColumns().contains(col.toLowerCase())) {
        // check if it available as reference
        if (!cubeQl.getDeNormCtx().addRefUsage(cubeQl, sc, col, cubeQl.getCube().getName())) {
          log.info("column {} is not available in fact table:{} ", col, sc);
          return false;
        }
      } else if (!isFactColumnValidForRange(cubeQl, sc, col)) {
        log.info("column {} is not available in range queried in fact {}", col, sc);
        return false;
      }
    }
    return true;
  }

  private static boolean isColumnAvailableInRange(final TimeRange range, Date startTime, Date endTime) {
    return (isColumnAvailableFrom(range.getFromDate(), startTime)
        && isColumnAvailableTill(range.getToDate(), endTime));
  }

  private static boolean isColumnAvailableFrom(@NonNull final Date date, Date startTime) {
    return (startTime == null) || date.equals(startTime) || date.after(startTime);
  }

  private static boolean isColumnAvailableTill(@NonNull final Date date, Date endTime) {
    return (endTime == null) || date.equals(endTime) || date.before(endTime);
  }

  public static boolean isFactColumnValidForRange(CubeQueryContext cubeql, StorageCandidate sc, String col) {
    for (TimeRange range : cubeql.getTimeRanges()) {
      if (!isColumnAvailableInRange(range, getFactColumnStartTime(sc, col), getFactColumnEndTime(sc, col))) {
        return false;
      }
    }
    return true;
  }

  public static Date getFactColumnStartTime(StorageCandidate sc, String factCol) {
    Date startTime = null;
    for (String key : sc.getTable().getProperties().keySet()) {
      if (key.contains(MetastoreConstants.FACT_COL_START_TIME_PFX)) {
        String propCol = StringUtils.substringAfter(key, MetastoreConstants.FACT_COL_START_TIME_PFX);
        if (factCol.equals(propCol)) {
          startTime = sc.getTable().getDateFromProperty(key, false, true);
        }
      }
    }
    return startTime;
  }

  public static Date getFactColumnEndTime(StorageCandidate sc, String factCol) {
    Date endTime = null;
    for (String key : sc.getTable().getProperties().keySet()) {
      if (key.contains(MetastoreConstants.FACT_COL_END_TIME_PFX)) {
        String propCol = StringUtils.substringAfter(key, MetastoreConstants.FACT_COL_END_TIME_PFX);
        if (factCol.equals(propCol)) {
          endTime = sc.getTable().getDateFromProperty(key, false, true);
        }
      }
    }
    return endTime;
  }

  static boolean checkForColumnExistsAndValidForRange(StorageCandidate sc, String column, CubeQueryContext cubeql) {
    return (sc.getColumns().contains(column) && isFactColumnValidForRange(cubeql, sc, column));
  }

}
