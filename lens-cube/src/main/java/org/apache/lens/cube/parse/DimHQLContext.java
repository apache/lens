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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.Dimension;

import static org.apache.lens.cube.parse.StorageUtil.joinWithAnd;

/**
 * Dimension HQLContext.
 *
 * Contains all the dimensions queried and their candidate dim tables Update
 * where string with storage filters added dimensions queried.
 */
abstract class DimHQLContext extends SimpleHQLContext {

  public static Log LOG = LogFactory.getLog(DimHQLContext.class.getName());

  private final Map<Dimension, CandidateDim> dimsToQuery;
  private final Set<Dimension> queriedDims;
  private String where;
  protected final CubeQueryContext query;

  public CubeQueryContext getQuery() {
    return query;
  }

  DimHQLContext(CubeQueryContext query, Map<Dimension, CandidateDim> dimsToQuery, Set<Dimension> queriedDims, String select, String where,
      String groupby, String orderby, String having, Integer limit) throws SemanticException {
    super(select, groupby, orderby, having, limit);
    this.query = query;
    this.dimsToQuery = dimsToQuery;
    this.where = where;
    this.queriedDims = queriedDims;
  }

  protected void setMissingExpressions() throws SemanticException {
    setFrom(getFromString());
    setWhere(joinWithAnd(
      getQuery().getHiveConf().getBoolean
        (CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, CubeQueryConfUtil.DEFAULT_REPLACE_TIMEDIM_WITH_PART_COL)
         ? getPostSelectionWhereClause() : null,
      genWhereClauseWithDimPartitions(where)
    ));
  }

  protected abstract String getPostSelectionWhereClause() throws SemanticException;

  protected String getFromString() throws SemanticException {
    String fromString = getFromTable();
    if (query.isAutoJoinResolved()) {
      fromString =
        query.getAutoJoinCtx().getFromString(fromString, getQueriedFact(), getQueriedDimSet(), getDimsToQuery(), query);
    }
    return fromString;
  }

  protected abstract Set<Dimension> getQueriedDimSet();

  protected abstract CandidateFact getQueriedFact();

  protected abstract String getFromTable() throws SemanticException;

  public Map<Dimension, CandidateDim> getDimsToQuery() {
    return dimsToQuery;
  }

  private String genWhereClauseWithDimPartitions(String originalWhere) {
    StringBuilder whereBuf;
    if (originalWhere != null) {
      whereBuf = new StringBuilder(originalWhere);
    } else {
      whereBuf = new StringBuilder();
    }

    // add where clause for all dimensions
    if (queriedDims != null) {
      boolean added = (originalWhere != null);
      for (Dimension dim : queriedDims) {
        CandidateDim cdim = dimsToQuery.get(dim);
        if (!cdim.isWhereClauseAdded() && !StringUtils.isBlank(cdim.whereClause)) {
          appendWhereClause(whereBuf, StorageUtil.getWhereClause(cdim, query.getAliasForTabName(dim.getName())), added);
          added = true;
        }
      }
    }
    if (whereBuf.length() == 0) {
      return null;
    }
    return whereBuf.toString();
  }

  static void appendWhereClause(StringBuilder filterCondition, String whereClause, boolean hasMore) {
    // Make sure we add AND only when there are already some conditions in where
    // clause
    if (hasMore && !filterCondition.toString().isEmpty() && !StringUtils.isBlank(whereClause)) {
      filterCondition.append(" AND ");
    }

    if (!StringUtils.isBlank(whereClause)) {
      filterCondition.append("(");
      filterCondition.append(whereClause);
      filterCondition.append(")");
    }
  }
}
