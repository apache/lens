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

import static org.apache.lens.cube.parse.StorageUtil.joinWithAnd;

import java.util.Map;
import java.util.Set;

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;

import lombok.Getter;

/**
 * Dimension HQLContext.
 * <p></p>
 * Contains all the dimensions queried and their candidate dim tables Update where string with storage filters added
 * dimensions queried.
 */
//@RequiredArgsConstructor
public abstract class DimHQLContext extends SimpleHQLContext implements QueryWriter, QueryWriterContext {
  protected final CubeQueryContext query;
  @Getter
  protected final Map<Dimension, CandidateDim> dimsToQuery;
  @Getter
  protected final Set<Dimension> queriedDims;
  @Getter
  protected final QueryAST queryAst;
  private String where;

  public abstract StorageCandidate getStorageCandidate();

  protected DimHQLContext(CubeQueryContext query, Map<Dimension, CandidateDim> dimsToQuery, QueryAST queryAst) {
    this.query = query;
    this.dimsToQuery = dimsToQuery;
    this.queriedDims = dimsToQuery.keySet();
    this.queryAst = queryAst;
  }

  public CubeQueryContext getQuery() {
    return query;
  }

  public CubeQueryContext getCubeQueryContext() {
    return query;
  }

  protected void setMissingExpressions() throws LensException {
    if (from == null) {
      from = "%s";
    }
    setFrom(String.format(getFrom(), getFromTable()));
    if (where == null) {
      where = queryAst.getWhereString();
    }
    setWhere(joinWithAnd(
      genWhereClauseWithDimPartitions(where), getQuery().getConf().getBoolean(
        CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, CubeQueryConfUtil.DEFAULT_REPLACE_TIMEDIM_WITH_PART_COL)
        ? getPostSelectionWhereClause() : null));
  }

  protected String getPostSelectionWhereClause() throws LensException {
    return null;
  }

  protected abstract String getFromTable() throws LensException;

  protected String genWhereClauseWithDimPartitions(String originalWhere) {
    StringBuilder whereBuf;
    if (originalWhere != null) {
      whereBuf = new StringBuilder(originalWhere);
    } else {
      whereBuf = new StringBuilder();
    }

    // add where clause for all dimensions
    if (getCubeQueryContext() != null) {
      boolean added = (originalWhere != null);
      for (Dimension dim : queriedDims) {
        CandidateDim cdim = dimsToQuery.get(dim);
        String alias = query.getAliasForTableName(dim.getName());
        if (!cdim.isWhereClauseAdded() && !StringUtils.isBlank(cdim.getWhereClause())) {
          appendWhereClause(whereBuf, StorageUtil.getWhereClause(cdim, alias), added);
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

  @Override
  public void addAutoJoinDims() throws LensException {
    if (getCubeQueryContext().isAutoJoinResolved()) {
      dimsToQuery.putAll(getCubeQueryContext().pickCandidateDimsToQuery(getCubeQueryContext().getAutoJoinCtx().pickOptionalTables(this, queriedDims, getCubeQueryContext())));
    }
  }

  @Override
  public void addExpressionDims() throws LensException {
    dimsToQuery.putAll(getCubeQueryContext().pickCandidateDimsToQuery(getCubeQueryContext().getExprCtx().rewriteExprCtx(getCubeQueryContext(), this, getDimsToQuery(), getQueryAst()))); // todo move inside else above. since it'll be empty otherwise
  }

  @Override
  public void addDenormDims() throws LensException {
    dimsToQuery.putAll(getCubeQueryContext().pickCandidateDimsToQuery(getCubeQueryContext().getDeNormCtx().rewriteDenormctx(getCubeQueryContext(), this, getDimsToQuery(), getStorageCandidate() != null)));
  }
}
