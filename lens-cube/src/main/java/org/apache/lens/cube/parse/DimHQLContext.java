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
public abstract class DimHQLContext extends SimpleHQLContext implements QueryWriterContext {
  @Getter
  protected final CubeQueryContext cubeQueryContext;
  @Getter
  protected final Map<Dimension, CandidateDim> dimsToQuery;

  DimHQLContext(CubeQueryContext query, final Map<Dimension, CandidateDim> dimsToQuery, QueryAST queryAST) {
    super(queryAST);
    this.cubeQueryContext = query;
    this.dimsToQuery = dimsToQuery;
  }

  public abstract StorageCandidate getStorageCandidate();

  private Set<Dimension> getQueriedDims() {
    return dimsToQuery.keySet();
  }

  protected abstract String getFromTable() throws LensException;

  String genWhereClauseWithDimPartitions(String originalWhere) {
    StringBuilder whereBuf;
    if (originalWhere != null) {
      whereBuf = new StringBuilder(originalWhere);
    } else {
      whereBuf = new StringBuilder();
    }

    // add where clause for all dimensions
    if (getCubeQueryContext() != null) {
      boolean added = (originalWhere != null);
      for (Map.Entry<Dimension, CandidateDim> dimensionCandidateDimEntry : getDimsToQuery().entrySet()) {
        Dimension dim = dimensionCandidateDimEntry.getKey();
        CandidateDim cdim = dimensionCandidateDimEntry.getValue();
        String alias = getCubeQueryContext().getAliasForTableName(dim.getName());
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
      Set<Dimension> autoJoinDims = getCubeQueryContext().getAutoJoinCtx().pickOptionalTables(this, getQueriedDims(),
        getCubeQueryContext());
      Map<Dimension, CandidateDim> autoJoinDimsToQuery = getCubeQueryContext().pickCandidateDimsToQuery(autoJoinDims);
      dimsToQuery.putAll(autoJoinDimsToQuery);
    }
  }

  @Override
  public void addExpressionDims() throws LensException {
    Set<Dimension> expressionDims = getCubeQueryContext().getExprCtx().rewriteExprCtx(getCubeQueryContext(), this,
      getDimsToQuery());
    Map<Dimension, CandidateDim> expressionDimsToQuery = getCubeQueryContext().pickCandidateDimsToQuery(expressionDims);
    dimsToQuery.putAll(expressionDimsToQuery);
  }

  @Override
  public void addDenormDims() throws LensException {
    Set<Dimension> denormDims = getCubeQueryContext().getDeNormCtx().rewriteDenormctx(getCubeQueryContext(), this,
      getDimsToQuery(), getStorageCandidate() != null);
    Map<Dimension, CandidateDim> denormDimsToQuery = getCubeQueryContext().pickCandidateDimsToQuery(denormDims);
    dimsToQuery.putAll(denormDimsToQuery);
  }

  @Override
  public QueryWriter toQueryWriter() throws LensException {
    return this;
  }
}
