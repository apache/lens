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

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

/**
 * HQL context class which passes all query strings from {@link CubeQueryContext} and works with all dimensions to be
 * queried.
 * <p/>
 * Updates from string with join clause expanded
 */
class DimOnlyHQLContext extends DimHQLContext {


  DimOnlyHQLContext(Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query, QueryAST ast)
    throws LensException {
    super(query, dimsToQuery, ast);
  }

  @Override
  public StorageCandidate getStorageCandidate() {
    return null;
  }

  protected String getFromTable() throws LensException {
    if (query.isAutoJoinResolved()) {
      return getDimsToQuery().get(query.getAutoJoinCtx().getAutoJoinTarget()).getStorageString(
        query.getAliasForTableName(query.getAutoJoinCtx().getAutoJoinTarget().getName()));
    } else {
      return query.getQBFromString(null, getDimsToQuery());
    }
  }

  @Override
  public void updateDimFilterWithFactFilter() throws LensException {
    //void
  }

  @Override
  public void updateFromString() throws LensException {
    String fromString = "%s"; // storage string is updated later
    if (getCubeQueryContext().isAutoJoinResolved()) {
      setFrom( //todo check setFrom correct or not
        getCubeQueryContext().getAutoJoinCtx().getFromString(fromString, this, getDimsToQuery(), getCubeQueryContext()));
    }
  }

  @Override
  protected void setMissingExpressions() throws LensException {
    if (getFrom() == null) {
      setFrom("%s");
    }
    setFrom(String.format(getFrom(), getFromTable()));
    if (getWhere() == null) {
      setWhere(queryAst.getWhereString());
    }
    setWhere(joinWithAnd(
      genWhereClauseWithDimPartitions(getWhere()), getQuery().getConf().getBoolean(
        CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, CubeQueryConfUtil.DEFAULT_REPLACE_TIMEDIM_WITH_PART_COL)
        ? getPostSelectionWhereClause() : null));
    setPrefix(query.getInsertClause());
    super.setMissingExpressions();
  }

  @Override
  public QueryWriter toQueryWriter() {
    return this;
  }
}
