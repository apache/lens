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

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

/**
 * HQL context class which passes all query strings from {@link CubeQueryContext} and works with all dimensions to be
 * queried.
 * <p/>
 * Updates from string with join clause expanded
 */
public class DimOnlyHQLContext extends DimHQLContext {


  DimOnlyHQLContext(Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query) throws LensException {
    this(dimsToQuery, query, query);
  }
  private DimOnlyHQLContext(Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query, QueryAST ast)
    throws LensException {
    super(query, dimsToQuery, ast);
  }

  @Override
  public StorageCandidate getStorageCandidate() {
    return null;
  }

  protected String getFromTable() throws LensException {
    if (getCubeQueryContext().isAutoJoinResolved()) {
      return getDimsToQuery().get(getCubeQueryContext().getAutoJoinCtx().getAutoJoinTarget())
        .getStorageString(getCubeQueryContext().getAliasForTableName(
          getCubeQueryContext().getAutoJoinCtx().getAutoJoinTarget().getName())
        );
    } else {
      return getCubeQueryContext().getQBFromString(null, getDimsToQuery());
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
      setFrom(
        getCubeQueryContext().getAutoJoinCtx().getFromString(
          fromString, this, getDimsToQuery(), getCubeQueryContext()
        )
      );
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
    setWhere(genWhereClauseWithDimPartitions(getWhere()));
    setPrefix(getCubeQueryContext().getInsertClause());
  }
}
