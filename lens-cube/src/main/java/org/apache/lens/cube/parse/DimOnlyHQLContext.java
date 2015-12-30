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

/**
 * HQL context class which passes all query strings from {@link CubeQueryContext} and works with all dimensions to be
 * queried.
 * <p/>
 * Updates from string with join clause expanded
 */
class DimOnlyHQLContext extends DimHQLContext {

  DimOnlyHQLContext(Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query, QueryAST ast)
    throws LensException {
    this(dimsToQuery, dimsToQuery.keySet(), query, ast);
  }

  DimOnlyHQLContext(Map<Dimension, CandidateDim> dimsToQuery, Set<Dimension> dimsQueried,
    CubeQueryContext query, QueryAST ast)
    throws LensException {
    super(query, dimsToQuery, dimsQueried, ast);
  }

  public String toHQL() throws LensException {
    return query.getInsertClause() + super.toHQL();
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
  protected Set<Dimension> getQueriedDimSet() {
    return getDimsToQuery().keySet();
  }

  @Override
  protected CandidateFact getQueriedFact() {
    return null;
  }
}
