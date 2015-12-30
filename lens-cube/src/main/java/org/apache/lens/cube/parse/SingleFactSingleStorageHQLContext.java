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
 * HQL context class which passes down all query strings to come from DimOnlyHQLContext and works with fact being
 * queried.
 * <p/>
 * Updates from string with join clause expanded
 */
class SingleFactSingleStorageHQLContext extends DimOnlyHQLContext {

  private final CandidateFact fact;
  private final Set<Dimension> queriedDimSet;
  private String storageAlias;

  SingleFactSingleStorageHQLContext(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery,
    CubeQueryContext query, QueryAST ast)
    throws LensException {
    this(fact, dimsToQuery, dimsToQuery.keySet(), query, ast);
  }

  SingleFactSingleStorageHQLContext(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery,
    Set<Dimension> dimsQueried, CubeQueryContext query, QueryAST ast)
    throws LensException {
    super(dimsToQuery, dimsQueried, query, ast);
    this.fact = fact;
    this.queriedDimSet = dimsQueried;
  }

  SingleFactSingleStorageHQLContext(CandidateFact fact, String storageAlias, Map<Dimension, CandidateDim> dimsToQuery,
    CubeQueryContext query, QueryAST ast) throws LensException {
    this(fact, dimsToQuery, query, ast);
    this.storageAlias = storageAlias;
  }

  @Override
  protected String getFromTable() throws LensException {
    if (getQuery().isAutoJoinResolved()) {
      if (storageAlias != null) {
        return storageAlias;
      } else {
        return fact.getStorageString(query.getAliasForTableName(query.getCube().getName()));
      }
    } else {
      if (fact.getStorageTables().size() == 1) {
        return getQuery().getQBFromString(fact, getDimsToQuery());
      } else {
        return storageAlias;
      }
    }
  }

  @Override
  protected CandidateFact getQueriedFact() {
    return fact;
  }

  @Override
  public Set<Dimension> getQueriedDimSet() {
    return queriedDimSet;
  }
}
