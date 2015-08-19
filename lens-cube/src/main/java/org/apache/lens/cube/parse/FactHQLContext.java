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

import lombok.extern.slf4j.Slf4j;

/**
 * HQL context class which passes all query strings from the fact and works with required dimensions for the fact.
 */
@Slf4j
public class FactHQLContext extends DimHQLContext {

  private final CandidateFact fact;
  private final Set<Dimension> factDims;

  FactHQLContext(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery, Set<Dimension> factDims,
    CubeQueryContext query) throws LensException {
    super(query, dimsToQuery, factDims, fact.getSelectTree(), fact.getWhereTree(), fact.getGroupByTree(), null, fact
      .getHavingTree(), null);
    this.fact = fact;
    this.factDims = factDims;
    log.info("factDims:{} for fact:{}", factDims, fact);
  }

  @Override
  protected Set<Dimension> getQueriedDimSet() {
    return factDims;
  }

  @Override
  protected CandidateFact getQueriedFact() {
    return fact;
  }

  protected String getFromTable() throws LensException {
    return query.getQBFromString(fact, getDimsToQuery());
  }

  public CandidateFact getFactToQuery() {
    return fact;
  }

}
