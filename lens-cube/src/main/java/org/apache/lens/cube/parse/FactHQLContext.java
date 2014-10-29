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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.Dimension;

/**
 * HQL context class which passes all query strings from the fact and works with
 * required dimensions for the fact.
 * 
 */
public class FactHQLContext extends DimHQLContext {

  public static Log LOG = LogFactory.getLog(FactHQLContext.class.getName());

  private final CandidateFact fact;
  private final CubeQueryContext query;
  private final Set<Dimension> factDims;

  FactHQLContext(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery, Set<Dimension> factDims,
      CubeQueryContext query) throws SemanticException {
    super(dimsToQuery, factDims, fact.getSelectTree(), fact.getWhereTree(), fact.getGroupByTree(), null, fact
        .getHavingTree(), null);
    this.fact = fact;
    this.query = query;
    this.factDims = factDims;
    LOG.info("factDims:" + factDims + " for fact:" + fact);
  }

  protected void setMissingExpressions() throws SemanticException {
    setFrom(getFromString());
    super.setMissingExpressions();
  }

  private String getFromString() throws SemanticException {
    String fromString = null;
    if (query.getAutoJoinCtx() != null && query.getAutoJoinCtx().isJoinsResolved()) {
      String fromTable = getFromTable();
      fromString = query.getAutoJoinCtx().getFromString(fromTable, fact, factDims, getDimsToQuery(), query);
    } else {
      fromString = query.getQBFromString(fact, getDimsToQuery());
    }
    return fromString;
  }

  protected String getFromTable() {
    return fact.getStorageString(query.getAliasForTabName(query.getCube().getName()));
  }

  public CandidateFact getFactToQuery() {
    return fact;
  }

}
