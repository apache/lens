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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.Dimension;

/**
 * HQL context class which passes all query strings from
 * {@link CubeQueryContext} and works with all dimensions to be queried.
 * 
 * Updates from string with join clause expanded
 * 
 */
class DimOnlyHQLContext extends DimHQLContext {

  public static Log LOG = LogFactory.getLog(DimOnlyHQLContext.class.getName());

  private final CubeQueryContext query;

  public CubeQueryContext getQuery() {
    return query;
  }

  DimOnlyHQLContext(Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query) throws SemanticException {
    super(dimsToQuery, dimsToQuery.keySet(), query.getSelectTree(), query.getWhereTree(), query.getGroupByTree(), query
        .getOrderByTree(), query.getHavingTree(), query.getLimitValue());
    this.query = query;
  }

  protected void setMissingExpressions() throws SemanticException {
    setFrom(getFromString());
    super.setMissingExpressions();
  }

  public String toHQL() throws SemanticException {
    return query.getInsertClause() + super.toHQL();
  }

  protected String getFromTable() throws SemanticException {
    if (query.getAutoJoinCtx() != null && query.getAutoJoinCtx().isJoinsResolved()) {
      return getDimsToQuery().get(query.getAutoJoinCtx().getAutoJoinTarget()).getStorageString(
          query.getAliasForTabName(query.getAutoJoinCtx().getAutoJoinTarget().getName()));
    } else {
      return query.getQBFromString(null, getDimsToQuery());
    }
  }

  private String getFromString() throws SemanticException {
    String fromString = null;
    String fromTable = getFromTable();
    if (query.getAutoJoinCtx() != null && query.getAutoJoinCtx().isJoinsResolved()) {
      fromString =
          query.getAutoJoinCtx().getFromString(fromTable, null, getDimsToQuery().keySet(), getDimsToQuery(), query);
    } else {
      fromString = fromTable;
    }
    return fromString;
  }
}
