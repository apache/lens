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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;

/**
 * HQL context class which passes down all query strings to come from DimOnlyHQLContext and works with fact being
 * queried.
 * <p/>
 * Updates from string with join clause expanded
 */
class SingleFactHQLContext extends DimOnlyHQLContext {

  private final CandidateFact fact;
  private String storageAlias;

  SingleFactHQLContext(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query)
    throws LensException {
    super(dimsToQuery, query);
    this.fact = fact;
  }

  SingleFactHQLContext(CandidateFact fact, String storageAlias, Map<Dimension, CandidateDim> dimsToQuery,
      CubeQueryContext query, String whereClause) throws LensException {
    super(dimsToQuery, query, whereClause);
    this.fact = fact;
    this.storageAlias = storageAlias;
  }


  public CandidateFact getFactToQuery() {
    return fact;
  }

  static void addRangeClauses(CubeQueryContext query, CandidateFact fact) throws LensException {
    if (fact != null) {
      // resolve timerange positions and replace it by corresponding where
      // clause
      for (TimeRange range : query.getTimeRanges()) {
        for (Map.Entry<String, String> entry : fact.getRangeToStorageWhereMap().get(range).entrySet()) {
          String table = entry.getValue();
          String rangeWhere = entry.getKey();

          if (!StringUtils.isBlank(rangeWhere)) {
            ASTNode rangeAST;
            try {
              rangeAST = HQLParser.parseExpr(rangeWhere);
            } catch (ParseException e) {
              throw new LensException(e);
            }
            rangeAST.setParent(range.getParent());
            range.getParent().setChild(range.getChildIndex(), rangeAST);
          }
          fact.getStorgeWhereClauseMap().put(table, query.getWhereTree());
        }
      }
    }
  }


  @Override
  protected String getFromTable() throws LensException {
    if (getQuery().getAutoJoinCtx() != null && getQuery().getAutoJoinCtx().isJoinsResolved()) {
      if (storageAlias != null) {
        return storageAlias;
      } else {
        return fact.getStorageString(getQuery().getAliasForTableName(getQuery().getCube().getName()));
      }
    } else {
      if (fact.getStorageTables().size() == 1) {
        return getQuery().getQBFromString(fact, getDimsToQuery());
      } else {
        return storageAlias;
      }
    }
  }
}
