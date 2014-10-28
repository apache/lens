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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.Dimension;

/**
 * HQL context class which passes down all query strings to come from
 * DimOnlyHQLContext and works with fact being queried.
 * 
 * Updates from string with join clause expanded
 * 
 */
class SingleFactHQLContext extends DimOnlyHQLContext {

  public static Log LOG = LogFactory.getLog(SingleFactHQLContext.class.getName());

  private CandidateFact fact;

  SingleFactHQLContext(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query)
      throws SemanticException {
    super(dimsToQuery, query);
    this.fact = fact;
  }

  public CandidateFact getFactToQuery() {
    return fact;
  }

  static void addRangeClauses(CubeQueryContext query, CandidateFact fact) throws SemanticException {
    if (fact != null) {
      // resolve timerange positions and replace it by corresponding where
      // clause
      for (TimeRange range : query.getTimeRanges()) {
        String rangeWhere = fact.rangeToWhereClause.get(range);
        if (!StringUtils.isBlank(rangeWhere)) {
          ASTNode rangeAST;
          try {
            rangeAST = HQLParser.parseExpr(rangeWhere);
          } catch (ParseException e) {
            throw new SemanticException(e);
          }
          rangeAST.setParent(range.getParent());
          range.getParent().setChild(range.getChildIndex(), rangeAST);
        }
      }
    }
  }

  private final String unionQueryFormat = "SELECT * FROM %s";

  String getUnionQueryFormat() {
    StringBuilder queryFormat = new StringBuilder();
    queryFormat.append(unionQueryFormat);
    if (getQuery().getGroupByTree() != null) {
      queryFormat.append(" GROUP BY %s");
    }
    if (getQuery().getHavingTree() != null) {
      queryFormat.append(" HAVING %s");
    }
    if (getQuery().getOrderByTree() != null) {
      queryFormat.append(" ORDER BY %s");
    }
    if (getQuery().getLimitValue() != null) {
      queryFormat.append(" LIMIT %s");
    }
    return queryFormat.toString();
  }

  protected String getFromTable() throws SemanticException {
    if (getQuery().getAutoJoinCtx() != null && getQuery().getAutoJoinCtx().isJoinsResolved()) {
      return fact.getStorageString(getQuery().getAliasForTabName(getQuery().getCube().getName()));
    } else {
      return getQuery().getQBFromString(fact, getDimsToQuery());
    }
  }
}
