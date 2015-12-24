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

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import com.google.common.collect.Lists;

/**
 * Writes a join query with all the facts involved, with where, groupby and having expressions pushed down to the fact
 * queries.
 */
class MultiFactHQLContext extends SimpleHQLContext {

  private Set<CandidateFact> facts;
  private CubeQueryContext query;
  private Map<CandidateFact, SimpleHQLContext> factHQLContextMap = new HashMap<>();

  MultiFactHQLContext(Set<CandidateFact> facts, Map<Dimension, CandidateDim> dimsToQuery,
    Map<CandidateFact, Set<Dimension>> factDimMap, CubeQueryContext query) throws LensException {
    super();
    this.query = query;
    this.facts = facts;
    for (CandidateFact fact : facts) {
      if (fact.getStorageTables().size() > 1) {
        factHQLContextMap.put(fact, new SingleFactMultiStorageHQLContext(fact, dimsToQuery, query, fact));
      } else {
        factHQLContextMap.put(fact,
          new SingleFactSingleStorageHQLContext(fact, dimsToQuery, factDimMap.get(fact), query,
            DefaultQueryAST.fromCandidateFact(fact, fact.getStorageTables().iterator().next(), fact)));
      }
    }
  }

  protected void setMissingExpressions() throws LensException {
    setSelect(getSelectString());
    setFrom(getFromString());
    setWhere(getWhereString());
    setGroupby(getGroupbyString());
    setHaving(getHavingString());
    setOrderby(getOrderbyString());
  }

  private String getOrderbyString() {
    return query.getOrderByTree();
  }

  private String getHavingString() {
    return null;
  }

  private String getGroupbyString() {
    return null;
  }

  private String getWhereString() {
    return null;
  }

  public String toHQL() throws LensException {
    return query.getInsertClause() + super.toHQL();
  }

  private String getSelectString() throws LensException {
    Map<Integer, List<Integer>> selectToFactIndex = new HashMap<>(query.getSelectAST().getChildCount());
    int fi = 1;
    for (CandidateFact fact : facts) {
      for (int ind : fact.getSelectIndices()) {
        if (!selectToFactIndex.containsKey(ind)) {
          selectToFactIndex.put(ind, Lists.<Integer>newArrayList());
        }
        selectToFactIndex.get(ind).add(fi);
      }
      fi++;
    }
    StringBuilder select = new StringBuilder();
    for (int i = 0; i < query.getSelectAST().getChildCount(); i++) {
      if (selectToFactIndex.get(i) == null) {
        throw new LensException(LensCubeErrorCode.EXPRESSION_NOT_IN_ANY_FACT.getLensErrorInfo(),
          HQLParser.getString((ASTNode) query.getSelectAST().getChild(i)));
      }
      if (selectToFactIndex.get(i).size() == 1) {
        select.append("mq").append(selectToFactIndex.get(i).get(0)).append(".")
          .append(query.getSelectAlias(i)).append(" ");
      } else {
        select.append("COALESCE(");
        String sep = "";
        for (Integer factIndex : selectToFactIndex.get(i)) {
          select.append(sep).append("mq").append(factIndex).append(".").append(query.getSelectAlias(i));
          sep = ", ";
        }
        select.append(") ");
      }
      select.append(query.getSelectFinalAlias(i));
      if (i != query.getSelectAST().getChildCount() - 1) {
        select.append(", ");
      }
    }
    return select.toString();
  }

  private String getFromString() throws LensException {
    StringBuilder fromBuilder = new StringBuilder();
    int aliasCount = 1;
    String sep = "";
    for (CandidateFact fact : facts) {
      SimpleHQLContext facthql = factHQLContextMap.get(fact);
      fromBuilder.append(sep).append("(").append(facthql.toHQL()).append(")").append(" mq").append(aliasCount++);
      sep = " full outer join ";
    }
    CandidateFact firstFact = facts.iterator().next();
    if (!firstFact.getDimFieldIndices().isEmpty()) {
      fromBuilder.append(" on ");
    }
    for (int i = 2; i <= facts.size(); i++) {
      Iterator<Integer> dimIter = firstFact.getDimFieldIndices().iterator();
      while (dimIter.hasNext()) {
        String dim = query.getSelectAlias(dimIter.next());
        fromBuilder.append("mq1").append(".").append(dim).append(" <=> ").append("mq").append(i).append(".")
          .append(dim);
        if (dimIter.hasNext()) {
          fromBuilder.append(" AND ");
        }
      }
      if (i != facts.size()) {
        fromBuilder.append(" AND ");
      }
    }
    return fromBuilder.toString();
  }
}
