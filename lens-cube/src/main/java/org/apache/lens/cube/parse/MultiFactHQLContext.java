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

import static org.apache.lens.cube.parse.HQLParser.*;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

/**
 * Writes a join query with all the facts involved, with where, groupby and having expressions pushed down to the fact
 * queries.
 */
@Slf4j
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
    return query.getOrderByString();
  }

  private String getHavingString() {
    return null;
  }

  private String getGroupbyString() {
    return null;
  }

  private String getWhereString() {
    return query.getWhereString();
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
          .append(query.getSelectPhrases().get(i).getSelectAlias()).append(" ");
      } else {
        select.append("COALESCE(");
        String sep = "";
        for (Integer factIndex : selectToFactIndex.get(i)) {
          select.append(sep).append("mq").append(factIndex).append(".").append(
            query.getSelectPhrases().get(i).getSelectAlias());
          sep = ", ";
        }
        select.append(") ");
      }
      select.append(query.getSelectPhrases().get(i).getFinalAlias());
      if (i != query.getSelectAST().getChildCount() - 1) {
        select.append(", ");
      }
    }
    return select.toString();
  }

  private String getMultiFactJoinCondition(int i, String dim) {
    StringBuilder joinCondition = new StringBuilder();
    if (i <= 1) {
      return "".toString();
    } else {
      joinCondition.append("mq").append(i - 2).append(".").append(dim).append(" <=> ").
          append("mq").append(i - 1).append(".").append(dim);
    }
    return joinCondition.toString();
  }

  private String getFromString() throws LensException {
    StringBuilder fromBuilder = new StringBuilder();
    int aliasCount = 1;
    String sep = "";
    for (CandidateFact fact : facts) {
      SimpleHQLContext facthql = factHQLContextMap.get(fact);
      fromBuilder.append(sep).append("(").append(facthql.toHQL()).append(")").append(" mq").append(aliasCount++);
      sep = " full outer join ";
      if (!fact.getDimFieldIndices().isEmpty() && aliasCount > 2) {
        fromBuilder.append(" on ");
        Iterator<Integer> dimIter = fact.getDimFieldIndices().iterator();
        while (dimIter.hasNext()) {
          String dim = query.getSelectPhrases().get(dimIter.next()).getSelectAlias();
          fromBuilder.append(getMultiFactJoinCondition(aliasCount, dim));
          if (dimIter.hasNext()) {
            fromBuilder.append(" AND ");
          }
        }
      }
    }
    return fromBuilder.toString();
  }


  public static ASTNode convertHavingToWhere(ASTNode havingAST, CubeQueryContext context, Set<CandidateFact> cfacts,
    AliasDecider aliasDecider) throws LensException {
    if (havingAST == null) {
      return null;
    }
    if (isAggregateAST(havingAST) || isTableColumnAST(havingAST) || isNonAggregateFunctionAST(havingAST)) {
      // if already present in select, pick alias
      String alias = null;
      for (CandidateFact fact : cfacts) {
        if (fact.isExpressionAnswerable(havingAST, context)) {
          alias = fact.addAndGetAliasFromSelect(havingAST, aliasDecider);
          return new ASTNode(new CommonToken(HiveParser.Identifier, alias));
        }
      }
    }
    if (havingAST.getChildren() != null) {
      for (int i = 0; i < havingAST.getChildCount(); i++) {
        ASTNode replaced = convertHavingToWhere((ASTNode) havingAST.getChild(i), context, cfacts, aliasDecider);
        havingAST.setChild(i, replaced);
      }
    }
    return havingAST;
  }

  public static ASTNode pushDownHaving(ASTNode ast, CubeQueryContext cubeQueryContext, Set<CandidateFact> cfacts)
    throws LensException {
    if (ast == null) {
      return null;
    }
    if (ast.getType() == HiveParser.KW_AND || ast.getType() == HiveParser.TOK_HAVING) {
      List<ASTNode> children = Lists.newArrayList();
      for (Node child : ast.getChildren()) {
        ASTNode newChild = pushDownHaving((ASTNode) child, cubeQueryContext, cfacts);
        if (newChild != null) {
          children.add(newChild);
        }
      }
      if (children.size() == 0) {
        return null;
      } else if (children.size() == 1) {
        return children.get(0);
      } else {
        ASTNode newASTNode = new ASTNode(ast.getToken());
        for (ASTNode child : children) {
          newASTNode.addChild(child);
        }
        return newASTNode;
      }
    }
    if (isPrimitiveBooleanExpression(ast)) {
      CandidateFact fact = pickFactToPushDown(ast, cubeQueryContext, cfacts);
      if (fact == null) {
        return ast;
      }
      fact.addToHaving(ast);
      return null;
    }
    return ast;
  }

  private static CandidateFact pickFactToPushDown(ASTNode ast, CubeQueryContext cubeQueryContext, Set<CandidateFact>
    cfacts) throws LensException {
    for (CandidateFact fact : cfacts) {
      if (fact.isExpressionAnswerable(ast, cubeQueryContext)) {
        return fact;
      }
    }
    return null;
  }

}
