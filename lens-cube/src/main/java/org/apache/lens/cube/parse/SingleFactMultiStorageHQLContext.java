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

import static org.apache.lens.cube.parse.CubeQueryConfUtil.DEFAULT_ENABLE_STORAGES_UNION;
import static org.apache.lens.cube.parse.CubeQueryConfUtil.ENABLE_STORAGES_UNION;
import static org.apache.lens.cube.parse.HQLParser.*;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

public class SingleFactMultiStorageHQLContext extends UnionHQLContext {

  private final QueryAST ast;

  private Map<HashableASTNode, ASTNode> innerToOuterASTs = new HashMap<>();
  private AliasDecider aliasDecider = new DefaultAliasDecider();

  SingleFactMultiStorageHQLContext(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery,
    CubeQueryContext query, QueryAST ast)
    throws LensException {
    super(query, fact);
    if (!query.getConf().getBoolean(ENABLE_STORAGES_UNION, DEFAULT_ENABLE_STORAGES_UNION)) {
      throw new LensException(LensCubeErrorCode.STORAGE_UNION_DISABLED.getLensErrorInfo());
    }
    this.ast = ast;
    processSelectAST();
    processGroupByAST();
    processHavingAST();
    processOrderByAST();
    processLimit();
    setHqlContexts(getUnionContexts(fact, dimsToQuery, query, ast));
  }

  private void processSelectAST() {
    ASTNode originalSelectAST = MetastoreUtil.copyAST(ast.getSelectAST());
    ast.setSelectAST(new ASTNode(originalSelectAST.getToken()));
    ASTNode outerSelectAST = processSelectExpression(originalSelectAST);
    setSelect(getString(outerSelectAST));
  }

  private void processGroupByAST() {
    if (ast.getGroupByAST() != null) {
      setGroupby(getString(processExpression(ast.getGroupByAST())));
    }
  }

  private void processHavingAST() throws LensException {
    if (ast.getHavingAST() != null) {
      setHaving(getString(processExpression(ast.getHavingAST())));
      ast.setHavingAST(null);
    }
  }


  private void processOrderByAST() {
    if (ast.getOrderByAST() != null) {
      setOrderby(getString(processOrderbyExpression(ast.getOrderByAST())));
      ast.setOrderByAST(null);
    }
  }

  private void processLimit() {
    setLimit(ast.getLimitValue());
    ast.setLimitValue(null);
  }

  private ASTNode processExpression(ASTNode astNode) {
    if (astNode == null) {
      return null;
    }
    ASTNode outerExpression = new ASTNode(astNode);
    // iterate over all children of the ast and get outer ast corresponding to it.
    for (Node child : astNode.getChildren()) {
      outerExpression.addChild(getOuterAST((ASTNode)child));
    }
    return outerExpression;
  }

  private ASTNode processSelectExpression(ASTNode astNode) {
    if (astNode == null) {
      return null;
    }
    ASTNode outerExpression = new ASTNode(astNode);
    // iterate over all children of the ast and get outer ast corresponding to it.
    for (Node node : astNode.getChildren()) {
      ASTNode child = (ASTNode)node;
      ASTNode outerSelect = new ASTNode(child);
      ASTNode selectExprAST = (ASTNode)child.getChild(0);
      ASTNode outerAST = getOuterAST(selectExprAST);
      outerSelect.addChild(outerAST);

      // has an alias? add it
      if (child.getChildCount() > 1) {
        outerSelect.addChild(child.getChild(1));
      }
      outerExpression.addChild(outerSelect);
    }
    return outerExpression;
  }

  private ASTNode processOrderbyExpression(ASTNode astNode) {
    if (astNode == null) {
      return null;
    }
    ASTNode outerExpression = new ASTNode(astNode);
    // sample orderby AST looks the following :
    /*
    TOK_ORDERBY
   TOK_TABSORTCOLNAMEDESC
      TOK_NULLS_LAST
         .
            TOK_TABLE_OR_COL
               testcube
            cityid
   TOK_TABSORTCOLNAMEASC
      TOK_NULLS_FIRST
         .
            TOK_TABLE_OR_COL
               testcube
            stateid
   TOK_TABSORTCOLNAMEASC
      TOK_NULLS_FIRST
         .
            TOK_TABLE_OR_COL
               testcube
            zipcode
     */
    for (Node node : astNode.getChildren()) {
      ASTNode child = (ASTNode)node;
      ASTNode outerOrderby = new ASTNode(child);
      ASTNode tokNullsChild = (ASTNode) child.getChild(0);
      ASTNode outerTokNullsChild = new ASTNode(tokNullsChild);
      outerTokNullsChild.addChild(getOuterAST((ASTNode)tokNullsChild.getChild(0)));
      outerOrderby.addChild(outerTokNullsChild);
      outerExpression.addChild(outerOrderby);
    }
    return outerExpression;
  }
  /*

  Perform a DFS on the provided AST, and Create an AST of similar structure with changes specific to the
  inner query - outer query dynamics. The resultant AST is supposed to be used in outer query.

  Base cases:
   1. ast is null => null
   2. ast is aggregate_function(table.column) => add aggregate_function(table.column) to inner select expressions,
            generate alias, return aggregate_function(cube.alias). Memoize the mapping
            aggregate_function(table.column) => aggregate_function(cube.alias)
            Assumption is aggregate_function is transitive i.e. f(a,b,c,d) = f(f(a,b), f(c,d)). SUM, MAX, MIN etc
            are transitive, while AVG, COUNT etc are not. For non-transitive aggregate functions, the re-written
            query will be incorrect.
   3. ast has aggregates - iterate over children and add the non aggregate nodes as is and recursively get outer ast
   for aggregate.
   4. If no aggregates, simply select its alias in outer ast.
   5. If given ast is memorized as mentioned in the above cases, return the mapping.
   */
  private ASTNode getOuterAST(ASTNode astNode) {
    if (astNode == null) {
      return null;
    }
    if (innerToOuterASTs.containsKey(new HashableASTNode(astNode))) {
      return innerToOuterASTs.get(new HashableASTNode(astNode));
    }
    if (isAggregateAST(astNode)) {
      return processAggregate(astNode);
    } else if (hasAggregate(astNode)) {
      ASTNode outerAST = new ASTNode(astNode);
      for (Node child : astNode.getChildren()) {
        ASTNode childAST = (ASTNode) child;
        if (hasAggregate(childAST)) {
          outerAST.addChild(getOuterAST(childAST));
        } else {
          outerAST.addChild(childAST);
        }
      }
      return outerAST;
    } else {
      ASTNode innerSelectASTWithoutAlias = MetastoreUtil.copyAST(astNode);
      ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR));
      innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
      String alias = aliasDecider.decideAlias(astNode);
      ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
      innerSelectExprAST.addChild(aliasNode);
      addToInnerSelectAST(innerSelectExprAST);
      ASTNode outerAST = getDotAST(query.getCube().getName(), alias);
      innerToOuterASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
      return outerAST;
    }
  }

  private ASTNode processAggregate(ASTNode astNode) {
    ASTNode innerSelectASTWithoutAlias = MetastoreUtil.copyAST(astNode);
    ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR));
    innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
    String alias = aliasDecider.decideAlias(astNode);
    ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
    innerSelectExprAST.addChild(aliasNode);
    addToInnerSelectAST(innerSelectExprAST);
    ASTNode dotAST = getDotAST(query.getCube().getName(), alias);
    ASTNode outerAST = new ASTNode(new CommonToken(TOK_FUNCTION));
    //TODO: take care or non-transitive aggregate functions
    outerAST.addChild(new ASTNode(new CommonToken(Identifier, astNode.getChild(0).getText())));
    outerAST.addChild(dotAST);
    innerToOuterASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
    return outerAST;
  }

  private void addToInnerSelectAST(ASTNode selectExprAST) {
    if (ast.getSelectAST() == null) {
      ast.setSelectAST(new ASTNode(new CommonToken(TOK_SELECT)));
    }
    ast.getSelectAST().addChild(selectExprAST);
  }

  private static ArrayList<HQLContextInterface> getUnionContexts(CandidateFact fact, Map<Dimension, CandidateDim>
    dimsToQuery, CubeQueryContext query, QueryAST ast)
    throws LensException {
    ArrayList<HQLContextInterface> contexts = new ArrayList<>();
    String alias = query.getAliasForTableName(query.getCube().getName());
    for (String storageTable : fact.getStorageTables()) {
      SingleFactSingleStorageHQLContext ctx = new SingleFactSingleStorageHQLContext(fact, storageTable + " " + alias,
        dimsToQuery, query, DefaultQueryAST.fromCandidateFact(fact, storageTable, ast));
      contexts.add(ctx);
    }
    return contexts;
  }
}
