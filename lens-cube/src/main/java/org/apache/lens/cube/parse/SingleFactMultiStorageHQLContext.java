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
    ASTNode originalSelectAST = copyAST(ast.getSelectAST());
    ast.setSelectAST(new ASTNode(originalSelectAST.getToken()));
    ASTNode outerSelectAST = processExpression(originalSelectAST);
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
      setOrderby(getString(processExpression(ast.getOrderByAST())));
      ast.setOrderByAST(null);
    }
  }

  private void processLimit() {
    setLimit(ast.getLimitValue());
    ast.setLimitValue(null);
  }

  /*
  Perform a DFS on the provided AST, and Create an AST of similar structure with changes specific to the
  inner query - outer query dynamics. The resultant AST is supposed to be used in outer query.

  Base cases:
   1. ast is null => null
   2. ast is table.column => add this to inner select expressions, generate alias, return cube.alias. Memoize the
            mapping table.column => cube.alias
   3. ast is aggregate_function(table.column) => add aggregate_function(table.column) to inner select expressions,
            generate alias, return aggregate_function(cube.alias). Memoize the mapping
            aggregate_function(table.column) => aggregate_function(cube.alias)
            Assumption is aggregate_function is transitive i.e. f(a,b,c,d) = f(f(a,b), f(c,d)). SUM, MAX, MIN etc
            are transitive, while AVG, COUNT etc are not. For non-transitive aggregate functions, the re-written
            query will be incorrect.
   4. If given ast is memoized as mentioned in the above cases, return the mapping.

   Recursive case:
     Copy the root node, process children recursively and add as children to the copied node. Return the copied node.
   */
  private ASTNode processExpression(ASTNode astNode) {
    if (astNode == null) {
      return null;
    }
    if (isAggregateAST(astNode)) {
      if (innerToOuterASTs.containsKey(new HashableASTNode(astNode))) {
        return innerToOuterASTs.get(new HashableASTNode(astNode));
      }
      ASTNode innerSelectASTWithoutAlias = copyAST(astNode);
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
    } else if (isTableColumnAST(astNode) || isNonAggregateFunctionAST(astNode)) {
      if (innerToOuterASTs.containsKey(new HashableASTNode(astNode))) {
        return innerToOuterASTs.get(new HashableASTNode(astNode));
      }
      ASTNode innerSelectASTWithoutAlias = copyAST(astNode);
      ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR));
      innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
      String alias = aliasDecider.decideAlias(astNode);
      ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
      innerSelectExprAST.addChild(aliasNode);
      addToInnerSelectAST(innerSelectExprAST);
      ASTNode outerAST = getDotAST(query.getCube().getName(), alias);
      innerToOuterASTs.put(new HashableASTNode(innerSelectASTWithoutAlias), outerAST);
      return outerAST;
    } else {
      ASTNode outerHavingExpression = new ASTNode(astNode);
      if (astNode.getChildren() != null) {
        for (Node child : astNode.getChildren()) {
          outerHavingExpression.addChild(processExpression((ASTNode) child));
        }
      }
      return outerHavingExpression;
    }
  }

  /**
   * Transforms the inner query's AST so that aliases are used now instead of column names.
   * Does so in-place, without creating new ASTNode instances.
   * @param astNode inner query's AST Node to transform
   * @return Transformed AST Node.
   */
  private ASTNode replaceAST(ASTNode astNode) {
    if (astNode == null) {
      return null;
    }
    if (isAggregateAST(astNode) || isTableColumnAST(astNode) || isNonAggregateFunctionAST(astNode)) {
      if (innerToOuterASTs.containsKey(new HashableASTNode(astNode))) {
        ASTNode ret = innerToOuterASTs.get(new HashableASTNode(astNode));
        // Set parent null for quicker GC
        astNode.setParent(null);
        return ret;
      }
    }
    for (int i = 0; i < astNode.getChildCount(); i++) {
      astNode.setChild(i, replaceAST((ASTNode) astNode.getChild(i)));
    }
    return astNode;
  }

  private void addToInnerSelectAST(ASTNode selectExprAST) {
    if (ast.getSelectAST() == null) {
      ast.setSelectAST(new ASTNode(new CommonToken(TOK_SELECT)));
    }
    ast.getSelectAST().addChild(selectExprAST);
  }

  private ASTNode getDotAST(String tableAlias, String fieldAlias) {
    ASTNode child = new ASTNode(new CommonToken(DOT, "."));
    child.addChild(new ASTNode(new CommonToken(TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL")));
    child.getChild(0).addChild(new ASTNode(new CommonToken(Identifier, tableAlias)));
    child.addChild(new ASTNode(new CommonToken(Identifier, fieldAlias)));
    return child;
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
