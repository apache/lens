/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.cube.parse;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import static org.apache.lens.cube.parse.HQLParser.*;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.*;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.join.JoinPath;
import org.apache.lens.cube.parse.ExpressionResolver.PickedExpression;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class to write union query. Given any complex Join or Union Candidate,
 * this class rewrites union query for all the participating StorageCandidates.
 */
@Slf4j
public class UnionQueryWriter extends SimpleHQLContext {

  private Map<HQLParser.HashableASTNode, ASTNode> innerToOuterSelectASTs = new HashMap<>();
  private Map<HQLParser.HashableASTNode, ASTNode> innerToOuterHavingASTs = new HashMap<>();
  private Map<StorageCandidateHQLContext, ASTNode> storageCandidateToSelectAstMap = new HashMap<>();
  private CubeQueryContext cubeql;
  static final ASTNode DEFAULT_MEASURE_AST;
  private static final String DEFAULT_MEASURE = "0";
  static {
    try {
      DEFAULT_MEASURE_AST = HQLParser.parseExpr(DEFAULT_MEASURE);
    } catch (LensException e) {
      throw new RuntimeException("default measure not parsable");
    }
  }
  Collection<StorageCandidateHQLContext> storageCandidates;
  public static final String DUPLICATE_EXPRESSION_PREFIX = "D";

  UnionQueryWriter(List<StorageCandidateHQLContext> storageCandidates, CubeQueryContext cubeql) throws LensException {
    super(DefaultQueryAST.fromStorageCandidate(storageCandidates.iterator().next()));
    this.storageCandidates = storageCandidates;
    if (storageCandidates.size() <= 1) {
      throw new IllegalArgumentException("There should be atleast two storage candidates to write a union query");
    }
    this.cubeql = cubeql;
  }

  @Override
  protected void setMissingExpressions() throws LensException {
    // Set the default queryAST for the outer query
    updateAsts();
    updateInnterSelectASTWithDefault();
    processSelectAndHavingAST();
    processGroupByAST();
    processOrderByAST();
    CandidateUtil.updateFinalAlias(queryAst.getSelectAST(), cubeql);
    setPrefix(cubeql.getInsertClause());
    setFrom(getFromString());
    CandidateUtil.updateOrderByWithFinalAlias(queryAst.getOrderByAST(), queryAst.getSelectAST());
  }

  /**
   * Set having, order by and limit clauses to null for inner queries
   * being constructed from StorageCandidate.
   */
  private void updateAsts() {
    for (StorageCandidateHQLContext sc : storageCandidates) {
      storageCandidateToSelectAstMap.put(sc, new ASTNode(new CommonToken(TOK_SELECT, "TOK_SELECT")));
      if (sc.getQueryAst().getHavingAST() != null) {
        cubeql.setHavingAST(sc.getQueryAst().getHavingAST());
      }
      sc.getQueryAst().setOrderByAST(null);
      sc.getQueryAst().setLimitValue(null);
    }
  }

  private void processGroupByAST() throws LensException {
    if (queryAst.getGroupByAST() != null) {
      queryAst.setGroupByAST(processGroupByExpression(queryAst.getGroupByAST()));
    }
  }

  /**
   * Process havingAST for a StorageCandidate. Any column not projected and part of having clause
   * project it in inner select
   *
   * @param innerAst
   * @param aliasDecider
   * @param sc
   * @return ASTNode
   * @throws LensException
   */
  private ASTNode processHavingAST(ASTNode innerAst, AliasDecider aliasDecider, StorageCandidateHQLContext sc)
    throws LensException {
    if (sc.getQueryAst().getHavingAST() == null
        && cubeql.getHavingAST() != null) {
      sc.getQueryAst().setHavingAST(cubeql.getHavingAST());
    }
    if (sc.getQueryAst().getHavingAST() != null) {
      ASTNode havingCopy = MetastoreUtil.copyAST(sc.getQueryAst().getHavingAST());
      Set<ASTNode> havingAggChildrenASTs = new LinkedHashSet<>();
      getAggregateChildrenInNode(havingCopy, havingAggChildrenASTs);
      processHavingExpression(innerAst, havingAggChildrenASTs, aliasDecider, sc);
      updateOuterHavingAST(havingCopy);
      queryAst.setHavingAST(havingCopy);
      HQLParser.getString(havingCopy);
    }
    return null;
  }

  /**
   * Update outer havingAST with proper alias name projected.
   *
   * @param node
   * @return
   */
  private ASTNode updateOuterHavingAST(ASTNode node) {
    if (node.getToken().getType() == HiveParser.TOK_FUNCTION
        && (HQLParser.isAggregateAST(node))) {
      if (innerToOuterSelectASTs.containsKey(new HQLParser.HashableASTNode(node))
          || innerToOuterHavingASTs.containsKey(new HQLParser.HashableASTNode(node))) {
        ASTNode expr = innerToOuterSelectASTs.containsKey(new HQLParser.HashableASTNode(node))
            ? innerToOuterSelectASTs.get(new HQLParser.HashableASTNode(node))
            : innerToOuterHavingASTs.get(new HQLParser.HashableASTNode(node));
        if (node.getChildCount() > 1) {
          node.replaceChildren(1, 1, expr.getChild(1));
        } else {
          node.replaceChildren(0, 0, expr);
        }
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      updateOuterHavingAST(child);
    }
    return node;
  }


  private void processOrderByAST() throws LensException {
    if (queryAst.getOrderByAST() != null) {
      queryAst.setOrderByAST(processOrderbyExpression(queryAst.getOrderByAST()));
    }
  }

  private ASTNode processOrderbyExpression(ASTNode astNode) throws LensException {
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
      ASTNode child = (ASTNode) node;
      ASTNode outerOrderby = new ASTNode(child);
      ASTNode tokNullsChild = (ASTNode) child.getChild(0);
      ASTNode outerTokNullsChild = new ASTNode(tokNullsChild);
      if (((ASTNode) tokNullsChild.getChild(0)).getToken().getType() == HiveParser.DOT
          || ((ASTNode) tokNullsChild.getChild(0)).getToken().getType() == HiveParser.TOK_FUNCTION) {
        outerTokNullsChild.addChild(innerToOuterSelectASTs.get(new HQLParser.HashableASTNode((ASTNode) tokNullsChild)));
      } else {
        outerTokNullsChild.addChild(tokNullsChild);
      }
      outerOrderby.addChild(outerTokNullsChild);
      outerExpression.addChild(outerOrderby);
    }
    return outerExpression;
  }

  /**
   * Get the select expression. In case of node is default retunrs "0" with alias
   * otherwise the select phrase with alias.
   *
   * @param nodeWithoutAlias
   * @param aliasNode
   * @param isDefault
   * @return
   * @throws LensException
   */
  private ASTNode getSelectExpr(ASTNode nodeWithoutAlias, ASTNode aliasNode, boolean isDefault)
    throws LensException {
    ASTNode node = getSelectExprAST();
    if (nodeWithoutAlias == null && isDefault) {
      node.addChild(HQLParser.parseExpr(DEFAULT_MEASURE));
      node.addChild(aliasNode);
    } else {
      node.addChild(nodeWithoutAlias);
      node.addChild(aliasNode);
    }
    return node;
  }


  private ASTNode getSelectExprAST() {
    return new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
  }


  /**
   * Get the aggregate node for the SelectPhrase index. A given measure might not be answerable
   * for a StorageCanddate. In that case get the non default aggregate node wcich ideally not "0",
   * from otherStorage candidate.
   *
   * @param position
   * @return
   */
  private ASTNode getAggregateNodesExpression(int position) {
    ASTNode node = null;
    for (StorageCandidateHQLContext sc : storageCandidates) {
      node = (ASTNode) sc.getQueryAst().getSelectAST().getChild(position).getChild(0);
      if (HQLParser.isAggregateAST(node) || HQLParser.hasAggregate(node)) {
        if (!node.getChild(1).toString().equals(DEFAULT_MEASURE))  {
          return MetastoreUtil.copyAST(node);
        }
      }
    }
    return MetastoreUtil.copyAST(node);
  }

  /**
   * Set the default value "0" in the non answerable aggreagte expressions.
   * @param node
   * @param sc
   * @return
   * @throws LensException
   */
  private ASTNode setDefaultValueInExprForAggregateNodes(ASTNode node, StorageCandidate sc) throws LensException {
    if (HQLParser.isAggregateAST(node)) {
      node.setChild(1, getSelectExpr(null, null, true));
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      setDefaultValueInExprForAggregateNodes(child, sc);
    }
    return node;
  }

  private boolean isAggregateFunctionUsedInAST(ASTNode node) {
    return HQLParser.isAggregateAST(node) || HQLParser.hasAggregate(node);
  }

  private boolean isNodeDefault(ASTNode node) {
    if (HQLParser.isAggregateAST((ASTNode) node.getChild(0)) || HQLParser.isAggregateAST(node)) {
      if (HQLParser.getString((ASTNode) node.getChild(0).getChild(1)).equals(DEFAULT_MEASURE)
          || HQLParser.getString((ASTNode) node.getChild(1)).equals(DEFAULT_MEASURE)) {
        return true;
      }
    }
    return false;
  }

  private List<ASTNode> getProjectedNonDefaultPhrases() {
    List<ASTNode> phrases = new ArrayList<>();
    for (int i = 0; i < storageCandidates.iterator().next().getQueryAst().getSelectAST().getChildCount(); i++) {
      for (StorageCandidateHQLContext sc : storageCandidates) {
        ASTNode selectAST = sc.getQueryAst().getSelectAST();
        if (!isNodeDefault((ASTNode) selectAST.getChild(i))) {
          phrases.add((ASTNode) selectAST.getChild(i));
          break;
        }
      }
    }
    return phrases;
  }

  private void removeRedundantProjectedPhrases() {
    List<ASTNode> phrases = getProjectedNonDefaultPhrases();
    List<String> phrasesWithoutAlias = phrases.stream().map(node -> node.getChild(0))
      .map(ASTNode.class::cast).map(HQLParser::getString).collect(toList());
    Map<String, List<Integer>> phraseCountMap = new HashMap<>();
    Map<String, List<String>> aliasMap = new HashMap<>();
    for (int i = 0; i < phrasesWithoutAlias.size(); i++) {
      String phrase = phrasesWithoutAlias.get(i);
      phraseCountMap.computeIfAbsent(phrase, x->new ArrayList()).add(i);
    }
    for (List<Integer> values : phraseCountMap.values()) {
      if (values.size() > 1) {
        String aliasToKeep = HQLParser.findNodeByPath((ASTNode)
            phrases.get(values.get(0)), Identifier).toString();
        ArrayList<String> dupAliases = new ArrayList<>();
        for (int i : values.subList(1, values.size())) {
          dupAliases.add(HQLParser.findNodeByPath((ASTNode)
              phrases.get(i), Identifier).toString());
        }
        aliasMap.put(aliasToKeep, dupAliases);
      }
    }

    for (String col : phraseCountMap.keySet()) {
      if (phraseCountMap.get(col).size() > 1) {
        List<Integer> childenToDelete = phraseCountMap.get(col).
            subList(1, phraseCountMap.get(col).size());
        for (int i : childenToDelete) {
          for (StorageCandidateHQLContext sc : storageCandidates) {
            sc.getQueryAst().getSelectAST().setChild(i,
                new ASTNode(new CommonToken(HiveParser.Identifier, DUPLICATE_EXPRESSION_PREFIX)));
          }
        }
      }
    }

    for (StorageCandidateHQLContext sc : storageCandidates) {
      for (Node node : sc.getQueryAst().getSelectAST().getChildren()) {
        ASTNode selectNode = (ASTNode) node;
        if (selectNode.getToken().getType() == HiveParser.Identifier
            && selectNode.getText().equals(DUPLICATE_EXPRESSION_PREFIX)) {
          sc.getQueryAst().getSelectAST().deleteChild(selectNode.getChildIndex());
        }
      }
    }

    updateOuterASTDuplicateAliases(queryAst.getSelectAST(), aliasMap);
    if (queryAst.getHavingAST() != null) {
      updateOuterASTDuplicateAliases(queryAst.getHavingAST(), aliasMap);
    }
  }

  private void updateOuterASTDuplicateAliases(ASTNode node, Map<String, List<String>> aliasMap) {
    if (node.getToken().getType() == HiveParser.DOT) {
      String col = node.getChild(1).toString();
      for (Map.Entry<String, List<String>> entry : aliasMap.entrySet()) {
        if (entry.getValue().contains(col)) {
          try {
            node.setChild(1, HQLParser.parseExpr(entry.getKey()));
          } catch (LensException e) {
            log.error("Unable to parse select expression: {}.", entry.getKey());
          }
        }

      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      updateOuterASTDuplicateAliases(child, aliasMap);
    }
  }


  /**
   * Set the default value for the non queriable measures. If a measure is not
   * answerable from a StorageCandidate set it as "0"
   *
   * @throws LensException
   */
  private void updateInnterSelectASTWithDefault() throws LensException {
    for (int i = 0; i < cubeql.getSelectPhrases().size(); i++) {
      SelectPhraseContext phrase = cubeql.getSelectPhrases().get(i);
      ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, phrase.getSelectAlias()));

      // Select phrase is dimension
      if (!phrase.hasMeasures(cubeql)) {
        for (StorageCandidateHQLContext sc : storageCandidates) {
          ASTNode exprWithOutAlias = (ASTNode) sc.getQueryAst().getSelectAST().getChild(i).getChild(0);
          storageCandidateToSelectAstMap.get(sc).
              addChild(getSelectExpr(exprWithOutAlias, aliasNode, false));
        }

        // Select phrase is measure
      } else if (!phrase.getQueriedMsrs().isEmpty()) {
        for (StorageCandidateHQLContext sc : storageCandidates) {
          if (sc.getStorageCandidate().getAnswerableMeasurePhraseIndices().contains(phrase.getPosition())) {
            ASTNode exprWithOutAlias = (ASTNode) sc.getQueryAst().getSelectAST().getChild(i).getChild(0);
            storageCandidateToSelectAstMap.get(sc).
                addChild(getSelectExpr(exprWithOutAlias, aliasNode, false));
          } else {
            ASTNode resolvedExprNode = getAggregateNodesExpression(i);
            if (isAggregateFunctionUsedInAST(resolvedExprNode)) {
              setDefaultValueInExprForAggregateNodes(resolvedExprNode, sc.getStorageCandidate());
            } else {
              resolvedExprNode = getSelectExpr(null, null, true);
            }
            storageCandidateToSelectAstMap.get(sc).
                addChild(getSelectExpr(resolvedExprNode, aliasNode, false));
          }
        }

        // Select phrase is expression
      } else {
        for (StorageCandidateHQLContext sc : storageCandidates) {
          if (sc.getStorageCandidate().getAnswerableMeasurePhraseIndices().contains(phrase.getPosition())) {
            ASTNode exprWithOutAlias = (ASTNode) sc.getQueryAst().getSelectAST().getChild(i).getChild(0);
            storageCandidateToSelectAstMap.get(sc).
                addChild(getSelectExpr(exprWithOutAlias, aliasNode, false));
          } else {
            ASTNode resolvedExprNode = getAggregateNodesExpression(i);
            if (isAggregateFunctionUsedInAST(resolvedExprNode)) {
              setDefaultValueInExprForAggregateNodes(resolvedExprNode, sc.getStorageCandidate());
            } else {
              resolvedExprNode = getSelectExpr(null, null, true);
            }
            storageCandidateToSelectAstMap.get(sc).
                addChild(getSelectExpr(resolvedExprNode, aliasNode, false));
          }
        }
      }
    }
  }

  /**
   * Update Select and Having clause of outer query.
   *
   * @throws LensException
   */
  private void processSelectAndHavingAST() throws LensException {
    ASTNode outerSelectAst = new ASTNode(queryAst.getSelectAST());
    DefaultAliasDecider aliasDecider = new DefaultAliasDecider();
    int selectAliasCounter = 0;
    for (StorageCandidateHQLContext sc : storageCandidates) {
      aliasDecider.setCounter(0);
      ASTNode innerSelectAST = new ASTNode(new CommonToken(TOK_SELECT, "TOK_SELECT"));
      processSelectExpression(sc, outerSelectAst, innerSelectAST, aliasDecider);
      selectAliasCounter = aliasDecider.getCounter();
    }
    queryAst.setSelectAST(outerSelectAst);

    // Iterate over the StorageCandidates and add non projected having columns in inner select ASTs
    Map<DimHQLContext, Map<String, Set<PickedExpression>>> pickedExpressionsPerCandidate = new HashMap<>();
    for (CubeQueryContext cubeQueryContext : storageCandidates.stream()
      .map(StorageCandidateHQLContext::getCubeQueryContext).collect(toSet())) {
      pickedExpressionsPerCandidate.putAll(cubeQueryContext.getExprCtx().getPickedExpressionsPerCandidate());
    }
    cubeql.getExprCtx().replaceHavingExpressions(pickedExpressionsPerCandidate);
    for (StorageCandidateHQLContext sc : storageCandidates) {
      aliasDecider.setCounter(selectAliasCounter);
      processHavingAST(sc.getQueryAst().getSelectAST(), aliasDecider, sc);
    }
    removeRedundantProjectedPhrases();
  }

  /**
   * Get the inner and outer AST with alias for each child of StorageCandidate
   *
   * @param sc
   * @param outerSelectAst
   * @param innerSelectAST
   * @param aliasDecider
   * @throws LensException
   */
  private void processSelectExpression(StorageCandidateHQLContext sc, ASTNode outerSelectAst, ASTNode innerSelectAST,
      AliasDecider aliasDecider) throws LensException {
    //ASTNode selectAST = sc.getQueryAst().getSelectAST();
    ASTNode selectAST = storageCandidateToSelectAstMap.get(sc);
    if (selectAST == null) {
      return;
    }
    // iterate over all children of the ast and get outer ast corresponding to it.
    for (int i = 0; i < selectAST.getChildCount(); i++) {
      ASTNode child = (ASTNode) selectAST.getChild(i);
      ASTNode outerSelect = new ASTNode(child);
      ASTNode selectExprAST = (ASTNode) child.getChild(0);
      ASTNode outerAST = getOuterAST(selectExprAST, innerSelectAST, aliasDecider, sc, true,
          cubeql.getBaseCube().getDimAttributeNames());
      outerSelect.addChild(outerAST);
      // has an alias? add it
      if (child.getChildCount() > 1) {
        outerSelect.addChild(child.getChild(1));
      }
      if (outerSelectAst.getChildCount() <= selectAST.getChildCount()) {
        if (outerSelectAst.getChild(i) == null) {
          outerSelectAst.addChild(outerSelect);
        } else if (HQLParser.getString((ASTNode) outerSelectAst.getChild(i).getChild(0)).equals(DEFAULT_MEASURE)) {
          outerSelectAst.replaceChildren(i, i, outerSelect);
        }
      }
    }
    sc.getQueryAst().setSelectAST(innerSelectAST);
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
  private ASTNode getOuterAST(ASTNode astNode, ASTNode innerSelectAST,
      AliasDecider aliasDecider, StorageCandidateHQLContext scContext, boolean isSelectAst, Set<String> dimensionSet)
      throws LensException {
    StorageCandidate sc = scContext.getStorageCandidate();
    if (astNode == null) {
      return null;
    }
    Set<String> msrCols = new HashSet<>();
    getAllColumnsOfNode(astNode, msrCols, scContext);
    msrCols.removeAll(dimensionSet);
    if (isAggregateAST(astNode) && sc.getColumns().containsAll(msrCols)) {
      return processAggregate(astNode, innerSelectAST, aliasDecider, isSelectAst);
    } else if (isAggregateAST(astNode) && !sc.getColumns().containsAll(msrCols)) {
      ASTNode outerAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
      ASTNode exprCopy = MetastoreUtil.copyAST(astNode);
      setDefaultValueInExprForAggregateNodes(exprCopy, sc);
      outerAST.addChild(getOuterAST(getSelectExpr(exprCopy, null, true),
          innerSelectAST, aliasDecider, scContext, isSelectAst, dimensionSet));
      return outerAST;
    } else {
      if (hasAggregate(astNode)) {
        ASTNode outerAST = new ASTNode(astNode);
        for (Node child : astNode.getChildren()) {
          ASTNode childAST = (ASTNode) child;
          if (hasAggregate(childAST) && sc.getColumns().containsAll(msrCols)) {
            outerAST.addChild(getOuterAST(childAST, innerSelectAST, aliasDecider,
                scContext, isSelectAst, dimensionSet));
          } else if (hasAggregate(childAST) && !sc.getColumns().containsAll(msrCols)) {
            childAST.replaceChildren(1, 1,  getSelectExpr(null, null, true));
            outerAST.addChild(getOuterAST(childAST, innerSelectAST, aliasDecider,
                scContext, isSelectAst, dimensionSet));
          } else {
            outerAST.addChild(childAST);
          }
        }
        return outerAST;
      } else {
        ASTNode innerSelectASTWithoutAlias = MetastoreUtil.copyAST(astNode);
        ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
        innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
        String alias = aliasDecider.decideAlias(astNode);
        ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
        innerSelectExprAST.addChild(aliasNode);
        innerSelectAST.addChild(innerSelectExprAST);
        if (astNode.getText().equals(DEFAULT_MEASURE)) {
          ASTNode outerAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
          outerAST.addChild(astNode);
          return outerAST;
        } else {
          ASTNode outerAST = getDotAST(cubeql.getCube().getName(), alias);
          HashableASTNode innerAST = new HashableASTNode(innerSelectASTWithoutAlias);
          if (isSelectAst && !innerToOuterSelectASTs.containsKey(innerAST)) {
            innerToOuterSelectASTs.put(innerAST, outerAST);
          } else if (!isSelectAst && !innerToOuterHavingASTs.containsKey(innerAST)) {
            innerToOuterHavingASTs.put(innerAST, outerAST);
          }
          return outerAST;
        }
      }
    }
  }

  private ASTNode processAggregate(ASTNode astNode, ASTNode innerSelectAST,
      AliasDecider aliasDecider, boolean isSelectAst) {
    ASTNode innerSelectASTWithoutAlias = MetastoreUtil.copyAST(astNode);
    ASTNode innerSelectExprAST = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
    innerSelectExprAST.addChild(innerSelectASTWithoutAlias);
    String alias = aliasDecider.decideAlias(astNode);
    ASTNode aliasNode = new ASTNode(new CommonToken(Identifier, alias));
    innerSelectExprAST.addChild(aliasNode);
    innerSelectAST.addChild(innerSelectExprAST);
    ASTNode dotAST = getDotAST(cubeql.getCube().getName(), alias);
    ASTNode outerAST = new ASTNode(new CommonToken(TOK_FUNCTION, "TOK_FUNCTION"));
    //TODO: take care or non-transitive aggregate functions
    outerAST.addChild(new ASTNode(new CommonToken(Identifier, astNode.getChild(0).getText())));
    outerAST.addChild(dotAST);
    HashableASTNode innerAST = new HashableASTNode(innerSelectASTWithoutAlias);
    if (isSelectAst && !innerToOuterSelectASTs.containsKey(innerAST)) {
      innerToOuterSelectASTs.put(innerAST, outerAST);
    } else if (!isSelectAst && !innerToOuterHavingASTs.containsKey(innerAST)) {
      innerToOuterHavingASTs.put(innerAST, outerAST);
    }
    return outerAST;
  }

  /**
   * GroupbyAST is having dim only columns all the columns should have been
   * projected. Get the alias for the projected columns and add to group by clause.
   *
   * @param astNode
   * @return
   * @throws LensException
   */

  private ASTNode processGroupByExpression(ASTNode astNode) throws LensException {
    ASTNode outerExpression = new ASTNode(astNode);
    // iterate over all children of the ast and get outer ast corresponding to it.
    for (Node child : astNode.getChildren()) {
      // Columns in group by should have been projected as they are dimension columns
      if (innerToOuterSelectASTs.containsKey(new HQLParser.HashableASTNode((ASTNode) child))) {
        outerExpression.addChild(innerToOuterSelectASTs.get(new HQLParser.HashableASTNode((ASTNode) child)));
      }
    }
    return outerExpression;
  }

  /**
   * Process having clause, if a columns is not projected add it
   * to the projected columns of inner selectAST.
   *
   * @param innerSelectAst
   * @param havingAggASTs
   * @param aliasDecider
   * @param sc
   * @throws LensException
   */

  private void processHavingExpression(ASTNode innerSelectAst, Set<ASTNode> havingAggASTs,
      AliasDecider aliasDecider, StorageCandidateHQLContext sc) throws LensException {
    // iterate over all children of the ast and get outer ast corresponding to it.
    for (ASTNode child : havingAggASTs) {
      getOuterAST(child, innerSelectAst, aliasDecider, sc, false, cubeql.getBaseCube().getDimAttributeNames());
    }
  }

  /**
   * Gets all aggreage nodes used in having
   *
   * @param node
   * @param havingClauses
   * @return
   */
  private Set<ASTNode> getAggregateChildrenInNode(ASTNode node, Set<ASTNode> havingClauses) {
    if (node.getToken().getType() == HiveParser.TOK_FUNCTION && (HQLParser.isAggregateAST(node))) {
      havingClauses.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getAggregateChildrenInNode(child, havingClauses);
    }
    return havingClauses;
  }

  /**
   * Get columns used in ASTNode
   *
   * @param node
   * @param msrs
   * @return
   */
  private Set<String> getAllColumnsOfNode(ASTNode node, Set<String> msrs, StorageCandidateHQLContext sc) {
    if (node.getToken().getType() == HiveParser.DOT) {
      String col = node.getChild(1).toString();
      msrs.addAll(getSourceColumnOfRefColumn(col, sc));
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getAllColumnsOfNode(child, msrs, sc);
    }
    return msrs;
  }

  /**
   * Returns the source column of the ref column
   *
   * @param refCol
   * @return
   */
  private Set<String> getSourceColumnOfRefColumn(String refCol, StorageCandidateHQLContext sc) {
    Set<String> sourceColumns = new HashSet<String>();
    for (Map.Entry<String, Set<String>> entry : sc.getCubeQueryContext().getTblAliasToColumns().entrySet()) {
      if (entry.getValue().contains(refCol)) {
        String table = entry.getKey();

        if (sc.getCubeQueryContext().getAutoJoinCtx() != null) {
          for (Map.Entry<Aliased<Dimension>, List<JoinPath>> dimPaths
              : sc.getCubeQueryContext().getAutoJoinCtx().getAllPaths().entrySet()) {

            if (dimPaths.getKey().alias.equals(table)) {
              List<JoinPath> joinPaths = dimPaths.getValue();
              for (JoinPath path : joinPaths) {
                sourceColumns.addAll(path.getColumnsForTable(sc.getCubeQueryContext().getBaseCube()));
              }
            }
          }
        }
      }
    }
    if (sourceColumns.isEmpty()) {
      sourceColumns.add(refCol);
    }
    return sourceColumns;
  }

  /**
   * Gets from string of the ouer query, this is a union query of all
   * StorageCandidates participated.
   *
   * @return
   * @throws LensException
   */
  private String getFromString() throws LensException {
    List<String> hqlQueries = new ArrayList<>();
    for (StorageCandidateHQLContext sc : storageCandidates) {
      removeAggreagateFromDefaultColumns(sc.getQueryAst().getSelectAST());
      sc.getQueryAst().setHavingAST(null);
      hqlQueries.add(sc.toHQL());
    }
    return hqlQueries.stream().collect(joining(" UNION ALL ", "(", ") as " + cubeql.getBaseCube()));
  }

  private void removeAggreagateFromDefaultColumns(ASTNode node) throws LensException {
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) node.getChild(i);
      if (selectExpr.getChildCount() == 2) {
        ASTNode column = (ASTNode) selectExpr.getChild(0);
        if (HQLParser.isAggregateAST(column)
            && column.getChildCount() == 2) {
          if (HQLParser.getString((ASTNode) column.getChild(1)).equals(DEFAULT_MEASURE)) {
            selectExpr.getParent().setChild(i, getSelectExpr(null, (ASTNode) selectExpr.getChild(1), true));
          }
        }
      }
    }

  }
}
