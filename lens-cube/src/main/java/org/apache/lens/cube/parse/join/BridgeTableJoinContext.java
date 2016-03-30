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
package org.apache.lens.cube.parse.join;

import static org.apache.lens.cube.parse.HQLParser.*;

import java.util.*;

import org.apache.lens.cube.metadata.join.TableRelationship;
import org.apache.lens.cube.parse.*;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Join context related to Bridge tables
 */
@Slf4j
public class BridgeTableJoinContext {
  private final String bridgeTableFieldAggr;
  private final String arrayFilter;
  private final CubeQueryContext cubeql;
  private final CandidateFact fact;
  private final QueryAST queryAST;
  private final boolean doFlatteningEarly;
  private boolean initedBridgeClauses = false;
  private final StringBuilder bridgeSelectClause = new StringBuilder();
  private final StringBuilder bridgeFromClause = new StringBuilder();
  private final StringBuilder bridgeFilterClause = new StringBuilder();
  private final StringBuilder bridgeJoinClause = new StringBuilder();
  private final StringBuilder bridgeGroupbyClause = new StringBuilder();

  public BridgeTableJoinContext(CubeQueryContext cubeql, CandidateFact fact, QueryAST queryAST,
    String bridgeTableFieldAggr, String arrayFilter, boolean doFlatteningEarly) {
    this.cubeql = cubeql;
    this.queryAST = queryAST;
    this.fact = fact;
    this.bridgeTableFieldAggr = bridgeTableFieldAggr;
    this.arrayFilter = arrayFilter;
    this.doFlatteningEarly = doFlatteningEarly;
  }

  public void resetContext() {
    initedBridgeClauses = false;
    bridgeSelectClause.setLength(0);
    bridgeFromClause.setLength(0);
    bridgeFilterClause.setLength(0);
    bridgeJoinClause.setLength(0);
    bridgeGroupbyClause.setLength(0);
  }

  public void initBridgeClauses(TableRelationship rel, String fromAlias, String toAlias, CandidateDim toTable, String
    userFilter,
    String storageFilter) {
    // we just found a bridge table in the path we need to initialize the clauses for subquery required for
    // aggregating fields of bridge table
    // initialize select clause with join key
    bridgeSelectClause.append(" (select ").append(toAlias).append(".").append(rel.getToColumn()).append(" as ")
      .append(rel.getToColumn());
    // group by join key
    bridgeGroupbyClause.append(" group by ").append(toAlias).append(".").append(rel.getToColumn());
    // from clause with bridge table
    bridgeFromClause.append(" from ").append(toTable.getStorageString(toAlias));
    // we need to initialize filter clause with user filter clause or storage filter if applicable
    if (StringUtils.isNotBlank(userFilter)) {
      bridgeFilterClause.append(userFilter);
    }
    if (StringUtils.isNotBlank(storageFilter)) {
      if (StringUtils.isNotBlank(bridgeFilterClause.toString())) {
        bridgeFilterClause.append(" and ");
      }
      bridgeFilterClause.append(storageFilter);
    }
    // initialize final join clause
    bridgeJoinClause.append(" on ").append(fromAlias).append(".")
      .append(rel.getFromColumn()).append(" = ").append("%s")
      .append(".").append(rel.getToColumn());
    initedBridgeClauses = true;
  }

  // if any relation has bridge table, the clause becomes the following :
  // join (" select " + joinkey + " aggr over fields from bridge table + from bridgeTable + [where user/storage
  // filters] + groupby joinkey) on joincond"
  // Or
  // " join (select " + joinkey + " aggr over fields from table reached through bridge table + from bridge table
  // join <next tables> on join condition + [and user/storage filters] + groupby joinkey) on joincond
  public void updateBridgeClause(TableRelationship rel, String fromAlias, String toAlias, CandidateDim toTable,
    String userFilter, String storageFilter) {
    if (!initedBridgeClauses) {
      initBridgeClauses(rel, fromAlias, toAlias, toTable, userFilter, storageFilter);
    } else {
      addAnotherJoinClause(rel, fromAlias, toAlias, toTable, userFilter, storageFilter);
    }
  }

  public void addAnotherJoinClause(TableRelationship rel, String fromAlias, String toAlias, CandidateDim toTable,
    String userFilter, String storageFilter) {
    // if bridge clauses are already inited, this is a next table getting joined with bridge table
    // we will append a simple join clause
    bridgeFromClause.append(" join ");
    bridgeFromClause.append(toTable.getStorageString(toAlias));
    bridgeFromClause.append(" on ").append(fromAlias).append(".")
      .append(rel.getFromColumn()).append(" = ").append(toAlias)
      .append(".").append(rel.getToColumn());

    if (StringUtils.isNotBlank(userFilter)) {
      bridgeFromClause.append(" and ").append(userFilter);
    }
    if (StringUtils.isNotBlank(storageFilter)) {
      bridgeFromClause.append(" and ").append(storageFilter);
    }
  }

  public String generateJoinClause(String joinTypeStr, String toAlias) throws LensException {
    StringBuilder clause = new StringBuilder(joinTypeStr);
    clause.append(" join ");
    clause.append(bridgeSelectClause.toString());
    // iterate over all select expressions and add them for select clause if do_flattening_early is disabled
    if (!doFlatteningEarly) {
      BridgeTableSelectCtx selectCtx = new BridgeTableSelectCtx(bridgeTableFieldAggr, arrayFilter, toAlias);
      selectCtx.processSelectAST(queryAST.getSelectAST());
      selectCtx.processWhereClauses(fact);
      selectCtx.processGroupbyAST(queryAST.getGroupByAST());
      selectCtx.processOrderbyAST(queryAST.getOrderByAST());
      clause.append(",").append(StringUtils.join(selectCtx.getSelectedBridgeExprs(), ","));
    } else {
      for (String col : cubeql.getTblAliasToColumns().get(toAlias)) {
        clause.append(",").append(bridgeTableFieldAggr).append("(").append(toAlias)
          .append(".").append(col)
          .append(")")
          .append(" as ").append(col);
      }
    }
    String bridgeFrom = bridgeFromClause.toString();
    clause.append(bridgeFrom);
    String bridgeFilter = bridgeFilterClause.toString();
    if (StringUtils.isNotBlank(bridgeFilter)) {
      if (bridgeFrom.contains(" join ")) {
        clause.append(" and ");
      } else {
        clause.append(" where ");
      }
      clause.append(bridgeFilter);
    }
    clause.append(bridgeGroupbyClause.toString());
    clause.append(") ").append(toAlias);
    clause.append(String.format(bridgeJoinClause.toString(), toAlias));
    return clause.toString();
  }

  @Data
  static class BridgeTableSelectCtx {
    private final HashMap<HashableASTNode, ASTNode> exprToDotAST = new HashMap<>();
    private final List<String> selectedBridgeExprs = new ArrayList<>();
    private final AliasDecider aliasDecider = new DefaultAliasDecider("balias");
    private final String bridgeTableFieldAggr;
    private final String arrayFilter;
    private final String tableAlias;

    List<String> processSelectAST(ASTNode selectAST)
      throws LensException {
      // iterate over children
      for (int i = 0; i < selectAST.getChildCount(); i++) {
        ASTNode selectExprNode = (ASTNode) selectAST.getChild(i);
        ASTNode child = (ASTNode) selectExprNode.getChild(0);
        if (hasBridgeCol(child, tableAlias)) {
          selectExprNode.setChild(0, getDotASTForExprAST(child));
        }
      }
      return selectedBridgeExprs;
    }

    private ASTNode getDotASTForExprAST(ASTNode child) {
      HashableASTNode hashAST = new HashableASTNode(child);
      if (!exprToDotAST.containsKey(hashAST)) {
        // add selected expression to get selected from bridge table, with a generated alias
        String colAlias = aliasDecider.decideAlias(child);
        selectedBridgeExprs.add(bridgeTableFieldAggr + "(" + HQLParser.getString(child) + ") as " + colAlias);

        // replace bridge expression with tableAlias.colAlias.
        ASTNode dot = HQLParser.getDotAST(tableAlias, colAlias);
        exprToDotAST.put(hashAST, dot);
      }
      return exprToDotAST.get(hashAST);
    }

    // process groupby
    void processGroupbyAST(ASTNode ast)
      throws LensException {
      if (ast == null) {
        return;
      }
      // iterate over children
      for (int i = 0; i < ast.getChildCount(); i++) {
        ASTNode exprNode = (ASTNode) ast.getChild(i);
        if (hasBridgeCol(exprNode, tableAlias)) {
          ast.setChild(i, getDotASTForExprAST(exprNode));
        }
      }
    }

    // process orderby
    void processOrderbyAST(ASTNode ast)
      throws LensException {
      if (ast == null) {
        return;
      }
      // iterate over children
      for (int i = 0; i < ast.getChildCount(); i++) {
        ASTNode exprNode = (ASTNode) ast.getChild(i);
        ASTNode child = (ASTNode) exprNode.getChild(0);
        if (hasBridgeCol(child, tableAlias)) {
          exprNode.setChild(0, getDotASTForExprAST(child));
        }
      }
    }

    void processWhereClauses(CandidateFact fact) throws LensException {

      for (Map.Entry<String, ASTNode> whereEntry : fact.getStorgeWhereClauseMap().entrySet()) {
        ASTNode whereAST = whereEntry.getValue();
        processWhereAST(whereAST, null, 0);
      }
    }

    void processWhereAST(ASTNode ast, ASTNode parent, int childPos)
      throws LensException {
      if (ast == null) {
        return;
      }
      ASTNode child;
      int replaceIndex = -1;
      if (isPrimitiveBooleanExpression(ast)) {
        replaceIndex = 0;
      } else if (isPrimitiveBooleanFunction(ast)) {
        replaceIndex = 1;
      }
      if (replaceIndex != -1) {
        child = (ASTNode) ast.getChild(replaceIndex);
        if (hasBridgeCol(child, tableAlias)) {
          ast.setChild(replaceIndex, getDotASTForExprAST(child));
          parent.setChild(childPos, replaceDirectFiltersWithArrayFilter(ast, arrayFilter));
        }
      }
      // recurse down
      for (int i = 0; i < ast.getChildCount(); i++) {
        processWhereAST((ASTNode) ast.getChild(i), ast, i);
      }
    }
  }
  /**
   * Update =, != and IN clause filters to arrayFilter. arrayFilter will have signature arrayFilter(col, value)
   *
   * @param ast AST for simple filter
   * @param arrayFilter arrayFilter function
   * @return ASTNode with converted filter
   *
   * @throws LensException
   */
  static ASTNode replaceDirectFiltersWithArrayFilter(ASTNode ast, String arrayFilter)
    throws LensException {
    StringBuilder filterBuilder = new StringBuilder();
    if ((ast.getType() == HiveParser.EQUAL || ast.getType() == HiveParser.NOTEQUAL)) {
      String colStr = getString((ASTNode) ast.getChild(0));
      if (ast.getType() == HiveParser.NOTEQUAL) {
        filterBuilder.append(" NOT ");
      }
      filterBuilder.append(arrayFilter);
      filterBuilder.append("(");
      filterBuilder.append(colStr).append(",");
      filterBuilder.append(getString((ASTNode)ast.getChild(1)));
      filterBuilder.append(")");
    } else if (ast.getType() == HiveParser.TOK_FUNCTION) {
      // This is IN clause as function
      String colStr = getString((ASTNode) ast.getChild(1));
      filterBuilder.append("(");
      for (int i = 2; i < ast.getChildCount(); i++) {
        filterBuilder.append(arrayFilter);
        filterBuilder.append("(");
        filterBuilder.append(colStr).append(",");
        filterBuilder.append(ast.getChild(i).getText());
        filterBuilder.append(")");
        if (i + 1 != ast.getChildCount()) {
          filterBuilder.append(" OR ");
        }
      }
      filterBuilder.append(")");
    }
    String finalFilter = filterBuilder.toString();
    if (StringUtils.isNotBlank(finalFilter)) {
      return HQLParser.parseExpr(finalFilter);
    }
    return ast;
  }

  static boolean hasBridgeCol(ASTNode astNode, String tableAlias) throws LensException {
    Set<String> bridgeCols = HQLParser.getColsInExpr(tableAlias, astNode);
    return !bridgeCols.isEmpty();
  }
}
