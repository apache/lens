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

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.lens.cube.metadata.AbstractBaseTable;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.Tree;

import lombok.extern.slf4j.Slf4j;

/**
 * Promotes groupby to select and select to groupby.
 */
@Slf4j
class GroupbyResolver implements ContextRewriter {

  private final boolean selectPromotionEnabled;
  private final boolean groupbyPromotionEnabled;

  public GroupbyResolver(Configuration conf) {
    selectPromotionEnabled =
      conf.getBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, CubeQueryConfUtil.DEFAULT_ENABLE_SELECT_TO_GROUPBY);
    groupbyPromotionEnabled =
      conf.getBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT,
        CubeQueryConfUtil.DEFAULT_ENABLE_GROUP_BY_TO_SELECT);
  }

  private void promoteSelect(CubeQueryContext cubeql, List<String> nonMsrNonAggSelExprsWithoutAlias,
    List<String> groupByExprs) throws LensException {
    if (!selectPromotionEnabled) {
      return;
    }

    if (!groupByExprs.isEmpty()) {
      log.info("Not promoting select expression to groupby, since there are already group by expressions");
      return;
    }

    // each selected column, if it is not a cube measure, and does not have
    // aggregation on the column, then it is added to group by columns.
    if (cubeql.hasAggregates()) {
      for (String expr : nonMsrNonAggSelExprsWithoutAlias) {

        if (!groupByExprs.contains(expr)) {
          if (!cubeql.isAggregateExpr(expr)) {
            ASTNode exprAST = HQLParser.parseExpr(expr);
            ASTNode groupbyAST = cubeql.getGroupByAST();
            if (!isConstantsUsed(exprAST)) {
              if (groupbyAST != null) {
                // groupby ast exists, add the expression to AST
                groupbyAST.addChild(exprAST);
              } else {
                // no group by ast exist, create one
                ASTNode newAST = new ASTNode(new CommonToken(TOK_GROUPBY));
                newAST.addChild(exprAST);
                cubeql.setGroupByAST(newAST);
              }
            }
          }
        }
      }
    }
  }

  /*
   * Check if constants projected
   */
  private boolean isConstantsUsed(ASTNode node) {
    return node != null && node.getToken() != null && !hasTableOrColumn(node);
  }


  /*
   * Check if table or column used in node
   */
  private boolean hasTableOrColumn(ASTNode node) {
    if (node.getToken() != null) {
      if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
        return true;
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasTableOrColumn((ASTNode) node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  private void promoteGroupby(CubeQueryContext cubeql, List<String> selectExprs, List<String> groupByExprs)
    throws LensException {
    if (!groupbyPromotionEnabled) {
      return;
    }

    for (String expr : selectExprs) {
      expr = getExpressionWithoutAlias(cubeql, expr);
      if (!cubeql.isAggregateExpr(expr)) {
        log.info("Not promoting groupby expression to select, since there are expression projected");
        return;
      }
    }

    int index = 0;
    for (String expr : groupByExprs) {
      if (!contains(cubeql, selectExprs, expr)) {
        ASTNode exprAST = HQLParser.parseExpr(expr);
        addChildAtIndex(index, cubeql.getSelectAST(), exprAST);
        index++;
      }
    }
  }

  private void addChildAtIndex(int index, ASTNode parent, ASTNode child) {
    // add the last child
    int count = parent.getChildCount();
    Tree lastchild = parent.getChild(count - 1);
    parent.addChild(lastchild);

    // move all the children from last upto index
    for (int i = count - 2; i >= index; i--) {
      Tree ch = parent.getChild(i);
      parent.setChild(i + 1, ch);
    }
    parent.setChild(index, child);
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    // Process Aggregations by making sure that all group by keys are projected;
    // and all projection fields are added to group by keylist;
    List<String> selectExprs = new ArrayList<String>();
    String[] sel = getExpressions(cubeql.getSelectAST(), cubeql).toArray(new String[]{});
    for (String s : sel) {
      selectExprs.add(s.trim());
    }
    List<String> groupByExprs = new ArrayList<String>();
    if (cubeql.getGroupByTree() != null) {
      String[] gby = getExpressions(cubeql.getGroupByAST(), cubeql).toArray(new String[]{});
      for (String g : gby) {
        groupByExprs.add(g.trim());
      }
    }
    promoteSelect(cubeql, getNonMsrNonAggSelExprsWithoutAlias(cubeql.getSelectAST(), cubeql), groupByExprs);
    promoteGroupby(cubeql, selectExprs, groupByExprs);
  }

  private String getExpressionWithoutAlias(CubeQueryContext cubeql, String sel) {
    String alias = cubeql.getAlias(sel);
    if (alias != null) {
      sel = sel.substring(0, (sel.length() - alias.length())).trim();
    }
    return sel;
  }

  private boolean contains(CubeQueryContext cubeql, List<String> selExprs, String expr) {
    for (String sel : selExprs) {
      sel = getExpressionWithoutAlias(cubeql, sel);
      if (sel.equals(expr)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param selectASTNode a select AST Node
   * @param cubeQueryCtx
   * @return List of non measure and non aggregate select expressions in string format without aliases
   */
  private List<String> getNonMsrNonAggSelExprsWithoutAlias(final ASTNode selectASTNode, CubeQueryContext cubeQueryCtx) {

    List<String> nonMsrNonAggSelExprsWithoutAlias = new LinkedList<String>();
    List<ASTNode> nonMsrNonAggSelASTChildren = filterNonMsrNonAggSelectASTChildren(selectASTNode, cubeQueryCtx);

    for (ASTNode nonMsrNonAggSelASTChild : nonMsrNonAggSelASTChildren) {

      /* Assuming all children of SelectASTNode are SELECT Expression AST Nodes only.
      Refer:https://reviews.apache.org/r/29422/#comment109498 for more details.
      Order of Children of select expression AST Node => Index 0: Select Expression Without Alias, Index 1: Alias */

      ASTNode selExprWithoutAlias = (ASTNode) nonMsrNonAggSelASTChild.getChildren().get(0);
      String result = HQLParser.getString(selExprWithoutAlias);
      nonMsrNonAggSelExprsWithoutAlias.add(result);

    }
    return nonMsrNonAggSelExprsWithoutAlias;
  }

  /**
   * @param selectASTNode a select ASTNode
   * @param cubeQueryCtx
   * @return list of selectASTNode Children which does not contain a measure or an aggregate. Empty list is returned
   * when selectASTNode is not a Select AST Node. Empty list is returned when there are no non measure and non aggregate
   * children nodes present in select AST.
   */
  private List<ASTNode> filterNonMsrNonAggSelectASTChildren(final ASTNode selectASTNode,
    CubeQueryContext cubeQueryCtx) {
    List<ASTNode> nonMsrNonAggSelASTChildren = new LinkedList<ASTNode>();

    if (!HQLParser.isSelectASTNode(selectASTNode)) {
      return nonMsrNonAggSelASTChildren;
    }

    for (int i = 0; i < selectASTNode.getChildCount(); i++) {
      ASTNode childNode = (ASTNode) selectASTNode.getChild(i);
      if (hasMeasure(childNode, cubeQueryCtx) || hasAggregate(childNode, cubeQueryCtx)) {
        continue;
      }
      nonMsrNonAggSelASTChildren.add(childNode);
    }

    return nonMsrNonAggSelASTChildren;
  }

  private List<String> getExpressions(ASTNode node, CubeQueryContext cubeql) {

    List<String> list = new ArrayList<String>();

    if (node == null) {
      return list;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      if (hasMeasure(child, cubeql)) {
        continue;
      }
      if (hasAggregate(child, cubeql)) {
        continue;
      }
      list.add(HQLParser.getString((ASTNode) node.getChild(i)));
    }

    return list;
  }

  boolean hasAggregate(ASTNode node, CubeQueryContext cubeql) {
    int nodeType = node.getToken().getType();
    if (nodeType == TOK_TABLE_OR_COL || nodeType == DOT) {
      String colname;
      String alias = "";

      if (node.getToken().getType() == TOK_TABLE_OR_COL) {
        colname = ((ASTNode) node.getChild(0)).getText().toLowerCase();
      } else {
        // node in 'alias.column' format
        ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
        ASTNode colIdent = (ASTNode) node.getChild(1);

        colname = colIdent.getText().toLowerCase();
        alias = tabident.getText().toLowerCase();
      }
      // by the time Groupby resolver is looking for aggregate, all columns should be aliased with correct
      // alias name.
      if (cubeql.getCubeTableForAlias(alias) instanceof AbstractBaseTable) {
        if (((AbstractBaseTable)cubeql.getCubeTableForAlias(alias)).getExpressionByName(colname) != null) {
          return cubeql.getExprCtx().getExpressionContext(colname, alias).hasAggregates();
        }
      }
    } else {
      if (HQLParser.isAggregateAST(node)) {
        return true;
      }

      for (int i = 0; i < node.getChildCount(); i++) {
        if (hasAggregate((ASTNode) node.getChild(i), cubeql)) {
          return true;
        }
      }
    }
    return false;
  }

  boolean hasMeasure(ASTNode node, CubeQueryContext cubeql) {
    int nodeType = node.getToken().getType();
    if (nodeType == TOK_TABLE_OR_COL || nodeType == DOT) {
      String colname;
      String tabname = null;

      if (node.getToken().getType() == TOK_TABLE_OR_COL) {
        colname = ((ASTNode) node.getChild(0)).getText();
      } else {
        // node in 'alias.column' format
        ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
        ASTNode colIdent = (ASTNode) node.getChild(1);

        colname = colIdent.getText();
        tabname = tabident.getText();
      }

      String msrname = StringUtils.isBlank(tabname) ? colname : tabname + "." + colname;
      if (cubeql.hasCubeInQuery() && cubeql.isCubeMeasure(msrname)) {
        return true;
      }
    } else {
      for (int i = 0; i < node.getChildCount(); i++) {
        if (hasMeasure((ASTNode) node.getChild(i), cubeql)) {
          return true;
        }
      }
    }
    return false;
  }

}
