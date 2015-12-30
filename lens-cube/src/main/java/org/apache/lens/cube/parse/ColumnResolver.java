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

import java.util.HashSet;
import java.util.Set;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.parse.HQLParser.ASTNodeVisitor;
import org.apache.lens.cube.parse.HQLParser.TreeNode;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import com.google.common.base.Optional;

class ColumnResolver implements ContextRewriter {

  public ColumnResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    extractColumns(cubeql);
  }

  private void extractColumns(CubeQueryContext cubeql) throws LensException {
    // Check if its 'select * from...'
    ASTNode selTree = cubeql.getSelectAST();
    if (selTree.getChildCount() == 1) {
      ASTNode star = HQLParser.findNodeByPath(selTree, TOK_SELEXPR, TOK_ALLCOLREF);
      if (star == null) {
        star = HQLParser.findNodeByPath(selTree, TOK_SELEXPR, TOK_FUNCTIONSTAR);
      }

      if (star != null) {
        int starType = star.getToken().getType();
        if (TOK_FUNCTIONSTAR == starType || TOK_ALLCOLREF == starType) {
          throw new LensException(LensCubeErrorCode.ALL_COLUMNS_NOT_SUPPORTED.getLensErrorInfo());
        }
      }
    }
    getColsForSelectTree(cubeql);
    getColsForWhereTree(cubeql);
    getColsForTree(cubeql, cubeql.getJoinAST(), cubeql);
    getColsForTree(cubeql, cubeql.getGroupByAST(), cubeql);
    getColsForTree(cubeql, cubeql.getHavingAST(), cubeql);
    getColsForTree(cubeql, cubeql.getOrderByAST(), cubeql);

    // Update join dimension tables
    for (String table : cubeql.getTblAliasToColumns().keySet()) {
      if (!CubeQueryContext.DEFAULT_TABLE.equalsIgnoreCase(table)) {
        if (!cubeql.addQueriedTable(table)) {
          throw new LensException(LensCubeErrorCode.NEITHER_CUBE_NOR_DIMENSION.getLensErrorInfo());
        }
      }
    }
  }

  // finds columns in AST passed.
  static void getColsForTree(final CubeQueryContext cubeql, ASTNode tree, final TrackQueriedColumns tqc)
    throws LensException {
    if (tree == null) {
      return;
    }
    // Traverse the tree to get column names
    // We are doing a complete traversal so that expressions of columns
    // are also captured ex: f(cola + colb/tab1.colc)
    HQLParser.bft(tree, new ASTNodeVisitor() {
      @Override
      public void visit(TreeNode visited) {
        ASTNode node = visited.getNode();
        ASTNode parent = null;
        if (visited.getParent() != null) {
          parent = visited.getParent().getNode();
        }

        if (node.getToken().getType() == TOK_TABLE_OR_COL && (parent != null && parent.getToken().getType() != DOT)) {
          // Take child ident.totext
          ASTNode ident = (ASTNode) node.getChild(0);
          String column = ident.getText().toLowerCase();
          if (cubeql.getExprToAliasMap().values().contains(column)) {
            // column is an existing alias
            return;
          }
          tqc.addColumnsQueried(CubeQueryContext.DEFAULT_TABLE, column);
        } else if (node.getToken().getType() == DOT) {
          // This is for the case where column name is prefixed by table name
          // or table alias
          // For example 'select fact.id, dim2.id ...'
          // Right child is the column name, left child.ident is table name
          ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
          ASTNode colIdent = (ASTNode) node.getChild(1);

          String column = colIdent.getText().toLowerCase();
          String table = tabident.getText().toLowerCase();
          tqc.addColumnsQueried(table, column);
        }
      }
    });
  }

  // find columns in where tree
  // if where expression is timerange function, then time range columns are
  // added
  // only if timerange clause shouldn't be replaced with its correspodning
  // partition column
  private void getColsForWhereTree(final CubeQueryContext cubeql) throws LensException {
    if (cubeql.getWhereAST() == null) {
      return;
    }
    addColumnsForWhere(cubeql, cubeql.getWhereAST(), null);
  }

  // Find all columns of select tree.
  // Finds columns in each select expression.
  //
  // Updates alias for each selected expression.
  // Alias is updated as follows:
  // Case 1: If select expression does not have an alias
  // ** And the expression has only one column queried, the column name is put
  // as
  // select alias.
  // ** If the expression has more than one column queried, the alias is
  // constructed as
  // 'expr' + index of the expression.
  // Case 2: If select expression already has alias
  // ** Adds it to exprToAlias map
  // ** and the alias does not have spaces, the select alias and final alias is
  // same.
  // ** If alias has spaces, select alias is constructed as 'expr' + index of
  // the expression
  // and user given alias is the final alias of the expression.
  private static final String SELECT_ALIAS_PREFIX = "expr";

  private void getColsForSelectTree(final CubeQueryContext cubeql) throws LensException {
    int exprInd = 1;
    for (int i = 0; i < cubeql.getSelectAST().getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) cubeql.getSelectAST().getChild(i);
      Set<String> cols = new HashSet<String>();
      addColumnsForSelectExpr(cubeql, selectExpr, cubeql.getSelectAST(), cols);
      ASTNode alias = HQLParser.findNodeByPath(selectExpr, Identifier);
      String selectAlias;
      String selectFinalAlias = null;
      if (alias != null) {
        cubeql.addExprToAlias(selectExpr, alias);
        if (HQLParser.hasSpaces(alias.getText())) {
          selectFinalAlias = alias.getText();
          selectAlias = SELECT_ALIAS_PREFIX + exprInd;
        } else {
          selectAlias = alias.getText().trim();
        }
      } else if (cols.size() == 1) {
        // add the column name as alias
        selectAlias = cols.iterator().next().toLowerCase();
      } else {
        selectAlias = SELECT_ALIAS_PREFIX + exprInd;
      }
      exprInd++;
      cubeql.addSelectAlias(selectAlias, selectFinalAlias);
    }
  }

  private static void addColumnsForWhere(final CubeQueryContext cubeql, ASTNode node, ASTNode parent) {
    if (node.getToken().getType() == TOK_TABLE_OR_COL && (parent != null && parent.getToken().getType() != DOT)) {
      // Take child ident.totext
      ASTNode ident = (ASTNode) node.getChild(0);
      String column = ident.getText().toLowerCase();
      if (cubeql.getExprToAliasMap().values().contains(column)) {
        // column is an existing alias
        return;
      }

      addColumnQueriedWithTimeRangeFuncCheck(cubeql, parent, CubeQueryContext.DEFAULT_TABLE, column);

    } else if (node.getToken().getType() == DOT) {
      // This is for the case where column name is prefixed by table name
      // or table alias
      // For example 'select fact.id, dim2.id ...'
      // Right child is the column name, left child.ident is table name
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      String column = colIdent.getText().toLowerCase();
      String table = tabident.getText().toLowerCase();

      addColumnQueriedWithTimeRangeFuncCheck(cubeql, parent, table, column);

    } else if (node.getToken().getType() == TOK_FUNCTION) {
      ASTNode fname = HQLParser.findNodeByPath(node, Identifier);
      if (fname != null && CubeQueryContext.TIME_RANGE_FUNC.equalsIgnoreCase(fname.getText())) {
        addColumnsForWhere(cubeql, (ASTNode) node.getChild(1), node);
      } else {
        for (int i = 0; i < node.getChildCount(); i++) {
          addColumnsForWhere(cubeql, (ASTNode) node.getChild(i), node);
        }
      }
    } else {
      for (int i = 0; i < node.getChildCount(); i++) {
        addColumnsForWhere(cubeql, (ASTNode) node.getChild(i), node);
      }
    }
  }

  private static void addColumnQueriedWithTimeRangeFuncCheck(final CubeQueryContext cubeql, final ASTNode parent,
      final String table, final String column) {

    if (isTimeRangeFunc(parent)) {
      cubeql.addQueriedTimeDimensionCols(column);
      cubeql.addColumnsQueriedWithTimeDimCheck(CubeQueryContext.DEFAULT_TABLE, column);
    } else {
      cubeql.addColumnsQueried(table, column);
    }
  }

  private static boolean isTimeRangeFunc(final ASTNode node) {

    Optional<String> funcNameOp =  getNameIfFunc(node);
    final String funcName = funcNameOp.isPresent() ? funcNameOp.get() : null;
    return CubeQueryContext.TIME_RANGE_FUNC.equalsIgnoreCase(funcName);
  }

  private static Optional<String> getNameIfFunc(final ASTNode node) {

    String funcName = null;
    if (node.getToken().getType() == TOK_FUNCTION) {
      ASTNode foundNode = HQLParser.findNodeByPath(node, Identifier);
      if (foundNode != null) {
        funcName = foundNode.getText();
      }
    }
    return Optional.fromNullable(funcName);
  }
  private static void addColumnsForSelectExpr(final CubeQueryContext cubeql, ASTNode node, ASTNode parent,
    Set<String> cols) {
    if (node.getToken().getType() == TOK_TABLE_OR_COL && (parent != null && parent.getToken().getType() != DOT)) {
      // Take child ident.totext
      ASTNode ident = (ASTNode) node.getChild(0);
      String column = ident.getText().toLowerCase();
      if (cubeql.getExprToAliasMap().values().contains(column)) {
        // column is an existing alias
        return;
      }
      cubeql.addColumnsQueried(CubeQueryContext.DEFAULT_TABLE, column);
      cols.add(column);
    } else if (node.getToken().getType() == DOT) {
      // This is for the case where column name is prefixed by table name
      // or table alias
      // For example 'select fact.id, dim2.id ...'
      // Right child is the column name, left child.ident is table name
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      String column = colIdent.getText().toLowerCase();
      String table = tabident.getText().toLowerCase();
      cubeql.addColumnsQueried(table, column);
      cols.add(column);
    } else {
      for (int i = 0; i < node.getChildCount(); i++) {
        addColumnsForSelectExpr(cubeql, (ASTNode) node.getChild(i), node, cols);
      }
    }
  }
}
