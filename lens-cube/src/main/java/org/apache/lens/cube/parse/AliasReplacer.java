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
import java.util.Set;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

/**
 * Finds queried column to table alias. Finds queried dim attributes and queried measures.
 * <p/>
 * Does queried field validation wrt derived cubes, if all fields of queried cube cannot be queried together.
 * <p/>
 * Replaces all the columns in all expressions with tablealias.column
 */
class AliasReplacer implements ContextRewriter {

  public AliasReplacer(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    Map<String, String> colToTableAlias = cubeql.getColToTableAlias();

    extractTabAliasForCol(cubeql);
    // Resolve aliases in all queried phrases
    for (QueriedPhraseContext qur : cubeql.getQueriedPhrases()) {
      extractTabAliasForCol(colToTableAlias, qur);
    }
    findExpressionsAndMeasures(cubeql);

    if (colToTableAlias.isEmpty()) {
      return;
    }

    // Rewrite the all the columns in the query with table alias prefixed.
    // If col1 of table tab1 is accessed, it would be changed as tab1.col1.
    // If tab1 is already aliased say with t1, col1 is changed as t1.col1
    // replace the columns in select, groupby, having, orderby by
    // prepending the table alias to the col
    // sample select trees
    // 1: (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL key))
    // (TOK_SELEXPR (TOK_FUNCTION count (TOK_TABLE_OR_COL value))))
    // 2: (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key))
    // (TOK_SELEXPR (TOK_FUNCTION count (. (TOK_TABLE_OR_COL src) value))))
    // 3: (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key) srckey))))
    replaceAliases(cubeql.getSelectAST(), 0, colToTableAlias);

    replaceAliases(cubeql.getHavingAST(), 0, colToTableAlias);

    replaceAliases(cubeql.getOrderByAST(), 0, colToTableAlias);

    replaceAliases(cubeql.getGroupByAST(), 0, colToTableAlias);

    replaceAliases(cubeql.getWhereAST(), 0, colToTableAlias);

    replaceAliases(cubeql.getJoinAST(), 0, colToTableAlias);

  }

  /**
   * Figure out queried dim attributes and measures from the cube query context
   * @param cubeql
   * @throws LensException
   */
  private void findExpressionsAndMeasures(CubeQueryContext cubeql) throws LensException {
    CubeInterface cube = cubeql.getCube();
    if (cube != null) {
      String cubeAlias = cubeql.getAliasForTableName(cube.getName());
      for (QueriedPhraseContext qur : cubeql.getQueriedPhrases()) {
        Set<String> cubeColsQueried = qur.getColumnsQueried(cubeAlias);
        if (cubeColsQueried != null && !cubeColsQueried.isEmpty()) {
          for (String col : cubeColsQueried) {
            if (cube.getMeasureNames().contains(col)) {
              qur.addQueriedMsr(col);
            } else if (cube.getDimAttributeNames().contains(col)) {
              qur.addQueriedDimAttr(col);
            } else if (cube.getExpressionNames().contains(col)) {
              qur.addQueriedExprColumn(col);
            }
          }
        }
        cubeql.addQueriedMsrs(qur.getQueriedMsrs());
        cubeql.addQueriedExprs(qur.getQueriedExprColumns());
      }
    }
  }

  private void extractTabAliasForCol(CubeQueryContext cubeql) throws LensException {
    extractTabAliasForCol(cubeql, cubeql);
  }

  static void extractTabAliasForCol(CubeQueryContext cubeql, TrackQueriedColumns tqc) throws LensException {
    Map<String, String> colToTableAlias = cubeql.getColToTableAlias();
    Set<String> columns = tqc.getTblAliasToColumns().get(CubeQueryContext.DEFAULT_TABLE);
    if (columns == null) {
      return;
    }
    for (String col : columns) {
      boolean inCube = false;
      if (cubeql.getCube() != null) {
        Set<String> cols = cubeql.getCube().getAllFieldNames();
        if (cols.contains(col.toLowerCase())) {
          String cubeAlias = cubeql.getAliasForTableName(cubeql.getCube().getName());
          colToTableAlias.put(col.toLowerCase(), cubeAlias);
          tqc.addColumnsQueried(cubeAlias, col.toLowerCase());
          inCube = true;
        }
      }
      for (Dimension dim : cubeql.getDimensions()) {
        if (dim.getAllFieldNames().contains(col.toLowerCase())) {
          if (!inCube) {
            String prevDim = colToTableAlias.get(col.toLowerCase());
            if (prevDim != null && !prevDim.equals(dim.getName())) {
              throw new LensException(LensCubeErrorCode.AMBIGOUS_DIM_COLUMN.getLensErrorInfo(),
                  col, prevDim, dim.getName());
            }
            String dimAlias = cubeql.getAliasForTableName(dim.getName());
            colToTableAlias.put(col.toLowerCase(), dimAlias);
            tqc.addColumnsQueried(dimAlias, col.toLowerCase());
          } else {
            // throw error because column is in both cube and dimension table
            throw new LensException(LensCubeErrorCode.AMBIGOUS_CUBE_COLUMN.getLensErrorInfo(), col,
                cubeql.getCube().getName(), dim.getName());
          }
        }
      }
      if (colToTableAlias.get(col.toLowerCase()) == null) {
        throw new LensException(LensCubeErrorCode.COLUMN_NOT_FOUND.getLensErrorInfo(), col);
      }
    }
  }

  static void extractTabAliasForCol(Map<String, String> colToTableAlias, TrackQueriedColumns tqc) throws LensException {
    Set<String> columns = tqc.getTblAliasToColumns().get(CubeQueryContext.DEFAULT_TABLE);
    if (columns == null) {
      return;
    }
    for (String col : columns) {
      tqc.addColumnsQueried(colToTableAlias.get(col.toLowerCase()), col.toLowerCase());
      if (colToTableAlias.get(col.toLowerCase()) == null) {
        throw new LensException(LensCubeErrorCode.COLUMN_NOT_FOUND.getLensErrorInfo(), col);
      }
    }
  }
  static ASTNode replaceAliases(ASTNode node, int nodePos, Map<String, String> colToTableAlias) {
    if (node == null) {
      return node;
    }

    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      String colName = HQLParser.getColName(node);
      String newAlias = colToTableAlias.get(colName.toLowerCase());

      if (StringUtils.isBlank(newAlias)) {
        return node;
      }

      if (nodeType == HiveParser.DOT) {
        // No need to create a new node, just replace the table name ident
        ASTNode aliasNode = (ASTNode) node.getChild(0);
        ASTNode newAliasIdent = new ASTNode(new CommonToken(HiveParser.Identifier, newAlias));
        aliasNode.setChild(0, newAliasIdent);
      } else {
        // Just a column ref, we need to make it alias.col
        // '.' will become the parent node
        ASTNode dot = new ASTNode(new CommonToken(HiveParser.DOT, "."));
        ASTNode aliasIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, newAlias));
        ASTNode tabRefNode = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"));

        tabRefNode.addChild(aliasIdentNode);
        dot.addChild(tabRefNode);

        ASTNode colIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, colName));
        dot.addChild(colIdentNode);

        ASTNode parent = (ASTNode) node.getParent();
        if (parent != null) {
          parent.setChild(nodePos, dot);
        } else {
          return dot;
        }
      }
    } else {
      // recurse down
      for (int i = 0; i < node.getChildCount(); i++) {
        ASTNode child = (ASTNode) node.getChild(i);
        replaceAliases(child, i, colToTableAlias);
      }
    }
    return node;
  }

}
