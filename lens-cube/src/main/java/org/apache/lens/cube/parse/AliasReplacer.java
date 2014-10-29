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

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SELEXPR;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.CommonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.AbstractCubeTable;
import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.DerivedCube;
import org.apache.lens.cube.metadata.Dimension;

/**
 * Finds queried column to table alias. Finds queried dim attributes and queried
 * measures.
 * 
 * Does queried field validation wrt derived cubes, if all fields of queried
 * cube cannot be queried together.
 * 
 * Replaces all the columns in all expressions with tablealias.column
 * 
 */
class AliasReplacer implements ContextRewriter {

  private static Log LOG = LogFactory.getLog(AliasReplacer.class.getName());

  // Mapping of a qualified column name to its table alias
  private Map<String, String> colToTableAlias;

  public AliasReplacer(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    colToTableAlias = new HashMap<String, String>();
    extractTabAliasForCol(cubeql);
    doFieldValidation(cubeql);

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
    if (colToTableAlias == null) {
      return;
    }

    ASTNode selectAST = cubeql.getSelectAST();
    replaceAliases(selectAST, 0, colToTableAlias);

    ASTNode havingAST = cubeql.getHavingAST();
    replaceAliases(havingAST, 0, colToTableAlias);

    ASTNode orderByAST = cubeql.getOrderByAST();
    replaceAliases(orderByAST, 0, colToTableAlias);

    ASTNode groupByAST = cubeql.getGroupByAST();
    replaceAliases(groupByAST, 0, colToTableAlias);

    ASTNode whereAST = cubeql.getWhereAST();
    replaceAliases(whereAST, 0, colToTableAlias);

    ASTNode joinAST = cubeql.getJoinTree();
    replaceAliases(joinAST, 0, colToTableAlias);

    // Update the aggregate expression set
    AggregateResolver.updateAggregates(selectAST, cubeql);
    AggregateResolver.updateAggregates(havingAST, cubeql);
    // Update alias map as well
    updateAliasMap(selectAST, cubeql);

  }

  // Finds all queried dim-attributes and measures from cube
  // If all fields in cube are not queryable together, does the validation
  // wrt to dervided cubes.
  private void doFieldValidation(CubeQueryContext cubeql) throws SemanticException {
    CubeInterface cube = cubeql.getCube();
    if (cube != null) {
      Set<String> cubeColsQueried = cubeql.getColumnsQueried(cube.getName());
      Set<String> queriedDimAttrs = new HashSet<String>();
      Set<String> queriedMsrs = new HashSet<String>();
      for (String col : cubeColsQueried) {
        if (cube.getMeasureNames().contains(col)) {
          queriedMsrs.add(col);
        } else if (cube.getDimAttributeNames().contains(col)) {
          queriedDimAttrs.add(col);
        }
      }
      cubeql.addQueriedDimAttrs(queriedDimAttrs);
      cubeql.addQueriedMsrs(queriedMsrs);
      if (!cube.allFieldsQueriable()) {
        // do queried field validation
        List<DerivedCube> dcubes;
        try {
          dcubes = cubeql.getMetastoreClient().getAllDerivedQueryableCubes(cube);
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        // do validation
        // Find atleast one derived cube which contains all the dimensions
        // queried.
        boolean derivedCubeFound = false;
        for (DerivedCube dcube : dcubes) {
          if (dcube.getDimAttributeNames().containsAll(queriedDimAttrs)) {
            // remove all the measures that are covered
            queriedMsrs.removeAll(dcube.getMeasureNames());
            derivedCubeFound = true;
          }
        }
        if (!derivedCubeFound) {
          throw new SemanticException(ErrorMsg.FIELDS_NOT_QUERYABLE, queriedDimAttrs.toString());
        }
        if (!queriedMsrs.isEmpty()) {
          // Add appropriate message to know which fields are not queryable
          // together
          if (!queriedDimAttrs.isEmpty()) {
            throw new SemanticException(ErrorMsg.FIELDS_NOT_QUERYABLE, queriedDimAttrs.toString() + " and "
                + queriedMsrs.toString());
          } else {
            throw new SemanticException(ErrorMsg.FIELDS_NOT_QUERYABLE, queriedMsrs.toString());
          }
        }
      }
    }
  }

  private void extractTabAliasForCol(CubeQueryContext cubeql) throws SemanticException {
    Set<String> columns = cubeql.getTblAlaisToColumns().get(CubeQueryContext.DEFAULT_TABLE);
    if (columns == null) {
      return;
    }
    for (String col : columns) {
      boolean inCube = false;
      if (cubeql.getCube() != null) {
        Set<String> cols = cubeql.getCube().getAllFieldNames();
        if (cols.contains(col.toLowerCase())) {
          colToTableAlias.put(col.toLowerCase(), cubeql.getAliasForTabName(cubeql.getCube().getName()));
          cubeql.addColumnsQueried((AbstractCubeTable) cubeql.getCube(), col.toLowerCase());
          inCube = true;
        }
      }
      for (Dimension dim : cubeql.getDimensions()) {
        if (dim.getAllFieldNames().contains(col.toLowerCase())) {
          if (!inCube) {
            String prevDim = colToTableAlias.get(col.toLowerCase());
            if (prevDim != null && !prevDim.equals(dim.getName())) {
              throw new SemanticException(ErrorMsg.AMBIGOUS_DIM_COLUMN, col, prevDim, dim.getName());
            }
            colToTableAlias.put(col.toLowerCase(), cubeql.getAliasForTabName(dim.getName()));
            cubeql.addColumnsQueried(dim, col.toLowerCase());
          } else {
            // throw error because column is in both cube and dimension table
            throw new SemanticException(ErrorMsg.AMBIGOUS_CUBE_COLUMN, col, cubeql.getCube().getName(), dim.getName());
          }
        }
      }
      if (colToTableAlias.get(col.toLowerCase()) == null) {
        throw new SemanticException(ErrorMsg.COLUMN_NOT_FOUND, col);
      }
    }
  }

  private void replaceAliases(ASTNode node, int nodePos, Map<String, String> colToTableAlias) {
    if (node == null) {
      return;
    }

    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      String colName = HQLParser.getColName(node);
      String newAlias = colToTableAlias.get(colName.toLowerCase());

      if (StringUtils.isBlank(newAlias)) {
        return;
      }

      if (nodeType == HiveParser.DOT) {
        // No need to create a new node, just replace the table name ident
        ASTNode aliasNode = (ASTNode) node.getChild(0);
        ASTNode newAliasIdent = new ASTNode(new CommonToken(HiveParser.Identifier, newAlias));
        aliasNode.setChild(0, newAliasIdent);
        newAliasIdent.setParent(aliasNode);
      } else {
        // Just a column ref, we need to make it alias.col
        // '.' will become the parent node
        ASTNode dot = new ASTNode(new CommonToken(HiveParser.DOT, "."));
        ASTNode aliasIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, newAlias));
        ASTNode tabRefNode = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"));

        tabRefNode.addChild(aliasIdentNode);
        aliasIdentNode.setParent(tabRefNode);
        dot.addChild(tabRefNode);
        tabRefNode.setParent(dot);

        ASTNode colIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, colName));

        dot.addChild(colIdentNode);

        ASTNode parent = (ASTNode) node.getParent();

        parent.setChild(nodePos, dot);
      }
    } else {
      // recurse down
      for (int i = 0; i < node.getChildCount(); i++) {
        ASTNode child = (ASTNode) node.getChild(i);
        replaceAliases(child, i, colToTableAlias);
      }
    }
  }

  private void updateAliasMap(ASTNode root, CubeQueryContext cubeql) {
    if (root == null) {
      return;
    }

    if (root.getToken().getType() == TOK_SELEXPR) {
      ASTNode alias = HQLParser.findNodeByPath(root, Identifier);
      if (alias != null) {
        cubeql.addExprToAlias(root, alias);
      }
    } else {
      for (int i = 0; i < root.getChildCount(); i++) {
        updateAliasMap((ASTNode) root.getChild(i), cubeql);
      }
    }
  }

}
