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
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL;

import java.util.Iterator;

import org.antlr.runtime.CommonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.CubeMeasure;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CubeTableCause;

/**
 * <p>
 * Replace select and having columns with default aggregate functions on them,
 * if default aggregate is defined and if there isn't already an aggregate
 * function specified on the columns.
 * </p>
 * 
 * <p>
 * Expressions which already contain aggregate sub-expressions will not be
 * changed.
 * </p>
 * 
 * <p>
 * At this point it's assumed that aliases have been added to all columns.
 * </p>
 */
class AggregateResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(AggregateResolver.class.getName());

  public AggregateResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getCube() == null) {
      return;
    }

    boolean nonDefaultAggregates = false;
    boolean aggregateResolverDisabled =
        cubeql.getHiveConf().getBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER,
            CubeQueryConfUtil.DEFAULT_DISABLE_AGGREGATE_RESOLVER);
    // Check if the query contains measures
    // 1. not inside default aggregate expressions
    // 2. With no default aggregate defined
    // 3. there are distinct selection of measures
    // If yes, only the raw (non aggregated) fact can answer this query.
    // In that case remove aggregate facts from the candidate fact list
    if (hasMeasuresInDistinctClause(cubeql, cubeql.getSelectAST(), false)
        || hasMeasuresInDistinctClause(cubeql, cubeql.getHavingAST(), false)
        || hasMeasuresNotInDefaultAggregates(cubeql, cubeql.getSelectAST(), null, aggregateResolverDisabled)
        || hasMeasuresNotInDefaultAggregates(cubeql, cubeql.getHavingAST(), null, aggregateResolverDisabled)
        || hasMeasures(cubeql, cubeql.getWhereAST()) || hasMeasures(cubeql, cubeql.getGroupByAST())
        || hasMeasures(cubeql, cubeql.getOrderByAST())) {
      Iterator<CandidateFact> factItr = cubeql.getCandidateFactTables().iterator();
      while (factItr.hasNext()) {
        CandidateFact candidate = factItr.next();
        if (candidate.fact.isAggregated()) {
          cubeql.addFactPruningMsgs(candidate.fact, new CandidateTablePruneCause(candidate.fact.getName(),
              CubeTableCause.MISSING_DEFAULT_AGGREGATE));
          factItr.remove();
        }
      }
      nonDefaultAggregates = true;
      LOG.info("Query has non default aggregates, no aggregate resolution will be done");
    }

    cubeql.pruneCandidateFactSet(CubeTableCause.MISSING_DEFAULT_AGGREGATE);

    if (nonDefaultAggregates || aggregateResolverDisabled) {
      return;
    }

    resolveClause(cubeql, cubeql.getSelectAST());

    resolveClause(cubeql, cubeql.getHavingAST());
  }

  // We need to traverse the clause looking for eligible measures which can be
  // wrapped inside aggregates
  // We have to skip any columns that are already inside an aggregate UDAF
  private String resolveClause(CubeQueryContext cubeql, ASTNode clause) throws SemanticException {

    if (clause == null) {
      return null;
    }

    for (int i = 0; i < clause.getChildCount(); i++) {
      transform(cubeql, clause, (ASTNode) clause.getChild(i), i);
    }

    return HQLParser.getString(clause);
  }

  private void transform(CubeQueryContext cubeql, ASTNode parent, ASTNode node, int nodePos) throws SemanticException {
    if (parent == null || node == null) {
      return;
    }
    int nodeType = node.getToken().getType();

    if (!(HQLParser.isAggregateAST(node))) {
      if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
        // Leaf node
        ASTNode wrapped = wrapAggregate(cubeql, node);
        if (wrapped != node) {
          parent.setChild(nodePos, wrapped);
          // Check if this node has an alias
          ASTNode sibling = HQLParser.findNodeByPath(parent, Identifier);
          String expr;
          if (sibling != null) {
            expr = HQLParser.getString(parent);
          } else {
            expr = HQLParser.getString(wrapped);
          }
          cubeql.addAggregateExpr(expr.trim());
        }
      } else {
        // Dig deeper in non-leaf nodes
        for (int i = 0; i < node.getChildCount(); i++) {
          transform(cubeql, node, (ASTNode) node.getChild(i), i);
        }
      }
    }
  }

  // Wrap an aggregate function around the node if its a measure, leave it
  // unchanged otherwise
  private ASTNode wrapAggregate(CubeQueryContext cubeql, ASTNode node) throws SemanticException {

    String tabname = null;
    String colname;

    if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
      colname = ((ASTNode) node.getChild(0)).getText();
    } else {
      // node in 'alias.column' format
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      colname = colIdent.getText();
      tabname = tabident.getText();
    }

    String msrname = StringUtils.isBlank(tabname) ? colname : tabname + "." + colname;

    if (cubeql.isCubeMeasure(msrname)) {
      CubeMeasure measure = cubeql.getCube().getMeasureByName(colname);
      String aggregateFn = measure.getAggregate();

      if (StringUtils.isBlank(aggregateFn)) {
        throw new SemanticException(ErrorMsg.NO_DEFAULT_AGGREGATE, colname);
      }
      ASTNode fnroot = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION));
      fnroot.setParent(node.getParent());

      ASTNode fnIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, aggregateFn));
      fnIdentNode.setParent(fnroot);
      fnroot.addChild(fnIdentNode);

      node.setParent(fnroot);
      fnroot.addChild(node);

      return fnroot;
    } else {
      return node;
    }
  }

  private boolean hasMeasuresNotInDefaultAggregates(CubeQueryContext cubeql, ASTNode node, String function,
      boolean aggregateResolverDisabled) {
    if (node == null) {
      return false;
    }

    if (HQLParser.isAggregateAST(node)) {
      if (node.getChild(0).getType() == HiveParser.Identifier) {
        function = BaseSemanticAnalyzer.unescapeIdentifier(node.getChild(0).getText());
      }
    } else if (isMeasure(cubeql, node)) {
      // Exit for the recursion

      String colname;
      if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
        colname = ((ASTNode) node.getChild(0)).getText();
      } else {
        // node in 'alias.column' format
        ASTNode colIdent = (ASTNode) node.getChild(1);
        colname = colIdent.getText();
      }
      CubeMeasure measure = cubeql.getCube().getMeasureByName(colname);
      if (function != null && !function.isEmpty()) {
        // Get the cube measure object and check if the passed function is the
        // default one set for this measure
        return !function.equalsIgnoreCase(measure.getAggregate());
      } else if (!aggregateResolverDisabled && measure.getAggregate() != null) {
        // not inside any aggregate, but default aggregate exists
        return false;
      }
      return true;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasMeasuresNotInDefaultAggregates(cubeql, (ASTNode) node.getChild(i), function, aggregateResolverDisabled)) {
        // Return on the first measure not inside its default aggregate
        return true;
      }
    }
    return false;
  }

  private boolean hasMeasuresInDistinctClause(CubeQueryContext cubeql, ASTNode node, boolean hasDistinct) {
    if (node == null) {
      return false;
    }

    int exprTokenType = node.getToken().getType();
    boolean isDistinct = hasDistinct;
    if (exprTokenType == HiveParser.TOK_FUNCTIONDI || exprTokenType == HiveParser.TOK_SELECTDI) {
      isDistinct = true;
    } else if (isMeasure(cubeql, node) && isDistinct) {
      // Exit for the recursion
      return true;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasMeasuresInDistinctClause(cubeql, (ASTNode) node.getChild(i), isDistinct)) {
        // Return on the first measure in distinct clause
        return true;
      }
    }
    return false;
  }

  private boolean hasMeasures(CubeQueryContext cubeql, ASTNode node) {
    if (node == null) {
      return false;
    }

    if (isMeasure(cubeql, node)) {
      return true;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasMeasures(cubeql, (ASTNode) node.getChild(i))) {
        return true;
      }
    }

    return false;
  }

  private boolean isMeasure(CubeQueryContext cubeql, ASTNode node) {
    String tabname = null;
    String colname;
    int nodeType = node.getToken().getType();
    if (!(nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT)) {
      return false;
    }

    if (nodeType == HiveParser.TOK_TABLE_OR_COL) {
      colname = ((ASTNode) node.getChild(0)).getText();
    } else {
      // node in 'alias.column' format
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      colname = colIdent.getText();
      tabname = tabident.getText();
    }

    String msrname = StringUtils.isBlank(tabname) ? colname : tabname + "." + colname;

    return cubeql.isCubeMeasure(msrname);
  }

  static void updateAggregates(ASTNode root, CubeQueryContext cubeql) {
    if (root == null) {
      return;
    }

    if (HQLParser.isAggregateAST(root)) {
      cubeql.addAggregateExpr(HQLParser.getString(root).trim());
    } else {
      for (int i = 0; i < root.getChildCount(); i++) {
        ASTNode child = (ASTNode) root.getChild(i);
        updateAggregates(child, cubeql);
      }
    }
  }
}
