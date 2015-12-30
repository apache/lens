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

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.CubeMeasure;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

import lombok.extern.slf4j.Slf4j;

/**
 * <p> Replace select and having columns with default aggregate functions on them, if default aggregate is defined and
 * if there isn't already an aggregate function specified on the columns. </p> <p/> <p> Expressions which already
 * contain aggregate sub-expressions will not be changed. </p> <p/> <p> At this point it's assumed that aliases have
 * been added to all columns. </p>
 */
@Slf4j
class AggregateResolver implements ContextRewriter {

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() == null) {
      return;
    }

    boolean nonDefaultAggregates = false;
    boolean aggregateResolverDisabled = cubeql.getConf().getBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER,
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
      Iterator<CandidateFact> factItr = cubeql.getCandidateFacts().iterator();
      while (factItr.hasNext()) {
        CandidateFact candidate = factItr.next();
        if (candidate.fact.isAggregated()) {
          cubeql.addFactPruningMsgs(candidate.fact,
            CandidateTablePruneCause.missingDefaultAggregate());
          factItr.remove();
        }
      }
      nonDefaultAggregates = true;
      log.info("Query has non default aggregates, no aggregate resolution will be done");
    }

    cubeql.pruneCandidateFactSet(CandidateTablePruneCode.MISSING_DEFAULT_AGGREGATE);

    if (nonDefaultAggregates || aggregateResolverDisabled) {
      return;
    }

    resolveClause(cubeql, cubeql.getSelectAST());

    resolveClause(cubeql, cubeql.getHavingAST());

    Configuration distConf = cubeql.getConf();
    boolean isDimOnlyDistinctEnabled = distConf.getBoolean(CubeQueryConfUtil.ENABLE_ATTRFIELDS_ADD_DISTINCT,
      CubeQueryConfUtil.DEFAULT_ATTR_FIELDS_ADD_DISTINCT);
    //Having clause will always work with measures, if only keys projected
    //query should skip distinct and promote group by.
    if (cubeql.getHavingAST() == null && isDimOnlyDistinctEnabled) {
      // Check if any measure/aggregate columns and distinct clause used in
      // select tree. If not, update selectAST token "SELECT" to "SELECT DISTINCT"
      if (!hasMeasures(cubeql, cubeql.getSelectAST()) && !isDistinctClauseUsed(cubeql.getSelectAST())
        && !HQLParser.hasAggregate(cubeql.getSelectAST())) {
        cubeql.getSelectAST().getToken().setType(HiveParser.TOK_SELECTDI);
      }
    }
  }

  // We need to traverse the clause looking for eligible measures which can be
  // wrapped inside aggregates
  // We have to skip any columns that are already inside an aggregate UDAF
  private String resolveClause(CubeQueryContext cubeql, ASTNode clause) throws LensException {

    if (clause == null) {
      return null;
    }

    for (int i = 0; i < clause.getChildCount(); i++) {
      transform(cubeql, clause, (ASTNode) clause.getChild(i), i);
    }

    return HQLParser.getString(clause);
  }

  private void transform(CubeQueryContext cubeql, ASTNode parent, ASTNode node, int nodePos) throws LensException {
    if (node == null) {
      return;
    }
    int nodeType = node.getToken().getType();

    if (!(HQLParser.isAggregateAST(node))) {
      if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
        // Leaf node
        ASTNode wrapped = wrapAggregate(cubeql, node);
        if (wrapped != node) {
          if (parent != null) {
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
  private ASTNode wrapAggregate(CubeQueryContext cubeql, ASTNode node) throws LensException {

    String tabname = null;
    String colname;

    if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
      colname = node.getChild(0).getText();
    } else {
      // node in 'alias.column' format
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      colname = colIdent.getText();
      tabname = tabident.getText();
    }

    String msrname = StringUtils.isBlank(tabname) ? colname : tabname + "." + colname;

    if (cubeql.isCubeMeasure(msrname)) {
      if (cubeql.getQueriedExprs().contains(colname)) {
        String alias = cubeql.getAliasForTableName(cubeql.getCube().getName());
        for (ASTNode exprNode : cubeql.getExprCtx().getExpressionContext(colname, alias).getAllASTNodes()) {
          transform(cubeql, null, exprNode, 0);
        }
        return node;
      } else {
        CubeMeasure measure = cubeql.getCube().getMeasureByName(colname);
        String aggregateFn = measure.getAggregate();

        if (StringUtils.isBlank(aggregateFn)) {
          throw new LensException(LensCubeErrorCode.NO_DEFAULT_AGGREGATE.getLensErrorInfo(), colname);
        }
        ASTNode fnroot = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION));
        ASTNode fnIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, aggregateFn));
        fnroot.addChild(fnIdentNode);
        fnroot.addChild(node);
        return fnroot;
      }
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
    } else if (cubeql.isCubeMeasure(node)) {
      // Exit for the recursion

      String colname;
      if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
        colname = node.getChild(0).getText();
      } else {
        // node in 'alias.column' format
        ASTNode colIdent = (ASTNode) node.getChild(1);
        colname = colIdent.getText();
      }
      colname = colname.toLowerCase();
      if (cubeql.getQueriedExprs().contains(colname)) {
        String cubeAlias = cubeql.getAliasForTableName(cubeql.getCube().getName());
        for (ASTNode exprNode : cubeql.getExprCtx().getExpressionContext(colname, cubeAlias).getAllASTNodes()) {
          if (hasMeasuresNotInDefaultAggregates(cubeql, exprNode, function, aggregateResolverDisabled)) {
            return true;
          }
        }
        return false;
      } else {
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
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasMeasuresNotInDefaultAggregates(cubeql, (ASTNode) node.getChild(i), function, aggregateResolverDisabled)) {
        // Return on the first measure not inside its default aggregate
        return true;
      }
    }
    return false;
  }

  /*
   * Check if distinct keyword used in node
   */
  private boolean isDistinctClauseUsed(ASTNode node) {
    if (node == null) {
      return false;
    }
    if (node.getToken() != null) {
      if (node.getToken().getType() == HiveParser.TOK_FUNCTIONDI
        || node.getToken().getType() == HiveParser.TOK_SELECTDI) {
        return true;
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (isDistinctClauseUsed((ASTNode) node.getChild(i))) {
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
    } else if (cubeql.isCubeMeasure(node) && isDistinct) {
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

    if (cubeql.isCubeMeasure(node)) {
      return true;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasMeasures(cubeql, (ASTNode) node.getChild(i))) {
        return true;
      }
    }

    return false;
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
