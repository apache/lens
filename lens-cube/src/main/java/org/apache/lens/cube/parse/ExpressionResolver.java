/*
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

import static java.util.stream.Collectors.toSet;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.*;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.ExprColumn.ExprSpec;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

/**
 * Replaces expression with its AST in all query ASTs
 */
@Slf4j
class ExpressionResolver implements ContextRewriter {

  static class ExpressionContext {
    @Getter
    private final ExprColumn exprCol;
    @Getter
    private final AbstractBaseTable srcTable;
    @Getter
    private final String srcAlias;
    @Getter
    private Set<ExprSpecContext> allExprs = new LinkedHashSet<ExprSpecContext>();
    private Set<CandidateTable> directlyAvailableIn = new HashSet<CandidateTable>();
    @Getter
    private Map<CandidateTable, Set<ExprSpecContext>> evaluableExpressions = new HashMap<>();
    private boolean hasMeasures = false;

    boolean hasMeasures() {
      return hasMeasures;
    }

    ExpressionContext(CubeQueryContext cubeql, ExprColumn exprCol, AbstractBaseTable srcTable, String srcAlias)
      throws LensException {
      this.srcTable = srcTable;
      this.exprCol = exprCol;
      this.srcAlias = srcAlias;
      for (ExprSpec es : exprCol.getExpressionSpecs()) {
        allExprs.add(new ExprSpecContext(es, cubeql));
      }
      resolveColumnsAndAlias(cubeql);
      log.debug("All exprs for {} are {}", exprCol.getName(), allExprs);
    }

    private void resolveColumnsAndAlias(CubeQueryContext cubeql) throws LensException {
      for (ExprSpecContext esc : allExprs) {
        esc.resolveColumns(cubeql);
        esc.replaceAliasInAST(cubeql);
        for (String table : esc.getTblAliasToColumns().keySet()) {
          if (!CubeQueryContext.DEFAULT_TABLE.equalsIgnoreCase(table) && !srcAlias.equals(table)) {
            cubeql.addOptionalDimTable(table, null, false, null, false,
                esc.getTblAliasToColumns().get(table).toArray(new String[0]));
            esc.exprDims.add((Dimension) cubeql.getCubeTableForAlias(table));
          }
        }
      }
      resolveColumnsAndReplaceAlias(cubeql, allExprs);
    }

    private void resolveColumnsAndReplaceAlias(CubeQueryContext cubeql, Set<ExprSpecContext> exprs)
      throws LensException {
      Set<ExprSpecContext> nestedExpressions = new LinkedHashSet<ExprSpecContext>();
      for (ExprSpecContext esc : exprs) {
        for (Map.Entry<String, Set<String>> entry : esc.getTblAliasToColumns().entrySet()) {
          if (entry.getKey().equals(CubeQueryContext.DEFAULT_TABLE)) {
            continue;
          }
          AbstractBaseTable baseTable = (AbstractBaseTable) cubeql.getCubeTableForAlias(entry.getKey());
          Set<String> exprCols = new HashSet<String>();
          for (String col : entry.getValue()) {
            // col is an expression
            if (baseTable.getExpressionNames().contains(col)) {
              exprCols.add(col);
            }
          }
          // get all combinations of expression replaced with inner exprs AST.
          addAllNestedExpressions(cubeql, esc, baseTable, nestedExpressions, exprCols);
        }
      }
      for (ExprSpecContext esc : nestedExpressions) {
        esc.resolveColumns(cubeql);
        esc.replaceAliasInAST(cubeql);
        for (String table : esc.getTblAliasToColumns().keySet()) {
          if (!CubeQueryContext.DEFAULT_TABLE.equalsIgnoreCase(table) && !srcAlias.equals(table)) {
            cubeql.addOptionalDimTable(table, null, false, null, false,
                esc.getTblAliasToColumns().get(table).toArray(new String[0]));
            esc.exprDims.add((Dimension) cubeql.getCubeTableForAlias(table));
          }
        }
      }
      exprs.addAll(nestedExpressions);
    }

    private void addAllNestedExpressions(CubeQueryContext cubeql, ExprSpecContext baseEsc, AbstractBaseTable baseTable,
      Set<ExprSpecContext> nestedExpressions, Set<String> exprCols) throws LensException {
      for (String col : exprCols) {
        Set<ExprSpecContext> replacedExpressions = new LinkedHashSet<ExprSpecContext>();
        for (ExprSpec es : baseTable.getExpressionByName(col).getExpressionSpecs()) {
          ASTNode finalAST = MetastoreUtil.copyAST(baseEsc.getFinalAST());
          replaceColumnInAST(finalAST, col, es.copyASTNode());
          ExprSpecContext replacedESC = new ExprSpecContext(baseEsc, es, finalAST, cubeql);
          nestedExpressions.add(replacedESC);
          replacedExpressions.add(replacedESC);
        }
        Set<String> remaining = new LinkedHashSet<String>(exprCols);
        remaining.remove(col);
        for (ExprSpecContext replacedESC : replacedExpressions) {
          addAllNestedExpressions(cubeql, replacedESC, baseTable, nestedExpressions, remaining);
        }
      }
    }

    void addDirectlyAvailable(CandidateTable cTable) {
      log.debug("Directly available in {}", cTable);
      directlyAvailableIn.add(cTable);
    }

    void addEvaluable(CubeQueryContext cubeql, CandidateTable cTable, ExprSpecContext esc) throws LensException {
      // add optional dimensions involved in expressions
      for (String table : esc.getTblAliasToColumns().keySet()) {
        if (!CubeQueryContext.DEFAULT_TABLE.equalsIgnoreCase(table) && !srcAlias.equals(table)) {
          cubeql.addOptionalExprDimTable(table, exprCol.getName(), srcAlias, cTable,
              esc.getTblAliasToColumns().get(table).toArray(new String[0]));
          esc.exprDims.add((Dimension) cubeql.getCubeTableForAlias(table));
        }
      }
      evaluableExpressions.computeIfAbsent(cTable, k -> new LinkedHashSet<>()).add(esc);
    }

    Set<ASTNode> getAllASTNodes() {
      Set<ASTNode> allAST = new HashSet<ASTNode>();
      for (ExprSpecContext esc : allExprs) {
        allAST.add(esc.finalAST);
      }
      return allAST;
    }

    boolean hasAggregates() {
      for (ExprSpecContext esc : allExprs) {
        if (HQLParser.hasAggregate(esc.finalAST)) {
          return true;
        }
      }
      return false;
    }

    boolean isEvaluable(CandidateTable cTable) {
      return directlyAvailableIn.contains(cTable)
        || (evaluableExpressions.get(cTable) != null && !evaluableExpressions.get(cTable).isEmpty());
    }
  }

  static class ExprSpecContext extends TracksQueriedColumns implements TrackDenormContext {
    private Set<ExprSpec> exprSpecs = new LinkedHashSet<>();
    @Getter
    @Setter
    private ASTNode finalAST;
    @Getter
    private Set<Dimension> exprDims = new HashSet<>();
    @Getter
    @Setter
    private DenormalizationResolver.DenormalizationContext deNormCtx;

    ExprSpecContext(ExprSpec exprSpec, CubeQueryContext cubeql) throws LensException {
      // replaces table names in expression with aliases in the query
      finalAST = replaceAlias(exprSpec.copyASTNode(), cubeql);
      exprSpecs.add(exprSpec);
    }
    ExprSpecContext(ExprSpecContext nested, ExprSpec current, ASTNode node,
      CubeQueryContext cubeql) throws LensException {
      exprSpecs.addAll(nested.exprSpecs);
      exprSpecs.add(current);
      finalAST = replaceAlias(node, cubeql);
    }
    void replaceAliasInAST(CubeQueryContext cubeql)
      throws LensException {
      AliasReplacer.extractTabAliasForCol(cubeql, this);
      finalAST = AliasReplacer.replaceAliases(finalAST, 0, cubeql.getColToTableAlias());
    }


    void resolveColumns(CubeQueryContext cubeql) throws LensException {
      // finds all columns and table aliases in the expression
      ColumnResolver.getColsForTree(cubeql, finalAST, this, false);
    }

    Date getStartTime() {
      Set<Date> startTimes = new HashSet<>();
      for (ExprSpec es : exprSpecs) {
        if (es.getStartTime() != null) {
          startTimes.add(es.getStartTime());
        }
      }
      if (!startTimes.isEmpty()) {
        return Collections.max(startTimes);
      }
      return null;
    }

    Date getEndTime() {
      Set<Date> endTimes = new HashSet<>();
      for (ExprSpec es : exprSpecs) {
        if (es.getEndTime() != null) {
          endTimes.add(es.getEndTime());
        }
      }
      if (!endTimes.isEmpty()) {
        return Collections.min(endTimes);
      }
      return null;
    }

    boolean isValidInTimeRange(final TimeRange range) {
      return isValidFrom(range.getFromDate()) && isValidTill(range.getToDate());
    }

    boolean isValidFrom(@NonNull final Date date) {
      return (getStartTime() == null) || (date.equals(getStartTime()) || date.after(getStartTime()));
    }

    boolean isValidTill(@NonNull final Date date) {
      return (getEndTime() == null) || (date.equals(getEndTime()) || date.before(getEndTime()));
    }

    public String toString() {
      return HQLParser.getString(finalAST);
    }

  }

  @RequiredArgsConstructor
  @ToString
  static class PickedExpression {
    private final String srcAlias;
    private final ExprSpecContext pickedCtx;
    private transient ASTNode reWrittenAST = null;

    /*
    Initialized rewrittenAST as copy of final AST if boolean is passed. Copy would be required if finalAST gets
    modified because of denormalization context.
    Otherwise, it is final AST reference, without any copy.
     */
    void initRewrittenAST(boolean copyFinal) {
      if (copyFinal) {
        reWrittenAST = MetastoreUtil.copyAST(pickedCtx.getFinalAST());
      } else {
        reWrittenAST = pickedCtx.getFinalAST();
      }
    }
    ASTNode getRewrittenAST() {
      return reWrittenAST;
    }
  }

  static class ExpressionResolverContext {
    @Getter
    private Map<String, Set<ExpressionContext>> allExprsQueried = new HashMap<>();
    private Map<String, Set<PickedExpression>> pickedExpressions = new HashMap<>();
    @Getter
    private Map<DimHQLContext, Map<String, Set<PickedExpression>>> pickedExpressionsPerCandidate = new HashMap<>();
    private Map<String, ASTNode> nonPickedExpressionsForCandidate = new HashMap<String, ASTNode>();
    private final CubeQueryContext cubeql;
    private boolean replacedHavingExpressions = false;
    ExpressionResolverContext(CubeQueryContext cubeql) {
      this.cubeql = cubeql;
    }
    void addExpressionQueried(ExpressionContext expr) {
      allExprsQueried.computeIfAbsent(expr.getExprCol().getName().toLowerCase(), k -> new LinkedHashSet<>()).add(expr);
    }

    boolean isQueriedExpression(String column) {
      return allExprsQueried.containsKey(column);
    }

    boolean hasAggregates() {
      for (Set<ExpressionContext> ecSet : allExprsQueried.values()) {
        for (ExpressionContext ec : ecSet) {
          if (ec.hasAggregates()) {
            return true;
          }
        }
      }
      return false;
    }

    ExpressionContext getExpressionContext(String expr, String alias) {
      if (allExprsQueried.get(expr) != null) {
        for (ExpressionContext ec : allExprsQueried.get(expr)) {
          if (ec.getSrcAlias().equals(alias)) {
            return ec;
          }
        }
      }
      return null;
    }

    boolean hasMeasures(String expr, CubeInterface cube) {
      String alias = cubeql.getAliasForTableName(cube.getName());
      ExpressionContext ec = getExpressionContext(expr, alias);
      boolean hasMeasures = false;
      for (ExprSpecContext esc : ec.allExprs) {
        if (esc.getTblAliasToColumns().get(alias) != null) {
          for (String cubeCol : esc.getTblAliasToColumns().get(alias)) {
            if (cube.getMeasureByName(cubeCol) != null) {
              hasMeasures = true;
              break;
            }
          }
        }
      }
      ec.hasMeasures = hasMeasures;
      return hasMeasures;
    }

    //updates all expression specs which are evaluable
    void updateEvaluables(String expr, CandidateTable cTable)
      throws LensException {
      String alias = cubeql.getAliasForTableName(cTable.getBaseTable().getName());
      ExpressionContext ec = getExpressionContext(expr, alias);
      if (cTable.getColumns().contains(expr)) {
        // expression is directly materialized in candidate table
        log.debug("{} is directly evaluable in {}", expr, cTable);
        ec.addDirectlyAvailable(cTable);
        return;
      }
      for (ExprSpecContext esc : ec.allExprs) {
        if (esc.getTblAliasToColumns().get(alias) == null) {
          log.debug("{} = {} is evaluable in {}", expr, esc, cTable);
          ec.addEvaluable(cubeql, cTable, esc);
        } else {
          Set<String> columns = esc.getTblAliasToColumns().get(alias);
          boolean isEvaluable = true;
          for (String col : columns) {
            if (!cTable.getColumns().contains(col.toLowerCase())) {
              if (!esc.getDeNormCtx().addRefUsage(cubeql, cTable, col, cTable.getBaseTable().getName())) {
                // check if it is available as reference, if not expression is not evaluable
                log.debug("{} = {} is not evaluable in {}", expr, esc, cTable);
                isEvaluable = false;
                break;
              }
            }
          }
          if (isEvaluable) {
            log.debug("{} = {} is evaluable in {}", expr, esc, cTable);
            ec.addEvaluable(cubeql, cTable, esc);
          }
        }
      }
    }

    // checks if expr is evaluable
    boolean isEvaluable(String expr, CandidateTable cTable) {
      ExpressionContext ec = getExpressionContext(expr, cubeql.getAliasForTableName(cTable.getBaseTable().getName()));
      return ec.isEvaluable(cTable);
    }

    Set<Dimension> rewriteExprCtx(CubeQueryContext cubeql, DimHQLContext sc, Map<Dimension, CandidateDim> dimsToQuery)
      throws LensException {
      Set<Dimension> exprDims = new HashSet<Dimension>();
      log.info("Picking expressions for candidate {} ", sc);
      if (!allExprsQueried.isEmpty()) {
        // pick expressions for fact
        if (sc.getStorageCandidate() != null) {
          pickExpressionsForTable(sc.getStorageCandidate());
        }
        // pick expressions for dimensions
        if (dimsToQuery != null && !dimsToQuery.isEmpty()) {
          for (CandidateDim cdim : dimsToQuery.values()) {
            pickExpressionsForTable(cdim);
          }
        }
        log.debug("Picked expressions: {}", pickedExpressions);
        for (Set<PickedExpression> peSet : pickedExpressions.values()) {
          for (PickedExpression pe : peSet) {
            exprDims.addAll(pe.pickedCtx.exprDims);
            pe.initRewrittenAST(pe.pickedCtx.deNormCtx.hasReferences());
            exprDims.addAll(pe.pickedCtx.deNormCtx.rewriteDenormctxInExpression(cubeql,
              sc.getStorageCandidate(), dimsToQuery, pe.getRewrittenAST()));
          }
        }
        // Replace picked expressions in all the base trees
        replacePickedExpressions(sc);
      }
      pickedExpressionsPerCandidate.put(sc, Maps.newHashMap(pickedExpressions));
      pickedExpressions.clear();
      nonPickedExpressionsForCandidate.clear();

      return exprDims;
    }

    private void replacePickedExpressions(DimHQLContext sc)
      throws LensException {
      QueryAST queryAST = sc.getQueryAst();
      replaceAST(cubeql, queryAST.getSelectAST());
      if (sc.getStorageCandidate() != null) {
        replaceAST(cubeql, sc.getQueryAst().getWhereAST());
      } else {
        replaceAST(cubeql, queryAST.getWhereAST());
      }
      replaceAST(cubeql, queryAST.getJoinAST());
      replaceAST(cubeql, queryAST.getGroupByAST());
      // Resolve having expression for StorageCandidate
      replaceAST(cubeql, queryAST.getOrderByAST());
    }
    public void replaceHavingExpressions() throws LensException {
      replaceHavingExpressions(pickedExpressionsPerCandidate);
    }
    public void replaceHavingExpressions(
      Map<DimHQLContext, Map<String, Set<PickedExpression>>> pickedExpressionsPerCandidate) throws LensException {
      if (cubeql.getHavingAST() != null && !replacedHavingExpressions) {
        HQLParser.bft(cubeql.getHavingAST(), visited -> {
          ASTNode node1 = visited.getNode();
          int childcount = node1.getChildCount();
          for (int i = 0; i < childcount; i++) {
            ASTNode current = (ASTNode) node1.getChild(i);
            if (current.getToken().getType() == DOT) {
              // This is for the case where column name is prefixed by table name
              // or table alias
              // For example 'select fact.id, dim2.id ...'
              // Right child is the column name, left child.ident is table name
              ASTNode tabident = HQLParser.findNodeByPath(current, TOK_TABLE_OR_COL, Identifier);
              ASTNode colIdent = (ASTNode) current.getChild(1);
              String column = colIdent.getText().toLowerCase();

              Optional<PickedExpression> exprOptional = pickedExpressionsPerCandidate.values().stream()
                .filter(x -> x.containsKey(column)).map(x -> x.get(column)).flatMap(Collection::stream)
                .filter(x -> x.srcAlias.equals(tabident.getText().toLowerCase())).findFirst();

              if (exprOptional.isPresent()) {
                PickedExpression expr = exprOptional.get();
                node1.setChild(i, replaceAlias(expr.getRewrittenAST(), cubeql));
              }
            }
          }
        });
        replacedHavingExpressions = true;
      }
    }
    private void replaceAST(final CubeQueryContext cubeql, ASTNode node) throws LensException {
      if (node == null) {
        return;
      }
      // Traverse the tree and resolve expression columns
      HQLParser.bft(node, visited -> {
        ASTNode node1 = visited.getNode();
        int childcount = node1.getChildCount();
        for (int i = 0; i < childcount; i++) {
          ASTNode current = (ASTNode) node1.getChild(i);
          if (current.getToken().getType() == DOT) {
            // This is for the case where column name is prefixed by table name
            // or table alias
            // For example 'select fact.id, dim2.id ...'
            // Right child is the column name, left child.ident is table name
            ASTNode tabident = HQLParser.findNodeByPath(current, TOK_TABLE_OR_COL, Identifier);
            ASTNode colIdent = (ASTNode) current.getChild(1);
            String column = colIdent.getText().toLowerCase();

            if (pickedExpressions.containsKey(column)) {
              assert tabident != null;
              PickedExpression expr = getPickedExpression(column, tabident.getText().toLowerCase());
              if (expr != null) {
                node1.setChild(i, replaceAlias(expr.getRewrittenAST(), cubeql));
              }
            } else if (nonPickedExpressionsForCandidate.containsKey(column)) {
              node1.setChild(i, nonPickedExpressionsForCandidate.get(column));

            }
          }
        }
      });
    }

    private PickedExpression getPickedExpression(String column, String alias) {
      Set<PickedExpression> peSet = pickedExpressions.get(column);
      if (peSet != null && !peSet.isEmpty()) {
        for (PickedExpression picked : peSet) {
          if (picked.srcAlias.equals(alias)) {
            return picked;
          }
        }
      }
      return null;
    }

    private void pickExpressionsForTable(CandidateTable cTable) throws LensException {
      for (Map.Entry<String, Set<ExpressionContext>> ecEntry : allExprsQueried.entrySet()) {
        Set<ExpressionContext> ecSet = ecEntry.getValue();
        for (ExpressionContext ec : ecSet) {
          if (ec.getSrcTable().getName().equals(cTable.getBaseTable().getName())) {
            if (!ec.directlyAvailableIn.contains(cTable)) {
              log.debug("{} is not directly evaluable in {}", ec, cTable);
              if (ec.evaluableExpressions.get(cTable) != null && !ec.evaluableExpressions.get(cTable).isEmpty()) {
                // pick first evaluable expression
                pickedExpressions.computeIfAbsent(ecEntry.getKey(), k -> new HashSet<>())
                  .add(new PickedExpression(ec.srcAlias, ec.evaluableExpressions.get(cTable).iterator().next()));
              } else {
                nonPickedExpressionsForCandidate.put(ecEntry.getKey(), getDefaultExpr(getExprAst(ec)));
              }
            }
          }
        }
      }
    }

    private ASTNode getExprAst(ExpressionContext expressionContext) {
      Set<StorageCandidate> scSet = CandidateUtil.getStorageCandidates(cubeql.getCandidates());
      Set<String> storageTableNames = new HashSet<>();
      Set<ExpressionContext> expressionContexts = Sets.newLinkedHashSet();
      expressionContexts.add(expressionContext);
      for (StorageCandidate sc : scSet) {
        storageTableNames.add(sc.getStorageTable());
        ExpressionContext ex = sc.getCubeQueryContext().getExprCtx().getExpressionContext(expressionContext.
            getExprCol().getName(), expressionContext.getSrcAlias());
        if (ex != null) {
          expressionContexts.add(ex);
        }
      }
      for (ExpressionContext ec : expressionContexts) {
        for (CandidateTable table : ec.evaluableExpressions.keySet()) {
          if (storageTableNames.contains(table.getStorageTable())) {
            return MetastoreUtil.copyAST(ec.evaluableExpressions.get(table).iterator().next().finalAST);
          }
        }
      }
      return null;
    }

    private  ASTNode getDefaultExpr(ASTNode node) {
      if (HQLParser.isAggregateAST(node)) {
        node.setChild(1, new ASTNode(new CommonToken(HiveParser.Identifier, "0.0")));
      }
      for (int i = 0; i < node.getChildCount(); i++) {
        ASTNode child = (ASTNode) node.getChild(i);
        getDefaultExpr(child);
      }
      return node;
    }

    void pruneExpressions() {
      for (Set<ExpressionContext> ecSet : allExprsQueried.values()) {
        for (ExpressionContext ec : ecSet) {
          Set<ExprSpecContext> removedEsc = new HashSet<ExprSpecContext>();
          for(Iterator<ExprSpecContext> iterator = ec.getAllExprs().iterator(); iterator.hasNext();) {
            ExprSpecContext esc = iterator.next();
            boolean removed = false;
            // Go over expression dims and remove expression involving dimensions for which candidate tables are
            // not there
            for (Dimension exprDim : esc.exprDims) {
              if (cubeql.getCandidateDims().get(exprDim) == null || cubeql.getCandidateDims().get(exprDim).isEmpty()) {
                log.info("Removing expression {} as {} it does not have any candidate tables", esc, exprDim);
                iterator.remove();
                removedEsc.add(esc);
                removed = true;
                break;
              }
            }
            if (removed) {
              continue;
            }
            // Remove expressions for which denormalized columns are no more reachable
            esc.getDeNormCtx().pruneReferences(cubeql);
            if (!esc.getDeNormCtx().getTableToRefCols().isEmpty()
              && esc.getDeNormCtx().getTableToRefCols().keySet().containsAll(
                ec.getEvaluableExpressions().keySet().stream().map(Named::getName).collect(toSet()))
              && esc.getDeNormCtx().getTableToRefCols().keySet().stream()
              .map(esc.getDeNormCtx()::getNonReachableReferenceFields).noneMatch(Set::isEmpty)) {
              log.info("Removing expression {} as all tables have non reachable fields", esc);
              iterator.remove();
              removedEsc.add(esc);
              removed = true;
            }
            if (removed) {
              continue;
            }
            //remove expressions which are not valid in the timerange queried
            // If an expression is defined as
            // ex = a + b // from t1 to t2;
            // ex = c + d // from t2 to t3
            // With range queried, invalid expressions will be removed
            // If range is including more than one expression, queries can be unioned as an improvement at later time.
            // But for now, they are not eligible expressions
            for (TimeRange range : cubeql.getTimeRanges()) {
              if (!esc.isValidInTimeRange(range)) {
                log.info("Removing expression {} as it is not valid in timerange queried", esc);
                iterator.remove();
                removedEsc.add(esc);
                removed = true;
                break;
              }
            }
            if (removed) {
              continue;
            }
            // Go over expressions and remove expression containing unavailable columns in timerange
            // In the example above,
            // if ex = a +b ; and a is not available in timerange queried, it will be removed.
            for (TimeRange range : cubeql.getTimeRanges()) {
              boolean toRemove = false;
              for (Map.Entry<String, Set<String>> entry : esc.getTblAliasToColumns().entrySet()) {
                if (CubeQueryContext.DEFAULT_TABLE.equalsIgnoreCase(entry.getKey())) {
                  continue;
                }
                AbstractBaseTable baseTable = (AbstractBaseTable) cubeql.getCubeTableForAlias(entry.getKey());
                for (String col : entry.getValue()) {
                  if (!baseTable.getColumnByName(col).isColumnAvailableInTimeRange(range)) {
                    toRemove = true;
                    break;
                  }
                }
                if (toRemove) {
                  break;
                }
              }
              if (toRemove) {
                log.info("Removing expression {} as its columns are unavailable in timerange queried", esc);
                iterator.remove();
                removedEsc.add(esc);
                removed = true;
                break;
              }
            }
          }
          for (Set<ExprSpecContext> evalSet : ec.evaluableExpressions.values()) {
            evalSet.removeAll(removedEsc);
          }
        }
      }
    }
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    ExpressionResolverContext exprCtx = cubeql.getExprCtx();
    if (exprCtx == null) {
      exprCtx = new ExpressionResolverContext(cubeql);
      cubeql.setExprCtx(exprCtx);
      for (Map.Entry<String, Set<String>> entry : cubeql.getTblAliasToColumns().entrySet()) {
        String alias = entry.getKey();
        // skip default alias
        if (Objects.equals(alias, CubeQueryContext.DEFAULT_TABLE)) {
          continue;
        }
        AbstractCubeTable tbl = cubeql.getCubeTableForAlias(alias);
        Set<String> columns = entry.getValue();
        for (String column : columns) {
          CubeColumn col;
          if (tbl instanceof CubeInterface) {
            col = ((CubeInterface) tbl).getColumnByName(column);
          } else {
            col = ((Dimension) tbl).getColumnByName(column);
          }
          if (col instanceof ExprColumn) {
            exprCtx.addExpressionQueried(new ExpressionContext(cubeql, (ExprColumn)col, (AbstractBaseTable)tbl, alias));
          }
        }
      }
      Set<String> exprsWithMeasures = new HashSet<String>();
      for (String expr : cubeql.getQueriedExprs()) {
        if (cubeql.getExprCtx().hasMeasures(expr, cubeql.getCube())) {
          // expression has measures
          exprsWithMeasures.add(expr);
        }
      }
      cubeql.addQueriedExprsWithMeasures(exprsWithMeasures);

    } else {
      // prune invalid expressions
      cubeql.getExprCtx().pruneExpressions();
      // prune candidate facts without any valid expressions
      if (cubeql.getCube() != null && !cubeql.getCandidates().isEmpty()) {
        for (Map.Entry<String, Set<ExpressionContext>> ecEntry : exprCtx.allExprsQueried.entrySet()) {
          String expr = ecEntry.getKey();
          Set<ExpressionContext> ecSet = ecEntry.getValue();
          cubeql.getCandidates().removeIf(x-> {
            if (ecSet.stream().noneMatch(x::isExpressionEvaluable)) {
              log.info("Not considering Candidate :{} as {} is not evaluable", x, expr);
              cubeql.addCandidatePruningMsg(x,
                CandidateTablePruneCause.expressionNotEvaluable(expr));
              return true;
            }
            return false;
          });
        }
      }
      // prune candidate dims without any valid expressions
      if (cubeql.getDimensions() != null && !cubeql.getDimensions().isEmpty()) {
        for (Dimension dim : cubeql.getDimensions()) {
          for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator(); i.hasNext();) {
            CandidateDim cdim = i.next();
            for (Map.Entry<String, Set<ExpressionContext>> ecEntry : exprCtx.allExprsQueried.entrySet()) {
              Set<ExpressionContext> ecSet = ecEntry.getValue();
              for (ExpressionContext ec : ecSet) {
                if (ec.getSrcTable().getName().equals(cdim.getBaseTable().getName())) {
                  if (!ec.isEvaluable(cdim)) {
                    log.info("Not considering dim table:{} as {} is not evaluable", cdim, ec.exprCol.getName());
                    cubeql.addDimPruningMsgs(dim, cdim.dimtable,
                      CandidateTablePruneCause.expressionNotEvaluable(ec.exprCol.getName()));
                    i.remove();
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private static ASTNode replaceAlias(final ASTNode expr, final CubeQueryContext cubeql) throws LensException {
    final ASTNode finalAST = MetastoreUtil.copyAST(expr);
    HQLParser.bft(finalAST, visited -> {
      ASTNode node = visited.getNode();
      ASTNode parent = null;
      if (visited.getParent() != null) {
        parent = visited.getParent().getNode();
      }

      if (node.getToken().getType() == TOK_TABLE_OR_COL && (parent != null && parent.getToken().getType() == DOT)) {
        ASTNode current = (ASTNode) node.getChild(0);
        if (current.getToken().getType() == Identifier) {
          String tableName = current.getToken().getText().toLowerCase();
          String alias = cubeql.getAliasForTableName(tableName);
          if (!alias.equalsIgnoreCase(tableName)) {
            node.setChild(0, new ASTNode(new CommonToken(HiveParser.Identifier, alias)));
          }
        }
      }
    });
    return finalAST;
  }

  private static void replaceColumnInAST(ASTNode expr, final String toReplace, final ASTNode columnAST)
    throws LensException {
    if (expr == null) {
      return;
    }
    // Traverse the tree and resolve expression columns
    HQLParser.bft(expr, visited -> {
      ASTNode node = visited.getNode();
      int childcount = node.getChildCount();
      for (int i = 0; i < childcount; i++) {
        ASTNode current = (ASTNode) node.getChild(i);
        if (current.getToken().getType() == TOK_TABLE_OR_COL && node.getToken().getType() != DOT) {
          // Take child ident.totext
          ASTNode ident = (ASTNode) current.getChild(0);
          String column = ident.getText().toLowerCase();
          if (toReplace.equals(column)) {
            node.setChild(i, MetastoreUtil.copyAST(columnAST));
          }
        } else if (current.getToken().getType() == DOT) {
          // This is for the case where column name is prefixed by table name
          // or table alias
          // For example 'select fact.id, dim2.id ...'
          // Right child is the column name, left child.ident is table name
          ASTNode tabident = HQLParser.findNodeByPath(current, TOK_TABLE_OR_COL, Identifier);
          ASTNode colIdent = (ASTNode) current.getChild(1);

          String column = colIdent.getText().toLowerCase();

          if (toReplace.equals(column)) {
            node.setChild(i, MetastoreUtil.copyAST(columnAST));
          }
        }
      }
    });
  }
}
