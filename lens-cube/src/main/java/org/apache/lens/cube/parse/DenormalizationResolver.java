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

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.ReferencedDimAttribute.ChainRefCol;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.cube.parse.ExpressionResolver.ExprSpecContext;
import org.apache.lens.cube.parse.ExpressionResolver.ExpressionContext;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * This class resolves all the reference columns that are queried.
 * <p></p>
 * Keeps track of the context that if any candidate needs to use columns through tables referenced and replaces the
 * columns from referenced tables in all the ASTs
 */
@Slf4j
public class DenormalizationResolver implements ContextRewriter {

  public DenormalizationResolver(Configuration conf) {
  }

  @ToString
  public static class ReferencedQueriedColumn {
    ReferencedDimAttribute col;
    AbstractCubeTable srcTable;
    transient List<ChainRefCol> chainRefCols = new ArrayList<>();

    ReferencedQueriedColumn(ReferencedDimAttribute col, AbstractCubeTable srcTable) {
      this.col = col;
      this.srcTable = srcTable;
      chainRefCols.addAll(col.getChainRefColumns());
    }
  }

  @ToString
  public static class PickedReference {
    @Getter
    ChainRefCol chainRef;
    String srcAlias;
    String pickedFor;

    PickedReference(String srcAlias, String pickedFor) {
      this.srcAlias = srcAlias;
      this.pickedFor = pickedFor;
    }

    PickedReference(ChainRefCol chainRef, String srcAlias, String pickedFor) {
      this.srcAlias = srcAlias;
      this.chainRef = chainRef;
      this.pickedFor = pickedFor;
    }
  }

  public static class DenormalizationContext {
    // map of column name to all references
    @Getter
    private Map<String, Set<ReferencedQueriedColumn>> referencedCols = new HashMap<>();

    // candidate table name to all the references columns it needs
    @Getter
    private Map<String, Set<ReferencedQueriedColumn>> tableToRefCols = new HashMap<>();

    // set of all picked references once all candidate tables are picked
    private Set<PickedReference> pickedRefs = new HashSet<>();
    // index on column name for picked references with map from column name to
    // pickedrefs
    private Map<String, Set<PickedReference>> pickedReferences = new HashMap<>();

    void addReferencedCol(String col, ReferencedQueriedColumn refer) {
      Set<ReferencedQueriedColumn> refCols = referencedCols.get(col);
      if (refCols == null) {
        refCols = new HashSet<>();
        referencedCols.put(col, refCols);
      }
      refCols.add(refer);
    }

    // When candidate table does not have the field, this method checks
    // if the field can be reached through reference,
    // if yes adds the ref usage and returns to true, if not returns false.
    boolean addRefUsage(CubeQueryContext cubeql, CandidateTable table, String col, String srcTbl) throws LensException {
      // available as referenced col
      if (referencedCols.containsKey(col)) {
        for (ReferencedQueriedColumn refer : referencedCols.get(col)) {
          if (refer.srcTable.getName().equalsIgnoreCase(srcTbl)) {
            // check if reference source column is available in src table?
            // should not be required here. Join resolution will figure out if
            // there is no path
            // to the source table
            log.info("Adding denormalized column for column:{} for table:{}", col, table);
            Set<ReferencedQueriedColumn> refCols = tableToRefCols.get(table.getName());
            if (refCols == null) {
              refCols = new HashSet<>();
              tableToRefCols.put(table.getName(), refCols);
            }
            refCols.add(refer);
            // Add to optional tables
            for (ChainRefCol refCol : refer.col.getChainRefColumns()) {
              cubeql.addOptionalDimTable(refCol.getChainName(), table, false, refer.col.getName(), true,
                refCol.getRefColumn());
            }
            return true;
          }
        }
      }
      return false;
    }

    private void addPickedReference(String col, PickedReference refer) {
      Set<PickedReference> refCols = pickedReferences.get(col);
      if (refCols == null) {
        refCols = new HashSet<>();
        pickedReferences.put(col, refCols);
      }
      refCols.add(refer);
    }

    private PickedReference getPickedReference(String col, String srcAlias) {
      if (pickedReferences.containsKey(col)) {
        for (PickedReference ref : pickedReferences.get(col)) {
          if (ref.srcAlias.equalsIgnoreCase(srcAlias)) {
            log.info("Picked reference for {} ref:{}", col, pickedReferences.get(col));
            return ref;
          }
        }
      }
      return null;
    }

    public Set<Dimension> rewriteDenormctx(CubeQueryContext cubeql, CandidateFact cfact, Map<Dimension,
      CandidateDim> dimsToQuery, boolean replaceFact) throws LensException {
      Set<Dimension> refTbls = new HashSet<>();
      log.info("Doing denorm changes for fact :{}", cfact);

      if (!tableToRefCols.isEmpty()) {
        // pick referenced columns for fact
        if (cfact != null) {
          pickColumnsForTable(cubeql, cfact.getName());
        }
        // pick referenced columns for dimensions
        if (dimsToQuery != null && !dimsToQuery.isEmpty()) {
          for (CandidateDim cdim : dimsToQuery.values()) {
            pickColumnsForTable(cubeql, cdim.getName());
          }
        }
        // Replace picked reference in all the base trees
        replaceReferencedColumns(cubeql, cfact, replaceFact);

        // Add the picked references to dimsToQuery
        for (PickedReference picked : pickedRefs) {
          if (isPickedFor(picked, cfact, dimsToQuery)) {
            refTbls.add((Dimension) cubeql.getCubeTableForAlias(picked.getChainRef().getChainName()));
            cubeql.addColumnsQueried(picked.getChainRef().getChainName(), picked.getChainRef().getRefColumn());
          }
        }
      }
      pickedReferences.clear();
      pickedRefs.clear();
      return refTbls;
    }

    public boolean hasReferences() {
      return !tableToRefCols.isEmpty();
    }
    public Set<Dimension> rewriteDenormctxInExpression(CubeQueryContext cubeql, CandidateFact cfact, Map<Dimension,
      CandidateDim> dimsToQuery, ASTNode exprAST) throws LensException {
      Set<Dimension> refTbls = new HashSet<>();

      log.info("Doing denorm changes for expressions in fact :{}", cfact);
      if (!tableToRefCols.isEmpty()) {
        // pick referenced columns for fact
        if (cfact != null) {
          pickColumnsForTable(cubeql, cfact.getName());
        }
        // pick referenced columns for dimensions
        if (dimsToQuery != null && !dimsToQuery.isEmpty()) {
          for (CandidateDim cdim : dimsToQuery.values()) {
            pickColumnsForTable(cubeql, cdim.getName());
          }
        }
        // Replace picked reference in expression ast
        resolveClause(exprAST);

        // Add the picked references to dimsToQuery
        for (PickedReference picked : pickedRefs) {
          if (isPickedFor(picked, cfact, dimsToQuery)) {
            refTbls.add((Dimension) cubeql.getCubeTableForAlias(picked.getChainRef().getChainName()));
            cubeql.addColumnsQueried(picked.getChainRef().getChainName(), picked.getChainRef().getRefColumn());
          }
        }
      }
      pickedReferences.clear();
      pickedRefs.clear();
      return refTbls;
    }
    // checks if the reference if picked for facts and dimsToQuery passed
    private boolean isPickedFor(PickedReference picked, CandidateFact cfact, Map<Dimension, CandidateDim> dimsToQuery) {
      if (cfact != null && picked.pickedFor.equalsIgnoreCase(cfact.getName())) {
        return true;
      }
      if (dimsToQuery != null) {
        for (CandidateDim cdim : dimsToQuery.values()) {
          if (picked.pickedFor.equalsIgnoreCase(cdim.getName())) {
            return true;
          }
        }
      }
      return false;
    }

    private void pickColumnsForTable(CubeQueryContext cubeql, String tbl) throws LensException {
      if (tableToRefCols.containsKey(tbl)) {
        for (ReferencedQueriedColumn refered : tableToRefCols.get(tbl)) {
          Iterator<ChainRefCol> iter = refered.chainRefCols.iterator();
          while (iter.hasNext()) {
            // remove unreachable references
            ChainRefCol reference = iter.next();
            if (!cubeql.getAutoJoinCtx().isReachableDim(
              (Dimension) cubeql.getCubeTableForAlias(reference.getChainName()), reference.getChainName())) {
              iter.remove();
            }
          }
          if (refered.chainRefCols.isEmpty()) {
            throw new LensException(LensCubeErrorCode.NO_REF_COL_AVAILABLE.getLensErrorInfo(), refered.col.getName());
          }
          PickedReference picked =
            new PickedReference(refered.chainRefCols.iterator().next(),
              cubeql.getAliasForTableName(refered.srcTable.getName()), tbl);
          addPickedReference(refered.col.getName(), picked);
          pickedRefs.add(picked);
        }
      }
    }

    public void pruneReferences(CubeQueryContext cubeql) {
      for (Set<ReferencedQueriedColumn> referencedQueriedColumns : referencedCols.values()) {
        for(Iterator<ReferencedQueriedColumn> iterator = referencedQueriedColumns.iterator(); iterator.hasNext();) {
          ReferencedQueriedColumn rqc = iterator.next();
          for (Iterator<ChainRefCol> iter = rqc.chainRefCols.iterator(); iter.hasNext();) {
            // remove unreachable references
            ChainRefCol reference = iter.next();
            if (cubeql.getAutoJoinCtx() == null || !cubeql.getAutoJoinCtx().isReachableDim(
              (Dimension) cubeql.getCubeTableForAlias(reference.getChainName()), reference.getChainName())) {
              log.info("{} is not reachable", reference.getChainName());
              iter.remove();
            }
          }
          if (rqc.chainRefCols.isEmpty()) {
            log.info("The referenced column: {} is not reachable", rqc.col.getName());
            iterator.remove();
            continue;
          }
          // do column life validation
          for (TimeRange range : cubeql.getTimeRanges()) {
            if (!rqc.col.isColumnAvailableInTimeRange(range)) {
              log.info("The referenced column: {} is not in the range queried", rqc.col.getName());
              iterator.remove();
              break;
            }
          }
        }
      }
    }
    private void replaceReferencedColumns(CubeQueryContext cubeql, CandidateFact cfact, boolean replaceFact)
      throws LensException {
      QueryAST ast = cubeql;
      boolean factRefExists = cfact != null && tableToRefCols.get(cfact.getName()) != null && !tableToRefCols.get(cfact
        .getName()).isEmpty();
      if (replaceFact && factRefExists) {
        ast = cfact;
      }
      resolveClause(ast.getSelectAST());
      if (factRefExists) {
        for (ASTNode storageWhereClauseAST : cfact.getStorgeWhereClauseMap().values()) {
          resolveClause(storageWhereClauseAST);
        }
      } else {
        resolveClause(ast.getWhereAST());
      }
      resolveClause(ast.getGroupByAST());
      resolveClause(ast.getHavingAST());
      resolveClause(cubeql.getOrderByAST());
    }

    private void resolveClause(ASTNode node) throws LensException {
      if (node == null) {
        return;
      }

      int nodeType = node.getToken().getType();
      if (nodeType == HiveParser.DOT) {
        String colName = HQLParser.getColName(node).toLowerCase();
        if (!pickedReferences.containsKey(colName)) {
          return;
        }
        // No need to create a new node,
        // replace the table name identifier and column name identifier
        ASTNode tableNode = (ASTNode) node.getChild(0);
        ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);

        PickedReference refered = getPickedReference(colName, tabident.getText().toLowerCase());
        if (refered == null) {
          return;
        }
        ASTNode newTableNode =
          new ASTNode(new CommonToken(HiveParser.Identifier, refered.getChainRef().getChainName()));
        tableNode.setChild(0, newTableNode);

        ASTNode newColumnNode = new ASTNode(new CommonToken(HiveParser.Identifier,
          refered.getChainRef().getRefColumn()));
        node.setChild(1, newColumnNode);
      } else {
        // recurse down
        for (int i = 0; i < node.getChildCount(); i++) {
          ASTNode child = (ASTNode) node.getChild(i);
          resolveClause(child);
        }
      }
    }

    public Set<String> getNonReachableReferenceFields(String table) {
      Set<String> nonReachableFields = new HashSet<>();
      if (tableToRefCols.containsKey(table)) {
        for (ReferencedQueriedColumn refcol : tableToRefCols.get(table)) {
          if (getReferencedCols().get(refcol.col.getName()).isEmpty()) {
            log.info("For table:{}, the column {} is not available", table, refcol.col);
            nonReachableFields.add(refcol.col.getName());
          }
        }
      }
      return nonReachableFields;
    }
  }

  private void addRefColsQueried(CubeQueryContext cubeql, TrackQueriedColumns tqc, DenormalizationContext denormCtx) {
    for (Map.Entry<String, Set<String>> entry : tqc.getTblAliasToColumns().entrySet()) {
      // skip default alias
      if (entry.getKey() == CubeQueryContext.DEFAULT_TABLE) {
        continue;
      }
      // skip join chain aliases
      if (cubeql.getJoinchains().keySet().contains(entry.getKey().toLowerCase())) {
        continue;
      }
      AbstractCubeTable tbl = cubeql.getCubeTableForAlias(entry.getKey());
      Set<String> columns = entry.getValue();
      for (String column : columns) {
        CubeColumn col;
        if (tbl instanceof CubeInterface) {
          col = ((CubeInterface) tbl).getColumnByName(column);
        } else {
          col = ((Dimension) tbl).getColumnByName(column);
        }
        if (col instanceof ReferencedDimAttribute) {
          // considering all referenced dimensions to be denormalized columns
          denormCtx.addReferencedCol(column, new ReferencedQueriedColumn((ReferencedDimAttribute) col, tbl));
        }
      }
    }
  }
  private static DenormalizationContext getOrCreateDeNormCtx(TrackDenormContext tdc) {
    DenormalizationContext denormCtx = tdc.getDeNormCtx();
    if (denormCtx == null) {
      denormCtx = new DenormalizationContext();
      tdc.setDeNormCtx(denormCtx);
    }
    return denormCtx;
  }
  /**
   * Find all de-normalized columns, if these columns are not directly available in candidate tables, query will be
   * replaced with the corresponding table reference
   */
  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    DenormalizationContext denormCtx = cubeql.getDeNormCtx();
    if (denormCtx == null) {
      // Adds all the reference dimensions as eligible for denorm fields
      // add ref columns in cube
      addRefColsQueried(cubeql, cubeql, getOrCreateDeNormCtx(cubeql));
      // add ref columns from expressions
      for (Set<ExpressionContext> ecSet : cubeql.getExprCtx().getAllExprsQueried().values()) {
        for (ExpressionContext ec : ecSet) {
          for (ExprSpecContext esc : ec.getAllExprs()) {
            addRefColsQueried(cubeql, esc, getOrCreateDeNormCtx(esc));
          }
        }
      }
    } else if (!denormCtx.tableToRefCols.isEmpty()) {
      denormCtx.pruneReferences(cubeql);
      // In the second iteration of denorm resolver
      // candidate tables which require denorm fields and the refernces are no
      // more valid will be pruned
      if (cubeql.getCube() != null && !cubeql.getCandidateFacts().isEmpty()) {
        for (Iterator<CandidateFact> i = cubeql.getCandidateFacts().iterator(); i.hasNext();) {
          CandidateFact cfact = i.next();
          Set<String> nonReachableFields = denormCtx.getNonReachableReferenceFields(cfact.getName());
          if (!nonReachableFields.isEmpty()) {
            log.info("Not considering fact table:{} as columns {} are not available", cfact, nonReachableFields);
            cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.columnNotFound(nonReachableFields));
            i.remove();
          }
        }
        if (cubeql.getCandidateFacts().size() == 0) {
          throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(),
              cubeql.getColumnsQueriedForTable(cubeql.getCube().getName()).toString());
        }
        cubeql.pruneCandidateFactSet(CandidateTablePruneCode.COLUMN_NOT_FOUND);
      }
      if (cubeql.getDimensions() != null && !cubeql.getDimensions().isEmpty()) {
        for (Dimension dim : cubeql.getDimensions()) {
          for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator(); i.hasNext();) {
            CandidateDim cdim = i.next();
            Set<String> nonReachableFields = denormCtx.getNonReachableReferenceFields(cdim.getName());
            if (!nonReachableFields.isEmpty()) {
              log.info("Not considering dim table:{} as column {} is not available", cdim, nonReachableFields);
              cubeql.addDimPruningMsgs(dim, cdim.dimtable,
                CandidateTablePruneCause.columnNotFound(nonReachableFields));
              i.remove();
            }
          }

          if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
            throw new LensException(LensCubeErrorCode.NO_DIM_HAS_COLUMN.getLensErrorInfo(),
              dim.toString(), cubeql.getColumnsQueriedForTable(dim.getName()).toString());
          }
        }
      }
    }
  }
}
