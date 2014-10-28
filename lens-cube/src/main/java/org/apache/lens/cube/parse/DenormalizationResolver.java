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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.CommonToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.AbstractCubeTable;
import org.apache.lens.cube.metadata.CubeColumn;
import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.ReferencedDimAtrribute;
import org.apache.lens.cube.metadata.TableReference;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CubeTableCause;

/**
 * This class resolves all the reference columns that are queried.
 * 
 * Keeps track of the context that if any candidate needs to use columns through
 * tables referenced and replaces the columns from referenced tables in all the
 * ASTs
 * 
 */
public class DenormalizationResolver implements ContextRewriter {

  private static final Log LOG = LogFactory.getLog(DenormalizationResolver.class);

  public DenormalizationResolver(Configuration conf) {
  }

  public static class ReferencedQueriedColumn {
    ReferencedDimAtrribute col;
    AbstractCubeTable srcTable;
    List<TableReference> references;

    ReferencedQueriedColumn(ReferencedDimAtrribute col, AbstractCubeTable srcTable) {
      this.col = col;
      this.srcTable = srcTable;
      references = new ArrayList<TableReference>();
      references.addAll(col.getReferences());
    }

    public String toString() {
      return srcTable + ":" + col;
    }
  }

  public static class PickedReference {
    TableReference reference;
    String srcAlias;
    String pickedFor;

    PickedReference(TableReference reference, String srcAlias, String pickedFor) {
      this.srcAlias = srcAlias;
      this.reference = reference;
      this.pickedFor = pickedFor;
    }

    public String toString() {
      return srcAlias + ":" + reference + " for :" + pickedFor;
    }
  }

  public static class DenormalizationContext {
    // map of column name to all references
    private Map<String, Set<ReferencedQueriedColumn>> referencedCols =
        new HashMap<String, Set<ReferencedQueriedColumn>>();

    // candidate table name to all the references columns it needs
    private Map<String, Set<ReferencedQueriedColumn>> tableToRefCols =
        new HashMap<String, Set<ReferencedQueriedColumn>>();

    private CubeQueryContext cubeql;

    // set of all picked references once all candidate tables are picked
    private Set<PickedReference> pickedRefs = new HashSet<PickedReference>();
    // index on column name for picked references with map from column name to
    // pickedrefs
    private Map<String, Set<PickedReference>> pickedReferences = new HashMap<String, Set<PickedReference>>();

    DenormalizationContext(CubeQueryContext cubeql) {
      this.cubeql = cubeql;
    }

    void addReferencedCol(String col, ReferencedQueriedColumn refer) {
      Set<ReferencedQueriedColumn> refCols = referencedCols.get(col);
      if (refCols == null) {
        refCols = new HashSet<ReferencedQueriedColumn>();
        referencedCols.put(col, refCols);
      }
      refCols.add(refer);
    }

    // When candidate table does not have the field, this method checks
    // if the field can be reached through reference,
    // if yes adds the ref usage and returns to true, if not returns false.
    boolean addRefUsage(CandidateTable table, String col, String srcTbl) throws SemanticException {
      // available as referenced col
      if (referencedCols.containsKey(col)) {
        for (ReferencedQueriedColumn refer : referencedCols.get(col)) {
          if (refer.srcTable.getName().equalsIgnoreCase(srcTbl)) {
            // check if reference source column is available in src table?
            // should not be required here. Join resolution will figure out if
            // there is no path
            // to the source table
            LOG.info("Adding denormalized column for column:" + col + " for table:" + table);
            Set<ReferencedQueriedColumn> refCols = tableToRefCols.get(table.getName());
            if (refCols == null) {
              refCols = new HashSet<ReferencedQueriedColumn>();
              tableToRefCols.put(table.getName(), refCols);
            }
            refCols.add(refer);
            // Add to optional tables
            for (TableReference reference : refer.col.getReferences()) {
              cubeql.addOptionalDimTable(reference.getDestTable(), reference.getDestColumn(), table, false);
            }
            return true;
          }
        }
      }
      return false;
    }

    Map<String, Set<ReferencedQueriedColumn>> getReferencedCols() {
      return referencedCols;
    }

    private void addPickedReference(String col, PickedReference refer) {
      Set<PickedReference> refCols = pickedReferences.get(col);
      if (refCols == null) {
        refCols = new HashSet<PickedReference>();
        pickedReferences.put(col, refCols);
      }
      refCols.add(refer);
    }

    private PickedReference getPickedReference(String col, String srcAlias) {
      if (pickedReferences.containsKey(col)) {
        for (PickedReference ref : pickedReferences.get(col)) {
          if (ref.srcAlias.equalsIgnoreCase(srcAlias)) {
            LOG.info("Picked reference for " + col + " ref:" + pickedReferences.get(col));
            return ref;
          }
        }
      }
      return null;
    }

    public Set<Dimension> rewriteDenormctx(CandidateFact cfact, Map<Dimension, CandidateDim> dimsToQuery,
        boolean replaceFact) throws SemanticException {
      Set<Dimension> refTbls = new HashSet<Dimension>();

      if (!tableToRefCols.isEmpty()) {
        // pick referenced columns for fact
        if (cfact != null) {
          pickColumnsForTable(cfact.getName());
        }
        // pick referenced columns for dimensions
        if (dimsToQuery != null && !dimsToQuery.isEmpty()) {
          for (CandidateDim cdim : dimsToQuery.values()) {
            pickColumnsForTable(cdim.getName());
          }
        }
        // Replace picked reference in all the base trees
        replaceReferencedColumns(cfact, replaceFact);

        // Add the picked references to dimsToQuery
        for (PickedReference picked : pickedRefs) {
          if (isPickedFor(picked, cfact, dimsToQuery)) {
            refTbls.add((Dimension) cubeql.getCubeTableForAlias(picked.reference.getDestTable()));
          }
        }
      }
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

    private void pickColumnsForTable(String tbl) {
      if (tableToRefCols.containsKey(tbl)) {
        for (ReferencedQueriedColumn refered : tableToRefCols.get(tbl)) {
          Iterator<TableReference> iter = refered.references.iterator();
          while (iter.hasNext()) {
            // remove unreachable references
            TableReference reference = iter.next();
            if (!cubeql.getAutoJoinCtx().isReachableDim(
                (Dimension) cubeql.getCubeTableForAlias(reference.getDestTable()))) {
              iter.remove();
            }
          }
          PickedReference picked =
              new PickedReference(refered.references.iterator().next(), cubeql.getAliasForTabName(refered.srcTable
                  .getName()), tbl);
          addPickedReference(refered.col.getName(), picked);
          pickedRefs.add(picked);
        }
      }
    }

    private void replaceReferencedColumns(CandidateFact cfact, boolean replaceFact) throws SemanticException {
      if (replaceFact
          && (tableToRefCols.get(cfact.getName()) != null && !tableToRefCols.get(cfact.getName()).isEmpty())) {
        resolveClause(cubeql, cfact.getSelectAST());
        resolveClause(cubeql, cfact.getWhereAST());
        resolveClause(cubeql, cfact.getGroupByAST());
        resolveClause(cubeql, cfact.getHavingAST());
      } else {
        resolveClause(cubeql, cubeql.getSelectAST());
        resolveClause(cubeql, cubeql.getWhereAST());
        resolveClause(cubeql, cubeql.getGroupByAST());
        resolveClause(cubeql, cubeql.getHavingAST());

      }
      resolveClause(cubeql, cubeql.getOrderByAST());
    }

    private void resolveClause(CubeQueryContext query, ASTNode node) throws SemanticException {
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
            new ASTNode(new CommonToken(HiveParser.Identifier, query.getAliasForTabName(refered.reference
                .getDestTable())));
        tableNode.setChild(0, newTableNode);
        newTableNode.setParent(tableNode);

        ASTNode newColumnNode = new ASTNode(new CommonToken(HiveParser.Identifier, refered.reference.getDestColumn()));
        node.setChild(1, newColumnNode);
        newColumnNode.setParent(node);
      } else {
        // recurse down
        for (int i = 0; i < node.getChildCount(); i++) {
          ASTNode child = (ASTNode) node.getChild(i);
          resolveClause(query, child);
        }
      }
    }
  }

  /**
   * Find all de-normalized columns, if these columns are not directly available
   * in candidate tables, query will be replaced with the corresponding table
   * reference
   * 
   */
  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    DenormalizationContext denormCtx = cubeql.getDenormCtx();
    if (denormCtx == null) {
      // Adds all the reference dimensions as eligible for denorm fields
      denormCtx = new DenormalizationContext(cubeql);
      cubeql.setDenormCtx(denormCtx);
      for (Map.Entry<String, Set<String>> entry : cubeql.getTblAlaisToColumns().entrySet()) {
        // skip default alias
        if (entry.getKey() == CubeQueryContext.DEFAULT_TABLE) {
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
          if (col instanceof ReferencedDimAtrribute) {
            // considering all referenced dimensions to be denormalized columns
            denormCtx.addReferencedCol(column, new ReferencedQueriedColumn((ReferencedDimAtrribute) col, tbl));
          }
        }
      }
    } else if (!denormCtx.tableToRefCols.isEmpty()) {
      // In the second iteration of denorm resolver
      // candidate tables which require denorm fields and the refernces are no
      // more valid will be pruned
      if (cubeql.getCube() != null && !cubeql.getCandidateFactTables().isEmpty()) {
        for (Iterator<CandidateFact> i = cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
          CandidateFact cfact = i.next();
          if (denormCtx.tableToRefCols.containsKey(cfact.getName())) {
            for (ReferencedQueriedColumn refcol : denormCtx.tableToRefCols.get(cfact.getName())) {
              if (denormCtx.getReferencedCols().get(refcol.col.getName()).isEmpty()) {
                LOG.info("Not considering fact table:" + cfact + " as column " + refcol.col + " is not available");
                cubeql.addFactPruningMsgs(cfact.fact, new CandidateTablePruneCause(cfact.fact.getName(),
                    CubeTableCause.COLUMN_NOT_FOUND));
                i.remove();
              }
            }
          }
        }
        if (cubeql.getCandidateFactTables().size() == 0) {
          throw new SemanticException(ErrorMsg.NO_FACT_HAS_COLUMN, cubeql.getColumnsQueried(cubeql.getCube().getName())
              .toString());
        }
        cubeql.pruneCandidateFactSet(CubeTableCause.COLUMN_NOT_FOUND);
      }
      if (cubeql.getDimensions() != null && !cubeql.getDimensions().isEmpty()) {
        for (Dimension dim : cubeql.getDimensions()) {
          for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator(); i.hasNext();) {
            CandidateDim cdim = i.next();
            if (denormCtx.tableToRefCols.containsKey(cdim.getName())) {
              for (ReferencedQueriedColumn refcol : denormCtx.tableToRefCols.get(cdim.getName())) {
                if (denormCtx.getReferencedCols().get(refcol.col.getName()).isEmpty()) {
                  LOG.info("Not considering dim table:" + cdim + " as column " + refcol.col + " is not available");
                  cubeql.addDimPruningMsgs(dim, cdim.dimtable, new CandidateTablePruneCause(cdim.dimtable.getName(),
                      CubeTableCause.COLUMN_NOT_FOUND));
                  i.remove();
                }
              }
            }
          }

          if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
            throw new SemanticException(ErrorMsg.NO_DIM_HAS_COLUMN, cubeql.getColumnsQueried(dim.getName()).toString());
          }
        }
      }
    }
  }
}
