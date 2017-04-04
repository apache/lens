package org.apache.lens.cube.parse;

import static org.apache.lens.cube.parse.StorageUtil.joinWithAnd;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.antlr.runtime.CommonToken;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;

/**
 * Created on 31/03/17.
 */
public class StorageCandidateHQLContext extends DimHQLContext {
  @Getter
  private StorageCandidate storageCandidate;
//  @Getter
//  private Set<Integer> answerableMeasurePhraseIndices = Sets.newHashSet();
  @Getter
  @Setter
  private String fromString;
  @Getter
  @Setter
  private String whereString;
  //todo set
  @Setter
  CubeQueryContext rootCubeQueryContext;
  StorageCandidateHQLContext(StorageCandidate storageCandidate, Map<Dimension, CandidateDim> dimsToQuery, Set<Dimension> queriedDims, QueryAST ast) throws LensException {
    super(storageCandidate.getCubeQueryContext(), dimsToQuery, queriedDims, ast);
    this.storageCandidate = storageCandidate;
    getCubeQueryContext().addRangeClauses(this);
  }

  private void addQueriedDims(Collection<Dimension> dims) { //todo move to DimHQLContext
    queriedDims.addAll(dims);
  }

  public CubeQueryContext getCubeQueryContext() {
    return storageCandidate.getCubeQueryContext();
  }

  public String getAliasForTable(String alias) {
    String database = SessionState.get().getCurrentDatabase();
    String ret;
    if (alias == null || alias.isEmpty()) {
      ret = storageCandidate.getResolvedName();
    } else {
      ret = storageCandidate.getResolvedName() + " " + alias;
    }
    if (StringUtils.isNotBlank(database) && !"default".equalsIgnoreCase(database)) {
      ret = database + "." + ret;
    }
    return ret;
  }

  public void updateFromString() throws LensException {
    String alias = getCubeQueryContext().getAliasForTableName(getCube().getName());
    fromString = getAliasForTable(alias);
    if (query.isAutoJoinResolved()) {
      fromString = query.getAutoJoinCtx().getFromString(fromString, this, this.queriedDims, this.dimsToQuery, query, getCubeQueryContext());
    }
  }

  @Override
  public QueryWriter toQueryWriter() {
    return this;
  }

  private CubeInterface getCube() {
    return storageCandidate.getCubeQueryContext().getCube();
  }

  @Override
  protected String getFromTable() throws LensException {
    if (storageCandidate.getCubeQueryContext().isAutoJoinResolved()) {
      return fromString;
    } else {
      return storageCandidate.getCubeQueryContext().getQBFromString(storageCandidate, getDimsToQuery());
    }
  }

  @Override
  public void addAutoJoinDims() throws LensException {
    if (getCubeQueryContext().isAutoJoinResolved()) {
      Set<Dimension> factJoiningTables = getCubeQueryContext().getAutoJoinCtx().pickOptionalTables(this, getQueriedDims(), getCubeQueryContext());
      addQueriedDims(factJoiningTables);
      dimsToQuery.putAll(getCubeQueryContext().pickCandidateDimsToQuery(factJoiningTables));
    }
  }

  @Override
  public void addExpressionDims() throws LensException {
    Set<Dimension> factExprDimTables = getCubeQueryContext().getExprCtx().rewriteExprCtx(getCubeQueryContext(), this, dimsToQuery, getQueryAst());
    addQueriedDims(factExprDimTables);
    dimsToQuery.putAll(getCubeQueryContext().pickCandidateDimsToQuery(factExprDimTables));
  }

  @Override
  public void addDenormDims() throws LensException {
    Set<Dimension> factDenormTables = getCubeQueryContext().getDeNormCtx().rewriteDenormctx(getCubeQueryContext(), this, dimsToQuery, true);
    addQueriedDims(factDenormTables);
    dimsToQuery.putAll(getCubeQueryContext().pickCandidateDimsToQuery(factDenormTables));
  }

  @Override
  public void updateDimFilterWithFactFilter() throws LensException {
    if (!getStorageCandidate().getStorageName().isEmpty()) {
      String qualifiedStorageTable = getStorageCandidate().getStorageName();
      String storageTable = qualifiedStorageTable.substring(qualifiedStorageTable.indexOf(".") + 1);
      String where = getCubeQueryContext().getWhere(this, getCubeQueryContext().getAutoJoinCtx(),
        getQueryAst().getWhereAST(), getCubeQueryContext().getAliasForTableName(getStorageCandidate().getBaseTable().getName()),
        getCubeQueryContext().shouldReplaceDimFilterWithFactFilter(), storageTable, dimsToQuery);
      setWhereString(where);
    }
  }
  public void updateAnswerableSelectColumns() throws LensException {
    rootCubeQueryContext = getCubeQueryContext(); // todo remove this and clean up
    CubeQueryContext cubeql = getCubeQueryContext();
    // update select AST with selected fields
    int currentChild = 0;
    for (int i = 0; i < cubeql.getSelectAST().getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) queryAst.getSelectAST().getChild(currentChild);
      Set<String> exprCols = HQLParser.getColsInExpr(cubeql.getAliasForTableName(cubeql.getCube()), selectExpr);
      if (getStorageCandidate().getColumns().containsAll(exprCols)) {
        ASTNode aliasNode = HQLParser.findNodeByPath(selectExpr, HiveParser.Identifier);
        String alias = cubeql.getSelectPhrases().get(i).getSelectAlias();
        if (aliasNode != null) {
          String queryAlias = aliasNode.getText();
          if (!queryAlias.equals(alias)) {
            // replace the alias node
            ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, alias));
            queryAst.getSelectAST().getChild(currentChild)
              .replaceChildren(selectExpr.getChildCount() - 1, selectExpr.getChildCount() - 1, newAliasNode);
          }
        } else {
          // add column alias
          ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, alias));
          queryAst.getSelectAST().getChild(currentChild).addChild(newAliasNode);
        }
      } else {
        queryAst.getSelectAST().deleteChild(currentChild);
        currentChild--;
      }
      currentChild++;
    }
  }
  private String genWhereClauseWithDimPartitions(String originalWhere, Set<Dimension> queriedDims) {
    StringBuilder whereBuf;
    if (originalWhere != null) {
      whereBuf = new StringBuilder(originalWhere);
    } else {
      whereBuf = new StringBuilder();
    }

    // add where clause for all dimensions
    if (getCubeQueryContext() != null) {
      boolean added = (originalWhere != null);
      for (Dimension dim : queriedDims) {
        CandidateDim cdim = dimsToQuery.get(dim);
        String alias = getCubeQueryContext().getAliasForTableName(dim.getName());
        if (!cdim.isWhereClauseAdded() && !StringUtils.isBlank(cdim.getWhereClause())) {
          appendWhereClause(whereBuf, StorageUtil.getWhereClause(cdim, alias), added);
          added = true;
        }
      }
    }
    if (whereBuf.length() == 0) {
      return null;
    }
    return whereBuf.toString();
  }

  private void setMissingExpressions(Set<Dimension> queriedDims) throws LensException {
    setFromString(String.format("%s", getFromTable()));
    setWhereString(joinWithAnd(
      genWhereClauseWithDimPartitions(whereString, queriedDims), getCubeQueryContext().getConf().getBoolean(
        CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL, CubeQueryConfUtil.DEFAULT_REPLACE_TIMEDIM_WITH_PART_COL)
        ? getPostSelectionWhereClause() : null));
    if (rootCubeQueryContext == getCubeQueryContext() && this == getCubeQueryContext().getPickedCandidate()) {
      if (getCubeQueryContext().getHavingAST() != null) {
        queryAst.setHavingAST(MetastoreUtil.copyAST(getCubeQueryContext().getHavingAST()));
      }
    }
  }
  private void updateOrderByWithFinalAlias(ASTNode orderby, ASTNode select) {
    if (orderby == null) {
      return;
    }
    for (Node orderbyNode : orderby.getChildren()) {
      ASTNode orderBychild = (ASTNode) orderbyNode;
      for (Node selectNode : select.getChildren()) {
        ASTNode selectChild = (ASTNode) selectNode;
        if (selectChild.getChildCount() == 2) {
          if (HQLParser.getString((ASTNode) selectChild.getChild(0))
            .equals(HQLParser.getString((ASTNode) orderBychild.getChild(0)))) {
            ASTNode alias = new ASTNode((ASTNode) selectChild.getChild(1));
            orderBychild.replaceChildren(0, 0, alias);
            break;
          }
        }
      }
    }
  }

  public String toHQL() throws LensException {
    setMissingExpressions(queriedDims);
    // Check if the picked candidate is a StorageCandidate and in that case
    // update the selectAST with final alias.
    String prefix = "";
    if (Objects.equals(rootCubeQueryContext, getCubeQueryContext()) && Objects.equals(getStorageCandidate(), getCubeQueryContext().getPickedCandidate())) {
      CandidateUtil.updateFinalAlias(queryAst.getSelectAST(), getCubeQueryContext());
      updateOrderByWithFinalAlias(queryAst.getOrderByAST(), queryAst.getSelectAST());
      prefix = getCubeQueryContext().getInsertClause();
    } else {
      queryAst.setHavingAST(null);
    }
    return prefix + CandidateUtil
      .buildHQLString(queryAst.getSelectString(), fromString, whereString, queryAst.getGroupByString(),
        queryAst.getOrderByString(), queryAst.getHavingString(), queryAst.getLimitValue());
  }
}
