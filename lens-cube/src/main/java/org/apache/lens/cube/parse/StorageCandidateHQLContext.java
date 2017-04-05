package org.apache.lens.cube.parse;


import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

import lombok.Getter;
import lombok.Setter;

/**
 * Created on 31/03/17.
 */
public class StorageCandidateHQLContext extends DimHQLContext {
  @Getter
  private StorageCandidate storageCandidate;
  //todo set
  @Setter
  CubeQueryContext rootCubeQueryContext;

  StorageCandidateHQLContext(StorageCandidate storageCandidate, Map<Dimension, CandidateDim> dimsToQuery, QueryAST ast) throws LensException {
    super(storageCandidate.getCubeQueryContext(), dimsToQuery, ast);
    this.storageCandidate = storageCandidate;
    getCubeQueryContext().addRangeClauses(this);
  }

  public CubeQueryContext getCubeQueryContext() {
    return storageCandidate.getCubeQueryContext();
  }

  public void updateFromString() throws LensException {
    String alias = getCubeQueryContext().getAliasForTableName(getCube().getName());
    setFrom(storageCandidate.getAliasForTable(alias));
    if (getCubeQueryContext().isAutoJoinResolved()) {
      setFrom(getCubeQueryContext().getAutoJoinCtx().getFromString(getFrom(), this, getDimsToQuery(), getCubeQueryContext()));
    }
  }

  private CubeInterface getCube() {
    return storageCandidate.getCubeQueryContext().getCube();
  }

  // todo check for unification of getFromTable and updateFromString
  @Override
  protected String getFromTable() throws LensException {
    if (storageCandidate.getCubeQueryContext().isAutoJoinResolved()) {
      return getFrom();
    } else {
      return storageCandidate.getCubeQueryContext().getQBFromString(storageCandidate, getDimsToQuery());
    }
  }

  @Override
  public void updateDimFilterWithFactFilter() throws LensException {
    if (!getStorageCandidate().getStorageName().isEmpty()) {
      String qualifiedStorageTable = getStorageCandidate().getStorageName();
      String storageTable = qualifiedStorageTable.substring(qualifiedStorageTable.indexOf(".") + 1);
      String where = getCubeQueryContext().getWhere(this, getCubeQueryContext().getAutoJoinCtx(),
        getQueryAst().getWhereAST(), getCubeQueryContext().getAliasForTableName(getStorageCandidate().getBaseTable().getName()),
        getCubeQueryContext().shouldReplaceDimFilterWithFactFilter(), storageTable, getDimsToQuery());
      setWhere(where);
    }
  }

  private void updateAnswerableSelectColumns() throws LensException {
    // update select AST with selected fields
    int currentChild = 0;
    for (int i = 0; i < getCubeQueryContext().getSelectAST().getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) queryAst.getSelectAST().getChild(currentChild);
      Set<String> exprCols = HQLParser.getColsInExpr(getCubeQueryContext().getAliasForTableName(getCube()), selectExpr);
      if (getStorageCandidate().getColumns().containsAll(exprCols)) {
        ASTNode aliasNode = HQLParser.findNodeByPath(selectExpr, HiveParser.Identifier);
        String alias = getCubeQueryContext().getSelectPhrases().get(i).getSelectAlias();
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

  @Override
  protected void setMissingExpressions() throws LensException {
    setFrom(getFromTable());
    setWhere(genWhereClauseWithDimPartitions(getWhere()));
    if (rootCubeQueryContext == null) {
      updateAnswerableSelectColumns();
      if (Objects.equals(getStorageCandidate(), getCubeQueryContext().getPickedCandidate())) {
        // Check if the picked candidate is a StorageCandidate and in that case
        // update the selectAST with final alias.
        CandidateUtil.updateFinalAlias(queryAst.getSelectAST(), getCubeQueryContext());
        updateOrderByWithFinalAlias(queryAst.getOrderByAST(), queryAst.getSelectAST());
        setPrefix(getCubeQueryContext().getInsertClause());
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
}
