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


import static org.apache.lens.cube.metadata.MetastoreConstants.VIRTUAL_FACT_FILTER;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

import lombok.Getter;

/**
 * Created on 31/03/17.
 */
public class StorageCandidateHQLContext extends DimHQLContext {
  @Getter
  private StorageCandidate storageCandidate;
  private CubeQueryContext rootCubeQueryContext;

  StorageCandidateHQLContext(StorageCandidate storageCandidate, Map<Dimension, CandidateDim> dimsToQuery,
    QueryAST ast, CubeQueryContext rootCubeQueryContext) throws LensException {
    super(storageCandidate.getCubeQueryContext(), dimsToQuery, ast);
    this.storageCandidate = storageCandidate;
    this.rootCubeQueryContext = rootCubeQueryContext;
    getCubeQueryContext().addRangeClauses(this);
    if (!Objects.equals(getStorageCandidate(), rootCubeQueryContext.getPickedCandidate())) {
      getQueryAst().setHavingAST(null);
    }
  }
  private boolean isRoot() {
    return Objects.equals(getCubeQueryContext(), rootCubeQueryContext)
      && Objects.equals(getStorageCandidate(), getCubeQueryContext().getPickedCandidate());
  }
  public CubeQueryContext getCubeQueryContext() {
    return storageCandidate.getCubeQueryContext();
  }

  public void updateFromString() throws LensException {
    String alias = getCubeQueryContext().getAliasForTableName(getCube().getName());
    setFrom(storageCandidate.getAliasForTable(alias));
    if (getCubeQueryContext().isAutoJoinResolved()) {
      setFrom(getCubeQueryContext().getAutoJoinCtx().getFromString(
        getFrom(), this, getDimsToQuery(), getCubeQueryContext()));
    }
  }

  CubeInterface getCube() {
    return storageCandidate.getCubeQueryContext().getCube();
  }

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
        getQueryAst().getWhereAST(),
        getCubeQueryContext().getAliasForTableName(getStorageCandidate().getBaseTable().getName()),
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
    String whereString = genWhereClauseWithDimPartitions(getWhere());
    StringBuilder whereStringBuilder = (whereString != null) ? new StringBuilder(whereString) :  new StringBuilder();

    if (this.storageCandidate.getFact().getProperties().get(VIRTUAL_FACT_FILTER) != null) {
      appendWhereClause(whereStringBuilder,
        this.storageCandidate.getFact().getProperties().get(VIRTUAL_FACT_FILTER), whereString != null);
    }
    setWhere(whereStringBuilder.length() == 0 ? null : whereStringBuilder.toString());

    if (isRoot()) {
      if (Objects.equals(getStorageCandidate(), getCubeQueryContext().getPickedCandidate())) {
        updateAnswerableSelectColumns();
        // Check if the picked candidate is a StorageCandidate and in that case
        // update the selectAST with final alias.
        CandidateUtil.updateFinalAlias(queryAst.getSelectAST(), getCubeQueryContext());
        CandidateUtil.updateOrderByWithFinalAlias(queryAst.getOrderByAST(), queryAst.getSelectAST());
        setPrefix(getCubeQueryContext().getInsertClause());
      }
    }
  }

  @Override
  public int hashCode() {
    final int PRIME = 59;
    int result = 1;
    result = result * PRIME + getStorageCandidate().hashCode();
    result = result * PRIME + getCube().hashCode();
    return result;
  }
}
