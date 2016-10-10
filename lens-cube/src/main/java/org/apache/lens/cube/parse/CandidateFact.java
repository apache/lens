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

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import java.util.*;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.antlr.runtime.CommonToken;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;

/**
 * Holds context of a candidate fact table.
 */
public class CandidateFact implements CandidateTable, QueryAST {
  final CubeFactTable fact;
  @Getter
  private Set<String> storageTables;
  @Getter
  private int numQueriedParts = 0;
  @Getter
  private final Set<FactPartition> partsQueried = Sets.newHashSet();

  private CubeInterface baseTable;
  @Getter
  @Setter
  private ASTNode selectAST;
  @Getter
  @Setter
  private ASTNode whereAST;
  @Getter
  @Setter
  private ASTNode groupByAST;
  @Getter
  @Setter
  private ASTNode havingAST;
  @Getter
  @Setter
  private ASTNode joinAST;
  @Getter
  @Setter
  private ASTNode orderByAST;
  @Getter
  @Setter
  private Integer limitValue;
  @Getter
  private String fromString;
  private final List<Integer> selectIndices = Lists.newArrayList();
  private final List<Integer> dimFieldIndices = Lists.newArrayList();
  private Collection<String> columns;
  @Getter
  private final Map<String, ASTNode> storgeWhereClauseMap = new HashMap<>();
  @Getter
  private final Map<String, String> storgeWhereStringMap = new HashMap<>();
  @Getter
  private final Map<TimeRange, Map<String, LinkedHashSet<FactPartition>>> rangeToStoragePartMap = new HashMap<>();
  @Getter
  private final Map<TimeRange, Map<String, String>> rangeToStorageWhereMap = new HashMap<>();

  CandidateFact(CubeFactTable fact, CubeInterface cube) {
    this.fact = fact;
    this.baseTable = cube;
  }

  @Override
  public String toString() {
    return fact.toString();
  }

  public Collection<String> getColumns() {
    if (columns == null) {
      columns = fact.getValidColumns();
      if (columns == null) {
        columns = fact.getAllFieldNames();
      }
    }
    return columns;
  }

  public boolean isValidForTimeRange(TimeRange timeRange) {
    return (!timeRange.getFromDate().before(fact.getStartTime())) && (!timeRange.getToDate().after(fact.getEndTime()));
  }

  public void addToHaving(ASTNode ast) {
    if (getHavingAST() == null) {
      setHavingAST(new ASTNode(new CommonToken(TOK_HAVING, "TOK_HAVING")));
      getHavingAST().addChild(ast);
      return;
    }
    ASTNode existingHavingAST = (ASTNode) getHavingAST().getChild(0);
    ASTNode newHavingAST = new ASTNode(new CommonToken(KW_AND, "AND"));
    newHavingAST.addChild(existingHavingAST);
    newHavingAST.addChild(ast);
    getHavingAST().setChild(0, newHavingAST);
  }

  public String addAndGetAliasFromSelect(ASTNode ast, AliasDecider aliasDecider) {
    for (Node n : getSelectAST().getChildren()) {
      ASTNode astNode = (ASTNode) n;
      if (HQLParser.equalsAST(ast, (ASTNode) astNode.getChild(0))) {
        if (astNode.getChildCount() > 1) {
          return astNode.getChild(1).getText();
        }
        String alias = aliasDecider.decideAlias(astNode);
        astNode.addChild(new ASTNode(new CommonToken(Identifier, alias)));
        return alias;
      }
    }
    // Not found, have to add to select
    String alias = aliasDecider.decideAlias(ast);
    ASTNode selectExprNode = new ASTNode(new CommonToken(TOK_SELEXPR));
    selectExprNode.addChild(ast);
    selectExprNode.addChild(new ASTNode(new CommonToken(Identifier, alias)));
    getSelectAST().addChild(selectExprNode);
    return alias;
  }

  void incrementPartsQueried(int incr) {
    numQueriedParts += incr;
  }

  // copy ASTs from CubeQueryContext
  public void copyASTs(CubeQueryContext cubeql) throws LensException {
    setSelectAST(MetastoreUtil.copyAST(cubeql.getSelectAST()));
    setWhereAST(MetastoreUtil.copyAST(cubeql.getWhereAST()));
    if (cubeql.getJoinAST() != null) {
      setJoinAST(MetastoreUtil.copyAST(cubeql.getJoinAST()));
    }
    if (cubeql.getGroupByAST() != null) {
      setGroupByAST(MetastoreUtil.copyAST(cubeql.getGroupByAST()));
    }
  }


  public ASTNode getStorageWhereClause(String storageTable) {
    return storgeWhereClauseMap.get(storageTable);
  }
  public String getStorageWhereString(String storageTable) {
    return storgeWhereStringMap.get(storageTable);
  }

  public boolean isExpressionAnswerable(ASTNode node, CubeQueryContext context) throws LensException {
    return getColumns().containsAll(HQLParser.getColsInExpr(context.getAliasForTableName(context.getCube()), node));
  }

  /**
   * Update the ASTs to include only the fields queried from this fact, in all the expressions
   *
   * @param cubeql
   * @throws LensException
   */
  public void updateASTs(CubeQueryContext cubeql) throws LensException {
    // update select AST with selected fields
    int currentChild = 0;
    for (int i = 0; i < cubeql.getSelectAST().getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) this.selectAST.getChild(currentChild);
      Set<String> exprCols = HQLParser.getColsInExpr(cubeql.getAliasForTableName(cubeql.getCube()), selectExpr);
      if (getColumns().containsAll(exprCols)) {
        selectIndices.add(i);
        if (exprCols.isEmpty() // no direct fact columns
          // does not have measure names
          || (!containsAny(cubeql.getCube().getMeasureNames(), exprCols))) {
          dimFieldIndices.add(i);
        }
        ASTNode aliasNode = HQLParser.findNodeByPath(selectExpr, Identifier);
        String alias = cubeql.getSelectPhrases().get(i).getSelectAlias();
        if (aliasNode != null) {
          String queryAlias = aliasNode.getText();
          if (!queryAlias.equals(alias)) {
            // replace the alias node
            ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, alias));
            this.selectAST.getChild(currentChild).replaceChildren(selectExpr.getChildCount() - 1,
              selectExpr.getChildCount() - 1, newAliasNode);
          }
        } else {
          // add column alias
          ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, alias));
          this.selectAST.getChild(currentChild).addChild(newAliasNode);
        }
      } else {
        this.selectAST.deleteChild(currentChild);
        currentChild--;
      }
      currentChild++;
    }

    // don't need to update where ast, since where is only on dim attributes and dim attributes
    // are assumed to be common in multi fact queries.

    // push down of having clauses happens just after this call in cubequerycontext
  }

  // The source set contains atleast one column in the colSet
  static boolean containsAny(Collection<String> srcSet, Collection<String> colSet) {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    for (String column : colSet) {
      if (srcSet.contains(column)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getStorageString(String alias) {
    return StringUtils.join(storageTables, ",") + " " + alias;
  }

  public void setStorageTables(Set<String> storageTables) {
    String database = SessionState.get().getCurrentDatabase();
    // Add database name prefix for non default database
    if (StringUtils.isNotBlank(database) && !"default".equalsIgnoreCase(database)) {
      Set<String> storageTbls = new TreeSet<>();
      Iterator<String> names = storageTables.iterator();
      while (names.hasNext()) {
        storageTbls.add(database + "." + names.next());
      }
      this.storageTables = storageTbls;
    } else {
      this.storageTables = storageTables;
    }
  }

  @Override
  public AbstractCubeTable getBaseTable() {
    return (AbstractCubeTable) baseTable;
  }

  @Override
  public CubeFactTable getTable() {
    return fact;
  }

  @Override
  public String getName() {
    return fact.getName();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CandidateFact other = (CandidateFact) obj;

    if (this.getTable() == null) {
      if (other.getTable() != null) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getTable() == null) ? 0 : getTable().getName().toLowerCase().hashCode());
    return result;
  }

  public String getSelectString() {
    return HQLParser.getString(selectAST);
  }

  public String getWhereString() {
    if (whereAST != null) {
      return HQLParser.getString(whereAST);
    }
    return null;
  }

  public String getHavingString() {
    if (havingAST != null) {
      return HQLParser.getString(havingAST);
    }
    return null;
  }

  @Override
  public String getOrderByString() {
    if (orderByAST != null) {
      return HQLParser.getString(orderByAST);
    }
    return null;
  }

  /**
   * @return the selectIndices
   */
  public List<Integer> getSelectIndices() {
    return selectIndices;
  }

  /**
   * @return the groupbyIndices
   */
  public List<Integer> getDimFieldIndices() {
    return dimFieldIndices;
  }

  public String getGroupByString() {
    if (groupByAST != null) {
      return HQLParser.getString(groupByAST);
    }
    return null;
  }

  public Set<String> getTimePartCols(CubeQueryContext query) throws LensException {
    Set<String> cubeTimeDimensions = baseTable.getTimedDimensions();
    Set<String> timePartDimensions = new HashSet<String>();
    String singleStorageTable = storageTables.iterator().next();
    List<FieldSchema> partitionKeys = null;
    partitionKeys = query.getMetastoreClient().getTable(singleStorageTable).getPartitionKeys();
    for (FieldSchema fs : partitionKeys) {
      if (cubeTimeDimensions.contains(CubeQueryContext.getTimeDimOfPartitionColumn(baseTable, fs.getName()))) {
        timePartDimensions.add(fs.getName());
      }
    }
    return timePartDimensions;
  }

  public void updateFromString(CubeQueryContext query, Set<Dimension> queryDims,
    Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    fromString = "%s"; // to update the storage alias later
    if (query.isAutoJoinResolved()) {
      fromString =
        query.getAutoJoinCtx().getFromString(fromString, this, queryDims, dimsToQuery,
          query, this);
    }
  }
}
