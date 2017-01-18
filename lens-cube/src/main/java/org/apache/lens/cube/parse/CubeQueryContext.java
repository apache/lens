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

import static org.apache.lens.cube.parse.CubeQueryConfUtil.*;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import static com.google.common.base.Preconditions.checkArgument;



import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.error.NoCandidateDimAvailableException;
import org.apache.lens.cube.error.NoCandidateFactAvailableException;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.join.TableRelationship;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.cube.parse.join.AutoJoinContext;
import org.apache.lens.cube.parse.join.JoinClause;
import org.apache.lens.cube.parse.join.JoinTree;
import org.apache.lens.cube.parse.join.JoinUtils;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.*;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CubeQueryContext extends TracksQueriedColumns implements QueryAST, TrackDenormContext {
  public static final String TIME_RANGE_FUNC = "time_range_in";
  public static final String NOW = "now";
  public static final String DEFAULT_TABLE = "_default_";
  private final ASTNode ast;
  @Getter
  private final QB qb;
  private String clauseName = null;
  @Getter
  private final Configuration conf;

  @Getter
  private final List<TimeRange> timeRanges;

  // metadata
  @Getter
  private CubeInterface cube;
  // Dimensions accessed in the query, contains dimensions that are joinchain destinations
  // of the joinchains used.
  @Getter
  protected Set<Dimension> dimensions = new HashSet<Dimension>();
  // The dimensions accessed by name in the query directly, via tablename.columname
  @Getter
  protected Set<Dimension> nonChainedDimensions = new HashSet<Dimension>();
  // Joinchains accessed in the query
  @Getter
  protected Map<String, JoinChain> joinchains = new HashMap<String, JoinChain>();

  @Getter
  private final Set<String> queriedMsrs = new HashSet<String>();

  @Getter
  private final Set<String> queriedExprs = new HashSet<String>();

  private final Set<String> queriedTimeDimCols = new LinkedHashSet<String>();

  @Getter
  private final Set<String> queriedExprsWithMeasures = new HashSet<String>();

  @Getter
  // Mapping of a qualified column name to its table alias
  private final Map<String, String> colToTableAlias = new HashMap<>();

  @Getter
  private final Set<Set<CandidateFact>> candidateFactSets = new HashSet<>();

  @Getter
  // would be added through join chains and de-normalized resolver
  protected Map<Aliased<Dimension>, OptionalDimCtx> optionalDimensionMap = new HashMap<>();

  // Alias to table object mapping of tables accessed in this query
  @Getter
  private final Map<String, AbstractCubeTable> cubeTbls = new HashMap<>();

  void addSelectPhrase(SelectPhraseContext sel) {
    selectPhrases.add(sel);
    addQueriedPhrase(sel);
  }

  boolean isColumnAnAlias(String col) {
    for (SelectPhraseContext sel : selectPhrases) {
      if (col.equals(sel.getActualAlias())) {
        return true;
      }
    }
    return false;
  }

  void addQueriedPhrase(QueriedPhraseContext qur) {
    queriedPhrases.add(qur);
  }
  @Getter
  private final List<SelectPhraseContext> selectPhrases = new ArrayList<>();

  @Getter
  private final List<QueriedPhraseContext> queriedPhrases = new ArrayList<>();

  // Join conditions used in all join expressions
  @Getter
  private final Map<QBJoinTree, String> joinConds = new HashMap<QBJoinTree, String>();

  // storage specific
  @Getter
  protected final Set<CandidateFact> candidateFacts = new HashSet<CandidateFact>();
  @Getter
  protected final Map<Dimension, Set<CandidateDim>> candidateDims = new HashMap<Dimension, Set<CandidateDim>>();

  // query trees
  @Getter
  @Setter
  private ASTNode havingAST;
  @Getter
  @Setter
  private ASTNode selectAST;

  // Will be set after the Fact is picked and time ranges replaced
  @Getter
  @Setter
  private ASTNode whereAST;

  @Getter
  @Setter
  private ASTNode orderByAST;
  // Setter is used in promoting the select when promotion is on.
  @Getter
  @Setter
  private ASTNode groupByAST;
  @Getter
  private CubeMetastoreClient metastoreClient;
  @Getter
  @Setter
  private AutoJoinContext autoJoinCtx;
  @Getter
  @Setter
  private ExpressionResolver.ExpressionResolverContext exprCtx;
  @Getter
  @Setter
  private DenormalizationResolver.DenormalizationContext deNormCtx;
  @Getter
  private PruneCauses<CubeFactTable> factPruningMsgs =
    new PruneCauses<CubeFactTable>();
  @Getter
  private Map<Dimension, PruneCauses<CubeDimensionTable>> dimPruningMsgs =
    new HashMap<Dimension, PruneCauses<CubeDimensionTable>>();
  @Getter
  private String fromString;
  public CubeQueryContext(ASTNode ast, QB qb, Configuration queryConf, HiveConf metastoreConf)
    throws LensException {
    this.ast = ast;
    this.qb = qb;
    this.conf = queryConf;
    this.clauseName = getClause();
    this.timeRanges = new ArrayList<TimeRange>();
    try {
      metastoreClient = CubeMetastoreClient.getInstance(metastoreConf);
    } catch (HiveException e) {
      throw new LensException(e);
    }
    if (qb.getParseInfo().getWhrForClause(clauseName) != null) {
      this.whereAST = qb.getParseInfo().getWhrForClause(clauseName);
    }
    if (qb.getParseInfo().getHavingForClause(clauseName) != null) {
      this.havingAST = qb.getParseInfo().getHavingForClause(clauseName);
    }
    if (qb.getParseInfo().getOrderByForClause(clauseName) != null) {
      this.orderByAST = qb.getParseInfo().getOrderByForClause(clauseName);
    }
    if (qb.getParseInfo().getGroupByForClause(clauseName) != null) {
      this.groupByAST = qb.getParseInfo().getGroupByForClause(clauseName);
    }
    if (qb.getParseInfo().getSelForClause(clauseName) != null) {
      this.selectAST = qb.getParseInfo().getSelForClause(clauseName);
    }

    extractMetaTables();
  }

  public boolean hasCubeInQuery() {
    return cube != null;
  }

  public boolean hasDimensionInQuery() {
    return dimensions != null && !dimensions.isEmpty();
  }

  private void extractMetaTables() throws LensException {
    List<String> tabAliases = new ArrayList<String>(qb.getTabAliases());
    Set<String> missing = new HashSet<String>();
    for (String alias : tabAliases) {
      boolean added = addQueriedTable(alias);
      if (!added) {
        missing.add(alias);
      }
    }
    for (String alias : missing) {
      // try adding them as joinchains
      boolean added = addJoinChain(alias, false);
      if (!added) {
        log.info("Queried tables do not exist. Missing table:{}", alias);
        throw new LensException(LensCubeErrorCode.NEITHER_CUBE_NOR_DIMENSION.getLensErrorInfo());
      }
    }
  }

  private boolean addJoinChain(String alias, boolean isOptional) throws LensException {
    boolean retVal = false;
    String aliasLowerCaseStr = alias.toLowerCase();
    JoinChain joinchain = null;

    if (getCube() != null) {
      JoinChain chainByName = getCube().getChainByName(aliasLowerCaseStr);
      if (chainByName != null) {
        joinchain = chainByName;
        retVal = true;
      }
    }

    if (!retVal) {
      for (Dimension table : dimensions) {
        JoinChain chainByName = table.getChainByName(aliasLowerCaseStr);
        if (chainByName != null) {
          joinchain = chainByName;
          retVal = true;
          break;
        }
      }
    }

    if (retVal) {
      joinchains.put(aliasLowerCaseStr, new JoinChain(joinchain));
      String destTable = joinchain.getDestTable();
      boolean added = addQueriedTable(alias, destTable, isOptional, true);
      if (!added) {
        log.info("Queried tables do not exist. Missing tables:{}", destTable);
        throw new LensException(LensCubeErrorCode.NEITHER_CUBE_NOR_DIMENSION.getLensErrorInfo());
      }
      log.info("Added join chain for {}", destTable);
      return true;
    }

    return retVal;
  }

  public boolean addQueriedTable(String alias) throws LensException {
    return addQueriedTable(alias, false);
  }

  private boolean addQueriedTable(String alias, boolean isOptional) throws LensException {
    String tblName = qb.getTabNameForAlias(alias);
    if (tblName == null) {
      tblName = alias;
    }
    boolean added = addQueriedTable(alias, tblName, isOptional, false);
    if (!added) {
      // try adding as joinchain
      added = addJoinChain(alias, isOptional);
    }
    return added;
  }

  /**
   * destination table  : a table whose columns are getting queried intermediate table : a table which is only used as a
   * link between cube and destination table
   *
   * @param alias
   * @param tblName
   * @param isOptional         pass false when it's a destination table pass true when it's an intermediate table when
   *                           join chain destination is being added, this will be false.
   * @param isChainedDimension pass true when you're adding the dimension as a joinchain destination, pass false when
   *                           this table is mentioned by name in the user query
   * @return true if added
   * @throws LensException
   */
  private boolean addQueriedTable(String alias, String tblName, boolean isOptional, boolean isChainedDimension)
    throws LensException {
    alias = alias.toLowerCase();
    if (cubeTbls.containsKey(alias)) {
      return true;
    }
    try {
      if (metastoreClient.isCube(tblName)) {
        if (cube != null) {
          if (!cube.getName().equalsIgnoreCase(tblName)) {
            throw new LensException(LensCubeErrorCode.MORE_THAN_ONE_CUBE.getLensErrorInfo(), cube.getName(), tblName);
          }
        }
        cube = metastoreClient.getCube(tblName);
        cubeTbls.put(alias, (AbstractCubeTable) cube);
      } else if (metastoreClient.isDimension(tblName)) {
        Dimension dim = metastoreClient.getDimension(tblName);
        if (!isOptional) {
          dimensions.add(dim);
        }
        if (!isChainedDimension) {
          nonChainedDimensions.add(dim);
        }
        cubeTbls.put(alias, dim);
      } else {
        return false;
      }
    } catch (LensException e) {
      //TODO: check if catch can be removed
      return false;
    }
    return true;
  }

  public boolean isAutoJoinResolved() {
    return autoJoinCtx != null && autoJoinCtx.isJoinsResolved();
  }

  public Cube getBaseCube() {
    if (cube instanceof Cube) {
      return (Cube) cube;
    }
    return ((DerivedCube) cube).getParent();
  }

  public Set<String> getPartitionColumnsQueried() {
    Set<String> partsQueried = Sets.newHashSet();
    for (TimeRange range : getTimeRanges()) {
      partsQueried.add(range.getPartitionColumn());
    }
    return partsQueried;
  }

  // map of ref column in query to set of Dimension that have the column - which are added as optional dims
  @Getter
  private Map<String, Set<Aliased<Dimension>>> refColToDim = Maps.newHashMap();

  public void updateRefColDim(String col, Aliased<Dimension> dim) {
    Set<Aliased<Dimension>> refDims = refColToDim.get(col.toLowerCase());
    if (refDims == null) {
      refDims = Sets.newHashSet();
      refColToDim.put(col.toLowerCase(), refDims);
    }
    refDims.add(dim);
  }

  @Data
  @AllArgsConstructor
  static class QueriedExprColumn {
    private String exprCol;
    private String alias;
  }

  // map of expression column in query to set of Dimension that are accessed in the expression column - which are added
  // as optional dims
  @Getter
  private Map<QueriedExprColumn, Set<Aliased<Dimension>>> exprColToDim = Maps.newHashMap();

  public void updateExprColDim(String tblAlias, String col, Aliased<Dimension> dim) {

    QueriedExprColumn qexpr = new QueriedExprColumn(col, tblAlias);
    Set<Aliased<Dimension>> exprDims = exprColToDim.get(qexpr);
    if (exprDims == null) {
      exprDims = Sets.newHashSet();
      exprColToDim.put(qexpr, exprDims);
    }
    exprDims.add(dim);
  }

  // Holds the context of optional dimension
  // A dimension is optional if it is not queried directly by the user, but is
  // required by a candidate table to get a denormalized field from reference
  // or required in a join chain
  @ToString
  public static class OptionalDimCtx {
    OptionalDimCtx() {
    }

    Set<String> colQueried = new HashSet<String>();
    Set<CandidateTable> requiredForCandidates = new HashSet<CandidateTable>();
    boolean isRequiredInJoinChain = false;
  }

  public void addOptionalJoinDimTable(String alias, boolean isRequired) throws LensException {
    addOptionalDimTable(alias, null, isRequired, null, false, (String[]) null);
  }

  public void addOptionalExprDimTable(String dimAlias, String queriedExpr, String srcTableAlias,
    CandidateTable candidate, String... cols) throws LensException {
    addOptionalDimTable(dimAlias, candidate, false, queriedExpr, false, srcTableAlias, cols);
  }

  public void addOptionalDimTable(String alias, CandidateTable candidate, boolean isRequiredInJoin, String cubeCol,
    boolean isRef, String... cols) throws LensException {
    addOptionalDimTable(alias, candidate, isRequiredInJoin, cubeCol, isRef, null, cols);
  }

  private void addOptionalDimTable(String alias, CandidateTable candidate, boolean isRequiredInJoin, String cubeCol,
    boolean isRef, String tableAlias, String... cols) throws LensException {
    alias = alias.toLowerCase();
    if (!addQueriedTable(alias, true)) {
      throw new LensException(LensCubeErrorCode.QUERIED_TABLE_NOT_FOUND.getLensErrorInfo(), alias);
    }
    Dimension dim = (Dimension) cubeTbls.get(alias);
    Aliased<Dimension> aliasedDim = Aliased.create(dim, alias);
    OptionalDimCtx optDim = optionalDimensionMap.get(aliasedDim);
    if (optDim == null) {
      optDim = new OptionalDimCtx();
      optionalDimensionMap.put(aliasedDim, optDim);
    }
    if (cols != null && candidate != null) {
      for (String col : cols) {
        optDim.colQueried.add(col);
      }
      optDim.requiredForCandidates.add(candidate);
    }
    if (cubeCol != null) {
      if (isRef) {
        updateRefColDim(cubeCol, aliasedDim);
      } else {
        updateExprColDim(tableAlias, cubeCol, aliasedDim);
      }
    }
    if (!optDim.isRequiredInJoinChain) {
      optDim.isRequiredInJoinChain = isRequiredInJoin;
    }
    if (log.isDebugEnabled()) {
      log.debug("Adding optional dimension:{} optDim:{} {} isRef:{}", aliasedDim, optDim,
        (cubeCol == null ? "" : " for column:" + cubeCol), isRef);
    }
  }

  public AbstractCubeTable getCubeTableForAlias(String alias) {
    return cubeTbls.get(alias);
  }

  private String getClause() {
    if (clauseName == null) {
      TreeSet<String> ks = new TreeSet<String>(qb.getParseInfo().getClauseNames());
      clauseName = ks.first();
    }
    return clauseName;
  }

  public Map<Dimension, Set<CandidateDim>> getCandidateDimTables() {
    return candidateDims;
  }

  public void addFactPruningMsgs(CubeFactTable fact, CandidateTablePruneCause factPruningMsg) {
    log.info("Pruning fact {} with cause: {}", fact, factPruningMsg);
    factPruningMsgs.addPruningMsg(fact, factPruningMsg);
  }

  public void addDimPruningMsgs(Dimension dim, CubeDimensionTable dimtable, CandidateTablePruneCause msg) {
    PruneCauses<CubeDimensionTable> dimMsgs = dimPruningMsgs.get(dim);
    if (dimMsgs == null) {
      dimMsgs = new PruneCauses<CubeDimensionTable>();
      dimPruningMsgs.put(dim, dimMsgs);
    }
    dimMsgs.addPruningMsg(dimtable, msg);
  }

  public String getAliasForTableName(Named named) {
    return getAliasForTableName(named.getName());
  }

  public String getAliasForTableName(String tableName) {
    for (String alias : qb.getTabAliases()) {
      String table = qb.getTabNameForAlias(alias);
      if (table != null && table.equalsIgnoreCase(tableName)) {
        return alias;
      }
    }
    // get alias from cubeTbls
    for (Map.Entry<String, AbstractCubeTable> cubeTblEntry : cubeTbls.entrySet()) {
      if (cubeTblEntry.getValue().getName().equalsIgnoreCase(tableName)) {
        return cubeTblEntry.getKey();
      }
    }
    return tableName.toLowerCase();
  }

  public void print() {
    if (!log.isDebugEnabled()) {
      return;
    }
    StringBuilder builder = new StringBuilder();
    builder.append("ASTNode:" + ast.dump() + "\n");
    builder.append("QB:");
    builder.append("\n numJoins:" + qb.getNumJoins());
    builder.append("\n numGbys:" + qb.getNumGbys());
    builder.append("\n numSels:" + qb.getNumSels());
    builder.append("\n numSelDis:" + qb.getNumSelDi());
    builder.append("\n aliasToTabs:");
    Set<String> tabAliases = qb.getTabAliases();
    for (String alias : tabAliases) {
      builder.append("\n\t" + alias + ":" + qb.getTabNameForAlias(alias));
    }
    builder.append("\n aliases:");
    for (String alias : qb.getAliases()) {
      builder.append(alias);
      builder.append(", ");
    }
    builder.append("id:" + qb.getId());
    builder.append("isQuery:" + qb.getIsQuery());
    builder.append("\n QBParseInfo");
    QBParseInfo parseInfo = qb.getParseInfo();
    builder.append("\n isSubQ: " + parseInfo.getIsSubQ());
    builder.append("\n alias: " + parseInfo.getAlias());
    if (parseInfo.getJoinExpr() != null) {
      builder.append("\n joinExpr: " + parseInfo.getJoinExpr().dump());
    }
    builder.append("\n hints: " + parseInfo.getHints());
    builder.append("\n aliasToSrc: ");
    for (String alias : tabAliases) {
      builder.append("\n\t" + alias + ": " + parseInfo.getSrcForAlias(alias).dump());
    }
    TreeSet<String> clauses = new TreeSet<String>(parseInfo.getClauseNames());
    for (String clause : clauses) {
      builder.append("\n\t" + clause + ": " + parseInfo.getClauseNamesForDest());
    }
    String clause = clauses.first();
    if (parseInfo.getWhrForClause(clause) != null) {
      builder.append("\n whereexpr: " + parseInfo.getWhrForClause(clause).dump());
    }
    if (parseInfo.getGroupByForClause(clause) != null) {
      builder.append("\n groupby expr: " + parseInfo.getGroupByForClause(clause).dump());
    }
    if (parseInfo.getSelForClause(clause) != null) {
      builder.append("\n sel expr: " + parseInfo.getSelForClause(clause).dump());
    }
    if (parseInfo.getHavingForClause(clause) != null) {
      builder.append("\n having expr: " + parseInfo.getHavingForClause(clause).dump());
    }
    if (parseInfo.getDestLimit(clause) != null) {
      builder.append("\n limit: " + parseInfo.getDestLimit(clause));
    }
    if (parseInfo.getAllExprToColumnAlias() != null && !parseInfo.getAllExprToColumnAlias().isEmpty()) {
      builder.append("\n exprToColumnAlias:");
      for (Map.Entry<ASTNode, String> entry : parseInfo.getAllExprToColumnAlias().entrySet()) {
        builder.append("\n\t expr: " + entry.getKey().dump() + " ColumnAlias: " + entry.getValue());
      }
    }
    if (parseInfo.getAggregationExprsForClause(clause) != null) {
      builder.append("\n aggregateexprs:");
      for (Map.Entry<String, ASTNode> entry : parseInfo.getAggregationExprsForClause(clause).entrySet()) {
        builder.append("\n\t key: " + entry.getKey() + " expr: " + entry.getValue().dump());
      }
    }
    if (parseInfo.getDistinctFuncExprsForClause(clause) != null) {
      builder.append("\n distinctFuncExprs:");
      for (ASTNode entry : parseInfo.getDistinctFuncExprsForClause(clause)) {
        builder.append("\n\t expr: " + entry.dump());
      }
    }

    if (qb.getQbJoinTree() != null) {
      builder.append("\n\n JoinTree");
      QBJoinTree joinTree = qb.getQbJoinTree();
      printJoinTree(joinTree, builder);
    }

    if (qb.getParseInfo().getDestForClause(clause) != null) {
      builder.append("\n Destination:");
      builder.append("\n\t dest expr:" + qb.getParseInfo().getDestForClause(clause).dump());
    }
    log.debug(builder.toString());
  }

  void printJoinTree(QBJoinTree joinTree, StringBuilder builder) {
    builder.append("leftAlias:" + joinTree.getLeftAlias());
    if (joinTree.getLeftAliases() != null) {
      builder.append("\n leftAliases:");
      for (String alias : joinTree.getLeftAliases()) {
        builder.append("\n\t " + alias);
      }
    }
    if (joinTree.getRightAliases() != null) {
      builder.append("\n rightAliases:");
      for (String alias : joinTree.getRightAliases()) {
        builder.append("\n\t " + alias);
      }
    }
    if (joinTree.getJoinSrc() != null) {
      builder.append("\n JoinSrc: {");
      printJoinTree(joinTree.getJoinSrc(), builder);
      builder.append("\n }");
    }
    if (joinTree.getBaseSrc() != null) {
      builder.append("\n baseSrcs:");
      for (String src : joinTree.getBaseSrc()) {
        builder.append("\n\t " + src);
      }
    }
    builder.append("\n noOuterJoin: " + joinTree.getNoOuterJoin());
    builder.append("\n noSemiJoin: " + joinTree.getNoSemiJoin());
    builder.append("\n mapSideJoin: " + joinTree.isMapSideJoin());
    if (joinTree.getJoinCond() != null) {
      builder.append("\n joinConds:");
      for (JoinCond cond : joinTree.getJoinCond()) {
        builder.append("\n\t left: " + cond.getLeft() + " right: " + cond.getRight() + " type:" + cond.getJoinType()
          + " preserved:" + cond.getPreserved());
      }
    }
  }

  void updateFromString(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    fromString = "%s"; // storage string is updated later
    if (isAutoJoinResolved()) {
      fromString =
        getAutoJoinCtx().getFromString(fromString, fact, dimsToQuery.keySet(), dimsToQuery, this, this);
    }
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

  public String getGroupByString() {
    if (groupByAST != null) {
      return HQLParser.getString(groupByAST);
    }
    return null;
  }

  public String getHavingString() {
    if (havingAST != null) {
      return HQLParser.getString(havingAST);
    }
    return null;
  }

  public ASTNode getJoinAST() {
    return qb.getParseInfo().getJoinExpr();
  }

  public String getOrderByString() {
    if (orderByAST != null) {
      return HQLParser.getString(orderByAST);
    }
    return null;
  }

  public Integer getLimitValue() {
    return qb.getParseInfo().getDestLimit(getClause());
  }

  public void setLimitValue(Integer value) {
    qb.getParseInfo().setDestLimit(getClause(), 0, value);
  }

  private String getStorageStringWithAlias(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery, String alias) {
    if (cubeTbls.get(alias) instanceof CubeInterface) {
      return fact.getStorageString(alias);
    } else {
      return dimsToQuery.get(cubeTbls.get(alias)).getStorageString(alias);
    }
  }

  private String getWhereClauseWithAlias(Map<Dimension, CandidateDim> dimsToQuery, String alias) {
    return StorageUtil.getWhereClause(dimsToQuery.get(cubeTbls.get(alias)), alias);
  }

  String getQBFromString(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    String fromString;
    if (getJoinAST() == null) {
      if (cube != null) {
        if (dimensions.size() > 0) {
          throw new LensException(LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE.getLensErrorInfo());
        }
        fromString = fact.getStorageString(getAliasForTableName(cube.getName()));
      } else {
        if (dimensions.size() != 1) {
          throw new LensException(LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE.getLensErrorInfo());
        }
        Dimension dim = dimensions.iterator().next();
        fromString = dimsToQuery.get(dim).getStorageString(getAliasForTableName(dim.getName()));
      }
    } else {
      StringBuilder builder = new StringBuilder();
      getQLString(qb.getQbJoinTree(), builder, fact, dimsToQuery);
      fromString = builder.toString();
    }
    return fromString;
  }

  private void getQLString(QBJoinTree joinTree, StringBuilder builder, CandidateFact fact,
    Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    List<String> joiningTables = new ArrayList<>();
    if (joinTree.getBaseSrc()[0] == null) {
      if (joinTree.getJoinSrc() != null) {
        getQLString(joinTree.getJoinSrc(), builder, fact, dimsToQuery);
      }
    } else { // (joinTree.getBaseSrc()[0] != null){
      String alias = joinTree.getBaseSrc()[0].toLowerCase();
      builder.append(getStorageStringWithAlias(fact, dimsToQuery, alias));
      joiningTables.add(alias);
    }
    if (joinTree.getJoinCond() != null) {
      builder.append(JoinUtils.getJoinTypeStr(joinTree.getJoinCond()[0].getJoinType()));
      builder.append(" JOIN ");
    }
    if (joinTree.getBaseSrc()[1] == null) {
      if (joinTree.getJoinSrc() != null) {
        getQLString(joinTree.getJoinSrc(), builder, fact, dimsToQuery);
      }
    } else { // (joinTree.getBaseSrc()[1] != null){
      String alias = joinTree.getBaseSrc()[1].toLowerCase();
      builder.append(getStorageStringWithAlias(fact, dimsToQuery, alias));
      joiningTables.add(alias);
    }

    String joinCond = joinConds.get(joinTree);
    if (joinCond != null) {
      builder.append(" ON ");
      builder.append(joinCond);
      // joining tables will contain all tables involved in joins.
      // we need to push storage filters for Dimensions into join conditions, thus the following code
      // takes care of the same.
      for (String joiningTable : joiningTables) {
        if (cubeTbls.get(joiningTable) instanceof Dimension) {
          DimOnlyHQLContext.appendWhereClause(builder, getWhereClauseWithAlias(dimsToQuery, joiningTable), true);
          dimsToQuery.get(cubeTbls.get(joiningTable)).setWhereClauseAdded(joiningTable);
        }
      }
    } else {
      throw new LensException(LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE.getLensErrorInfo());
    }
  }

  void setNonexistingParts(Map<String, Set<String>> nonExistingParts) throws LensException {
    if (!nonExistingParts.isEmpty()) {
      ByteArrayOutputStream out = null;
      String partsStr;
      try {
        ObjectMapper mapper = new ObjectMapper();
        out = new ByteArrayOutputStream();
        mapper.writeValue(out, nonExistingParts);
        partsStr = out.toString("UTF-8");
      } catch (Exception e) {
        throw new LensException("Error writing non existing parts", e);
      } finally {
        if (out != null) {
          try {
            out.close();
          } catch (IOException e) {
            throw new LensException(e);
          }
        }
      }
      conf.set(NON_EXISTING_PARTITIONS, partsStr);
    } else {
      conf.unset(NON_EXISTING_PARTITIONS);
    }
  }

  public String getNonExistingParts() {
    return conf.get(NON_EXISTING_PARTITIONS);
  }

  private Map<Dimension, CandidateDim> pickCandidateDimsToQuery(Set<Dimension> dimensions) throws LensException {
    Map<Dimension, CandidateDim> dimsToQuery = new HashMap<Dimension, CandidateDim>();
    if (!dimensions.isEmpty()) {
      for (Dimension dim : dimensions) {
        if (candidateDims.get(dim) != null && candidateDims.get(dim).size() > 0) {
          CandidateDim cdim = candidateDims.get(dim).iterator().next();
          log.info("Available candidate dims are:{}, picking up {} for querying.", candidateDims.get(dim),
            cdim.dimtable);
          dimsToQuery.put(dim, cdim);
        } else {
          String reason = "";
          if (dimPruningMsgs.get(dim) != null && !dimPruningMsgs.get(dim).isEmpty()) {
            ByteArrayOutputStream out = null;
            try {
              ObjectMapper mapper = new ObjectMapper();
              out = new ByteArrayOutputStream();
              mapper.writeValue(out, dimPruningMsgs.get(dim).getJsonObject());
              reason = out.toString("UTF-8");
            } catch (Exception e) {
              throw new LensException("Error writing dim pruning messages", e);
            } finally {
              if (out != null) {
                try {
                  out.close();
                } catch (IOException e) {
                  throw new LensException(e);
                }
              }
            }
          }
          log.error("Query rewrite failed due to NO_CANDIDATE_DIM_AVAILABLE, Cause {}",
            dimPruningMsgs.get(dim).toJsonObject());
          throw new NoCandidateDimAvailableException(dimPruningMsgs.get(dim));
        }
      }
    }
    return dimsToQuery;
  }

  private Set<CandidateFact> pickCandidateFactToQuery() throws LensException {
    Set<CandidateFact> facts = null;
    if (hasCubeInQuery()) {
      if (candidateFactSets.size() > 0) {
        facts = candidateFactSets.iterator().next();
        log.info("Available candidate facts:{}, picking up {} for querying", candidateFactSets, facts);
      } else {
        String reason = "";
        if (!factPruningMsgs.isEmpty()) {
          ByteArrayOutputStream out = null;
          try {
            ObjectMapper mapper = new ObjectMapper();
            out = new ByteArrayOutputStream();
            mapper.writeValue(out, factPruningMsgs.getJsonObject());
            reason = out.toString("UTF-8");
          } catch (Exception e) {
            throw new LensException("Error writing fact pruning messages", e);
          } finally {
            if (out != null) {
              try {
                out.close();
              } catch (IOException e) {
                throw new LensException(e);
              }
            }
          }
        }
        log.error("Query rewrite failed due to NO_CANDIDATE_FACT_AVAILABLE, Cause {}", factPruningMsgs.toJsonObject());
        throw new NoCandidateFactAvailableException(factPruningMsgs);
      }
    }
    return facts;
  }

  private HQLContextInterface hqlContext;
  @Getter
  private Collection<CandidateFact> pickedFacts;
  @Getter
  private Collection<CandidateDim> pickedDimTables;

  private void addRangeClauses(CandidateFact fact) throws LensException {
    if (fact != null) {
      // resolve timerange positions and replace it by corresponding where clause
      for (TimeRange range : getTimeRanges()) {
        for (Map.Entry<String, String> entry : fact.getRangeToStorageWhereMap().get(range).entrySet()) {
          String table = entry.getKey();
          String rangeWhere = entry.getValue();
          if (!StringUtils.isBlank(rangeWhere)) {
            ASTNode rangeAST = HQLParser.parseExpr(rangeWhere, conf);
            range.getParent().setChild(range.getChildIndex(), rangeAST);
          }
          fact.getStorgeWhereClauseMap().put(table, HQLParser.parseExpr(getWhereString(), conf));
        }
      }
    }
  }

  public String toHQL() throws LensException {
    Set<CandidateFact> cfacts = pickCandidateFactToQuery();
    Map<Dimension, CandidateDim> dimsToQuery = pickCandidateDimsToQuery(dimensions);
    log.info("facts:{}, dimsToQuery: {}", cfacts, dimsToQuery);
    if (autoJoinCtx != null) {
      // prune join paths for picked fact and dimensions
      autoJoinCtx.pruneAllPaths(cube, cfacts, dimsToQuery);
    }

    Map<CandidateFact, Set<Dimension>> factDimMap = new HashMap<>();
    if (cfacts != null) {
      if (cfacts.size() > 1) {
        // copy ASTs for each fact
        for (CandidateFact cfact : cfacts) {
          cfact.copyASTs(this);
          factDimMap.put(cfact, new HashSet<>(dimsToQuery.keySet()));
        }
      }
      for (CandidateFact fact : cfacts) {
        addRangeClauses(fact);
      }
    }

    // pick dimension tables required during expression expansion for the picked fact and dimensions
    Set<Dimension> exprDimensions = new HashSet<>();
    if (cfacts != null) {
      for (CandidateFact cfact : cfacts) {
        Set<Dimension> factExprDimTables = exprCtx.rewriteExprCtx(this, cfact, dimsToQuery,
          cfacts.size() > 1 ? cfact : this);
        exprDimensions.addAll(factExprDimTables);
        if (cfacts.size() > 1) {
          factDimMap.get(cfact).addAll(factExprDimTables);
        }
      }
      if (cfacts.size() > 1) {
        havingAST = MultiFactHQLContext.pushDownHaving(havingAST, this, cfacts);
      }
    } else {
      // dim only query
      exprDimensions.addAll(exprCtx.rewriteExprCtx(this, null, dimsToQuery, this));
    }
    dimsToQuery.putAll(pickCandidateDimsToQuery(exprDimensions));
    log.info("facts:{}, dimsToQuery: {}", cfacts, dimsToQuery);

    // pick denorm tables for the picked fact and dimensions
    Set<Dimension> denormTables = new HashSet<>();
    if (cfacts != null) {
      for (CandidateFact cfact : cfacts) {
        Set<Dimension> factDenormTables = deNormCtx.rewriteDenormctx(this, cfact, dimsToQuery, cfacts.size() > 1);
        denormTables.addAll(factDenormTables);
        if (cfacts.size() > 1) {
          factDimMap.get(cfact).addAll(factDenormTables);
        }
      }
    } else {
      denormTables.addAll(deNormCtx.rewriteDenormctx(this, null, dimsToQuery, false));
    }
    dimsToQuery.putAll(pickCandidateDimsToQuery(denormTables));
    log.info("facts:{}, dimsToQuery: {}", cfacts, dimsToQuery);
    // Prune join paths once denorm tables are picked
    if (autoJoinCtx != null) {
      // prune join paths for picked fact and dimensions
      autoJoinCtx.pruneAllPaths(cube, cfacts, dimsToQuery);
    }
    if (autoJoinCtx != null) {
      // add optional dims from Join resolver
      Set<Dimension> joiningTables = new HashSet<>();
      if (cfacts != null && cfacts.size() > 1) {
        for (CandidateFact cfact : cfacts) {
          Set<Dimension> factJoiningTables = autoJoinCtx.pickOptionalTables(cfact, factDimMap.get(cfact), this);
          factDimMap.get(cfact).addAll(factJoiningTables);
          joiningTables.addAll(factJoiningTables);
        }
      } else {
        joiningTables.addAll(autoJoinCtx.pickOptionalTables(null, dimsToQuery.keySet(), this));
      }
      dimsToQuery.putAll(pickCandidateDimsToQuery(joiningTables));
    }
    log.info("Picked Fact:{} dimsToQuery: {}", cfacts, dimsToQuery);
    pickedDimTables = dimsToQuery.values();
    pickedFacts = cfacts;
    if (cfacts != null) {
      if (cfacts.size() > 1) {
        // Update ASTs for each fact
        for (CandidateFact cfact : cfacts) {
          cfact.updateASTs(this);
        }
        whereAST = MultiFactHQLContext.convertHavingToWhere(havingAST, this, cfacts, new DefaultAliasDecider());
        for (CandidateFact cFact : cfacts) {
          cFact.updateFromString(this, factDimMap.get(cFact), dimsToQuery);
        }
      }
    }
    if (cfacts == null || cfacts.size() == 1) {
      updateFromString(cfacts == null ? null : cfacts.iterator().next(), dimsToQuery);
    }
    //update dim filter with fact filter
    if (cfacts != null && cfacts.size() > 0) {
      for (CandidateFact cfact : cfacts) {
        if (!cfact.getStorageTables().isEmpty()) {
          for (String qualifiedStorageTable : cfact.getStorageTables()) {
            String storageTable = qualifiedStorageTable.substring(qualifiedStorageTable.indexOf(".") + 1);
            String where = getWhere(cfact, autoJoinCtx,
                cfact.getStorageWhereClause(storageTable), getAliasForTableName(cfact.getBaseTable().getName()),
                shouldReplaceDimFilterWithFactFilter(), storageTable, dimsToQuery);
            cfact.getStorgeWhereStringMap().put(storageTable, where);
          }
        }
      }
    }
    hqlContext = createHQLContext(cfacts, dimsToQuery, factDimMap);
    return hqlContext.toHQL();
  }

  private HQLContextInterface createHQLContext(Set<CandidateFact> facts, Map<Dimension, CandidateDim> dimsToQuery,
    Map<CandidateFact, Set<Dimension>> factDimMap) throws LensException {
    if (facts == null || facts.size() == 0) {
      return new DimOnlyHQLContext(dimsToQuery, this, this);
    } else if (facts.size() == 1 && facts.iterator().next().getStorageTables().size() > 1) {
      //create single fact with multiple storage context
      return new SingleFactMultiStorageHQLContext(facts.iterator().next(), dimsToQuery, this, this);
    } else if (facts.size() == 1 && facts.iterator().next().getStorageTables().size() == 1) {
      CandidateFact fact = facts.iterator().next();
      // create single fact context
      return new SingleFactSingleStorageHQLContext(fact, null,
        dimsToQuery, this, DefaultQueryAST.fromCandidateFact(fact, fact.getStorageTables().iterator().next(), this));
    } else {
      return new MultiFactHQLContext(facts, dimsToQuery, factDimMap, this);
    }
  }

  public ASTNode toAST(Context ctx) throws LensException {
    String hql = toHQL();
    ParseDriver pd = new ParseDriver();
    ASTNode tree;
    try {
      log.info("HQL:{}", hql);
      tree = pd.parse(hql, ctx);
    } catch (ParseException e) {
      throw new LensException(e);
    }
    return ParseUtils.findRootNonNullToken(tree);
  }

  public Set<String> getColumnsQueriedForTable(String tblName) {
    return getColumnsQueried(getAliasForTableName(tblName));
  }

  public void addColumnsQueriedWithTimeDimCheck(QueriedPhraseContext qur, String alias, String timeDimColumn) {

    if (!shouldReplaceTimeDimWithPart()) {
      qur.addColumnsQueried(alias, timeDimColumn);
    }
  }

  public boolean isCubeMeasure(String col) {
    if (col == null) {
      return false;
    }

    col = col.trim();
    // Take care of brackets added around col names in HQLParsrer.getString
    if (col.startsWith("(") && col.endsWith(")") && col.length() > 2) {
      col = col.substring(1, col.length() - 1);
    }

    String[] split = StringUtils.split(col, ".");
    if (split.length <= 1) {
      col = col.trim().toLowerCase();
      if (queriedExprs.contains(col)) {
        return exprCtx.getExpressionContext(col, getAliasForTableName(cube.getName())).hasMeasures();
      } else {
        return cube.getMeasureNames().contains(col);
      }
    } else {
      String cubeName = split[0].trim().toLowerCase();
      String colName = split[1].trim().toLowerCase();
      if (cubeName.equalsIgnoreCase(cube.getName()) || cubeName.equals(getAliasForTableName(cube.getName()))) {
        if (queriedExprs.contains(colName)) {
          return exprCtx.getExpressionContext(colName, cubeName).hasMeasures();
        } else {
          return cube.getMeasureNames().contains(colName.toLowerCase());
        }
      } else {
        return false;
      }
    }
  }

  boolean isCubeMeasure(ASTNode node) {
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

    return isCubeMeasure(msrname);
  }

  public boolean hasAggregates() {
    if (getExprCtx().hasAggregates()) {
      return true;
    }
    for (QueriedPhraseContext qur : queriedPhrases) {
      if (qur.isAggregate()) {
        return true;
      }
    }
    return false;
  }

  public void setJoinCond(QBJoinTree qb, String cond) {
    joinConds.put(qb, cond);
  }

  public AbstractCubeTable getQueriedTable(String alias) {
    if (cube != null && cube.getName().equalsIgnoreCase(qb.getTabNameForAlias((alias)))) {
      return (AbstractCubeTable) cube;
    }
    for (Dimension dim : dimensions) {
      if (dim.getName().equalsIgnoreCase(qb.getTabNameForAlias(alias))) {
        return dim;
      }
    }
    return null;
  }

  public String getInsertClause() {
    ASTNode destTree = qb.getParseInfo().getDestForClause(clauseName);
    if (destTree != null && ((ASTNode) (destTree.getChild(0))).getToken().getType() != TOK_TMP_FILE) {
      return "INSERT OVERWRITE" + HQLParser.getString(destTree);
    }
    return "";
  }

  public Set<Aliased<Dimension>> getOptionalDimensions() {
    return optionalDimensionMap.keySet();
  }

  /**
   * @return the hqlContext
   */
  public HQLContextInterface getHqlContext() {
    return hqlContext;
  }

  public boolean shouldReplaceTimeDimWithPart() {
    return getConf().getBoolean(REPLACE_TIMEDIM_WITH_PART_COL, DEFAULT_REPLACE_TIMEDIM_WITH_PART_COL);
  }

  public boolean shouldReplaceDimFilterWithFactFilter() {
    return getConf().getBoolean(REWRITE_DIM_FILTER_TO_FACT_FILTER, DEFAULT_REWRITE_DIM_FILTER_TO_FACT_FILTER);
  }

  public String getPartitionColumnOfTimeDim(String timeDimName) {
    return getPartitionColumnOfTimeDim(cube, timeDimName);
  }

  public static String getPartitionColumnOfTimeDim(CubeInterface cube, String timeDimName) {
    if (cube == null) {
      return timeDimName;
    }
    if (cube instanceof DerivedCube) {
      return ((DerivedCube) cube).getParent().getPartitionColumnOfTimeDim(timeDimName);
    } else {
      return ((Cube) cube).getPartitionColumnOfTimeDim(timeDimName);
    }
  }

  public String getTimeDimOfPartitionColumn(String partCol) {
    return getTimeDimOfPartitionColumn(cube, partCol);
  }

  public static String getTimeDimOfPartitionColumn(CubeInterface cube, String partCol) {
    if (cube == null) {
      return partCol;
    }
    if (cube instanceof DerivedCube) {
      return ((DerivedCube) cube).getParent().getTimeDimOfPartitionColumn(partCol);
    } else {
      return ((Cube) cube).getTimeDimOfPartitionColumn(partCol);
    }
  }

  public void addQueriedMsrs(Set<String> msrs) {
    queriedMsrs.addAll(msrs);
  }

  public void addQueriedExprs(Set<String> exprs) {
    queriedExprs.addAll(exprs);
  }

  public void addQueriedExprsWithMeasures(Set<String> exprs) {
    queriedExprsWithMeasures.addAll(exprs);
  }

  /**
   * Prune candidate fact sets with respect to available candidate facts.
   * <p></p>
   * Prune a candidate set, if any of the fact is missing.
   *
   * @param pruneCause
   */
  public void pruneCandidateFactSet(CandidateTablePruneCode pruneCause) {
    // remove candidate fact sets that have missing facts
    for (Iterator<Set<CandidateFact>> i = candidateFactSets.iterator(); i.hasNext();) {
      Set<CandidateFact> cfacts = i.next();
      if (!candidateFacts.containsAll(cfacts)) {
        log.info("Not considering fact table set:{} as they have non candidate tables and facts missing because of {}",
          cfacts, pruneCause);
        i.remove();
      }
    }
    // prune candidate facts
    pruneCandidateFactWithCandidateSet(CandidateTablePruneCode.ELEMENT_IN_SET_PRUNED);
  }

  /**
   * Prune candidate fact with respect to available candidate fact sets.
   * <p></p>
   * If candidate fact is not present in any of the candidate fact sets, remove it.
   *
   * @param pruneCause
   */
  public void pruneCandidateFactWithCandidateSet(CandidateTablePruneCode pruneCause) {
    // remove candidate facts that are not part of any covering set
    pruneCandidateFactWithCandidateSet(new CandidateTablePruneCause(pruneCause));
  }

  public void pruneCandidateFactWithCandidateSet(CandidateTablePruneCause pruneCause) {
    // remove candidate facts that are not part of any covering set
    Set<CandidateFact> allCoveringFacts = new HashSet<CandidateFact>();
    for (Set<CandidateFact> set : candidateFactSets) {
      allCoveringFacts.addAll(set);
    }
    for (Iterator<CandidateFact> i = candidateFacts.iterator(); i.hasNext();) {
      CandidateFact cfact = i.next();
      if (!allCoveringFacts.contains(cfact)) {
        log.info("Not considering fact table:{} as {}", cfact, pruneCause);
        addFactPruningMsgs(cfact.fact, pruneCause);
        i.remove();
      }
    }
  }

  public void addQueriedTimeDimensionCols(final String timeDimColName) {

    checkArgument(StringUtils.isNotBlank(timeDimColName));
    this.queriedTimeDimCols.add(timeDimColName);
  }

  public ImmutableSet<String> getQueriedTimeDimCols() {
    return ImmutableSet.copyOf(this.queriedTimeDimCols);
  }

  private String getWhere(CandidateFact cfact, AutoJoinContext autoJoinCtx,
                          ASTNode node, String cubeAlias,
                          boolean shouldReplaceDimFilter, String storageTable,
                          Map<Dimension, CandidateDim> dimToQuery) throws LensException {
    String whereString;
    if (autoJoinCtx != null && shouldReplaceDimFilter) {
      List<String> allfilters = new ArrayList<>();
      getAllFilters(node, cubeAlias, allfilters, autoJoinCtx.getJoinClause(cfact), dimToQuery);
      whereString = StringUtils.join(allfilters, " and ");
    } else {
      whereString = HQLParser.getString(cfact.getStorageWhereClause(storageTable));
    }
    return whereString;
  }

  private List<String> getAllFilters(ASTNode node, String cubeAlias, List<String> allFilters,
                                    JoinClause joinClause,  Map<Dimension, CandidateDim> dimToQuery)
    throws LensException {

    if (node.getToken().getType() == HiveParser.KW_AND) {
      // left child is and
      if (node.getChild(0).getType() == HiveParser.KW_AND) {
        // take right corresponding to right
        String table = getTableFromFilterAST((ASTNode) node.getChild(1));
        allFilters.add(getFilter(table, cubeAlias, node, joinClause, 1, dimToQuery));
      } else if (node.getChildCount() > 1) {
        for (int i = 0; i < node.getChildCount(); i++) {
          String table = getTableFromFilterAST((ASTNode) node.getChild(i));
          allFilters.add(getFilter(table, cubeAlias, node, joinClause, i, dimToQuery));
        }
      }
    } else if (node.getParent() == null
        && node.getToken().getType() != HiveParser.KW_AND) {
      // if node is the only child
      allFilters.add(HQLParser.getString((ASTNode) node));
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      getAllFilters(child, cubeAlias, allFilters, joinClause, dimToQuery);
    }
    return allFilters;
  }

  private String getFilter(String table, String cubeAlias, ASTNode node,  JoinClause joinClause,
                           int index,  Map<Dimension, CandidateDim> dimToQuery)
    throws LensException{
    String filter;
    if (table != null && !table.equals(cubeAlias) && getStarJoin(joinClause, table) != null) {
      //rewrite dim filter to fact filter if its a star join with fact
      filter = buildFactSubqueryFromDimFilter(getStarJoin(joinClause, table),
          (ASTNode) node.getChild(index), table, dimToQuery, cubeAlias);
    } else {
      filter = HQLParser.getString((ASTNode) node.getChild(index));
    }
    return filter;
  }

  private TableRelationship getStarJoin(JoinClause joinClause, String table) {
    TableRelationship rel;
    for (Map.Entry<TableRelationship, JoinTree>  entry : joinClause.getJoinTree().getSubtrees().entrySet()) {
      if (entry.getValue().getDepthFromRoot() == 1 && table.equals(entry.getValue().getAlias())) {
        return entry.getKey();
      }
    }
    return null;
  }

  private String getTableFromFilterAST(ASTNode node) {

    if (node.getToken().getType() == HiveParser.DOT) {
      return HQLParser.findNodeByPath((ASTNode) node,
          TOK_TABLE_OR_COL, Identifier).getText();
    } else {
      // recurse down
      for (int i = 0; i < node.getChildCount(); i++) {
        ASTNode child = (ASTNode) node.getChild(i);
        String ret = getTableFromFilterAST(child);
        if (ret != null) {
          return ret;
        }
      }
    }
    return null;
  }

  private String buildFactSubqueryFromDimFilter(TableRelationship tabRelation, ASTNode dimFilter,
                                                String dimAlias, Map<Dimension, CandidateDim> dimToQuery,
                                                String cubeAlias)
    throws LensException {
    StringBuilder builder = new StringBuilder();
    String storageClause = dimToQuery.get(tabRelation.getToTable()).getWhereClause();

    builder.append(cubeAlias)
        .append(".")
        .append(tabRelation.getFromColumn())
        .append(" in ( ")
        .append("select ")
        .append(tabRelation.getToColumn())
        .append(" from ")
        .append(dimToQuery.get(tabRelation.getToTable()).getStorageString(dimAlias))
        .append(" where ")
        .append(HQLParser.getString((ASTNode) dimFilter));
    if (storageClause != null) {
      builder.append(" and ")
          .append(String.format(storageClause, dimAlias))
          .append(" ) ");
    } else {
      builder.append(" ) ");
    }

    return builder.toString();
  }

}
