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

import static org.apache.lens.cube.parse.CubeQueryConfUtil.*;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.error.NoCandidateDimAvailableException;
import org.apache.lens.cube.error.NoCandidateFactAvailableException;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.join.TableRelationship;
import org.apache.lens.cube.parse.join.AutoJoinContext;
import org.apache.lens.cube.parse.join.JoinClause;
import org.apache.lens.cube.parse.join.JoinUtils;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.util.ReflectionUtils;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CubeQueryContext extends TracksQueriedColumns implements QueryAST, TrackDenormContext {
  static final String TIME_RANGE_FUNC = "time_range_in";
  public static final String NOW = "now";
  static final String DEFAULT_TABLE = "_default_";
  @Getter
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

  /**
   * This is the set of working Candidates that gets updated during different phases of
   * query resolution. Each {@link ContextRewriter} may add/remove/update Candiadtes in
   * this working set and from the final set of Candidates single {@link #pickedCandidate}
   * is chosen.
   */
  @Getter
  private final Set<Candidate> candidates = new HashSet<>();

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
    return selectPhrases.stream().map(SelectPhraseContext::getActualAlias).anyMatch(Predicate.isEqual(col));
  }

  void addQueriedPhrase(QueriedPhraseContext qur) {
    queriedPhrases.add(qur);
    qur.setPosition(queriedPhrases.size() -1);
  }

  @Getter
  private final List<SelectPhraseContext> selectPhrases = new ArrayList<>();

  @Getter
  private final List<QueriedPhraseContext> queriedPhrases = new ArrayList<>();

  // Join conditions used in all join expressions
  @Getter
  private final Map<QBJoinTree, String> joinConds = new HashMap<>();
  @Getter
  protected final Map<Dimension, Set<CandidateDim>> candidateDims = new HashMap<>();
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
  private PruneCauses<Candidate>  storagePruningMsgs = new PruneCauses<>();
  @Getter
  private Map<Dimension, PruneCauses<CubeDimensionTable>> dimPruningMsgs =
    new HashMap<Dimension, PruneCauses<CubeDimensionTable>>();
  @Setter
  @Getter
  private String fromString;
  @Getter
  private TimeRangeWriter rangeWriter = null;
  public CubeQueryContext(ASTNode ast, QB qb, Configuration queryConf, HiveConf metastoreConf)
    throws LensException {
    this.ast = ast;
    this.qb = qb;
    this.conf = queryConf;
    this.clauseName = getClause();
    this.timeRanges = new ArrayList<>();
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

    this.rangeWriter = ReflectionUtils.newInstance(conf.getClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS,
      CubeQueryConfUtil.DEFAULT_TIME_RANGE_WRITER, TimeRangeWriter.class), conf);
  }

  boolean hasCubeInQuery() {
    return cube != null;
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

    return false;
  }

  boolean addQueriedTable(String alias) throws LensException {
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
      return false;
    }
    return true;
  }

  boolean isAutoJoinResolved() {
    return autoJoinCtx != null && autoJoinCtx.isJoinsResolved();
  }

  Cube getBaseCube() {
    return cube instanceof Cube ? (Cube) cube : ((DerivedCube) cube).getParent();
  }

  Set<String> getPartitionColumnsQueried() {
    return getTimeRanges().stream().map(TimeRange::getPartitionColumn).collect(toSet());
  }

  // map of ref column in query to set of Dimension that have the column - which are added as optional dims
  @Getter
  private Map<String, Set<Aliased<Dimension>>> refColToDim = Maps.newHashMap();

  private void updateRefColDim(String col, Aliased<Dimension> dim) {
    refColToDim.computeIfAbsent(col.toLowerCase(), k -> Sets.newHashSet()).add(dim);
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

  private void updateExprColDim(String tblAlias, String col, Aliased<Dimension> dim) {
    exprColToDim.computeIfAbsent(new QueriedExprColumn(col, tblAlias), k -> Sets.newHashSet()).add(dim);
  }

  // Holds the context of optional dimension
  // A dimension is optional if it is not queried directly by the user, but is
  // required by a candidate table to get a denormalized field from reference
  // or required in a join chain
  @ToString
  static class OptionalDimCtx {
    OptionalDimCtx() {
    }

    Set<String> colQueried = new HashSet<String>();
    Set<CandidateTable> requiredForCandidates = new HashSet<CandidateTable>();
    boolean isRequiredInJoinChain = false;
  }

  void addOptionalJoinDimTable(String alias, boolean isRequired) throws LensException {
    addOptionalDimTable(alias, null, isRequired, null, false, (String[]) null);
  }

  void addOptionalExprDimTable(String dimAlias, String queriedExpr, String srcTableAlias,
    CandidateTable candidate, String... cols) throws LensException {
    addOptionalDimTable(dimAlias, candidate, false, queriedExpr, false, srcTableAlias, cols);
  }

  void addOptionalDimTable(String alias, CandidateTable candidate, boolean isRequiredInJoin, String cubeCol,
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
    OptionalDimCtx optDim = optionalDimensionMap.computeIfAbsent(aliasedDim, k -> new OptionalDimCtx());
    if (cols != null && candidate != null) {
      optDim.colQueried.addAll(Arrays.asList(cols));
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

  void addCandidatePruningMsg(Collection<Candidate> candidateCollection, CandidateTablePruneCause pruneCause) {
    for (Candidate c : candidateCollection){
      addCandidatePruningMsg(c, pruneCause);
    }
  }

  void addCandidatePruningMsg(Candidate cand, CandidateTablePruneCause pruneCause) {
    if (cand instanceof SegmentationCandidate) {
      addStoragePruningMsg(cand, pruneCause);
    } else {
      Set<StorageCandidate> scs = CandidateUtil.getStorageCandidates(cand);
      for (StorageCandidate sc : scs) {
        addStoragePruningMsg(sc, pruneCause);
      }
    }
  }

  void addStoragePruningMsg(Candidate sc, CandidateTablePruneCause... factPruningMsgs) {
    for (CandidateTablePruneCause factPruningMsg: factPruningMsgs) {
      log.info("Pruning Storage {} with cause: {}", sc, factPruningMsg);
      storagePruningMsgs.addPruningMsg(sc, factPruningMsg);
    }
  }

  public void addDimPruningMsgs(Dimension dim, CubeDimensionTable dimtable, CandidateTablePruneCause msg) {
    dimPruningMsgs.computeIfAbsent(dim, k -> new PruneCauses<>()).addPruningMsg(dimtable, msg);
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
    StringBuilder builder = new StringBuilder()
      .append("ASTNode:").append(ast.dump()).append("\n")
      .append("QB:")
      .append("\n numJoins:").append(qb.getNumJoins())
      .append("\n numGbys:").append(qb.getNumGbys())
      .append("\n numSels:").append(qb.getNumSels())
      .append("\n numSelDis:").append(qb.getNumSelDi())
      .append("\n aliasToTabs:");
    Set<String> tabAliases = qb.getTabAliases();
    for (String alias : tabAliases) {
      builder.append("\n\t").append(alias).append(":").append(qb.getTabNameForAlias(alias));
    }
    builder.append("\n aliases:");
    for (String alias : qb.getAliases()) {
      builder.append(alias);
      builder.append(", ");
    }
    builder
      .append("id:").append(qb.getId())
      .append("isQuery:").append(qb.getIsQuery())
      .append("\n QBParseInfo");
    QBParseInfo parseInfo = qb.getParseInfo();
    builder
      .append("\n isSubQ: ").append(parseInfo.getIsSubQ())
      .append("\n alias: ").append(parseInfo.getAlias());
    if (parseInfo.getJoinExpr() != null) {
      builder.append("\n joinExpr: ").append(parseInfo.getJoinExpr().dump());
    }
    builder.append("\n hints: ").append(parseInfo.getHints());
    builder.append("\n aliasToSrc: ");
    for (String alias : tabAliases) {
      builder.append("\n\t").append(alias).append(": ").append(parseInfo.getSrcForAlias(alias).dump());
    }
    TreeSet<String> clauses = new TreeSet<String>(parseInfo.getClauseNames());
    for (String clause : clauses) {
      builder.append("\n\t").append(clause).append(": ").append(parseInfo.getClauseNamesForDest());
    }
    String clause = clauses.first();
    if (parseInfo.getWhrForClause(clause) != null) {
      builder.append("\n whereexpr: ").append(parseInfo.getWhrForClause(clause).dump());
    }
    if (parseInfo.getGroupByForClause(clause) != null) {
      builder.append("\n groupby expr: ").append(parseInfo.getGroupByForClause(clause).dump());
    }
    if (parseInfo.getSelForClause(clause) != null) {
      builder.append("\n sel expr: ").append(parseInfo.getSelForClause(clause).dump());
    }
    if (parseInfo.getHavingForClause(clause) != null) {
      builder.append("\n having expr: ").append(parseInfo.getHavingForClause(clause).dump());
    }
    if (parseInfo.getDestLimit(clause) != null) {
      builder.append("\n limit: ").append(parseInfo.getDestLimit(clause));
    }
    if (parseInfo.getAllExprToColumnAlias() != null && !parseInfo.getAllExprToColumnAlias().isEmpty()) {
      builder.append("\n exprToColumnAlias:");
      for (Map.Entry<ASTNode, String> entry : parseInfo.getAllExprToColumnAlias().entrySet()) {
        builder.append("\n\t expr: ").append(entry.getKey().dump()).append(" ColumnAlias: ").append(entry.getValue());
      }
    }
    if (parseInfo.getAggregationExprsForClause(clause) != null) {
      builder.append("\n aggregateexprs:");
      for (Map.Entry<String, ASTNode> entry : parseInfo.getAggregationExprsForClause(clause).entrySet()) {
        builder.append("\n\t key: ").append(entry.getKey()).append(" expr: ").append(entry.getValue().dump());
      }
    }
    if (parseInfo.getDistinctFuncExprsForClause(clause) != null) {
      builder.append("\n distinctFuncExprs:");
      for (ASTNode entry : parseInfo.getDistinctFuncExprsForClause(clause)) {
        builder.append("\n\t expr: ").append(entry.dump());
      }
    }

    if (qb.getQbJoinTree() != null) {
      builder.append("\n\n JoinTree");
      QBJoinTree joinTree = qb.getQbJoinTree();
      printJoinTree(joinTree, builder);
    }

    if (qb.getParseInfo().getDestForClause(clause) != null) {
      builder.append("\n Destination:")
        .append("\n\t dest expr:").append(qb.getParseInfo().getDestForClause(clause).dump());
    }
    log.debug(builder.toString());
  }

  private void printJoinTree(QBJoinTree joinTree, StringBuilder builder) {
    builder.append("leftAlias:").append(joinTree.getLeftAlias());
    if (joinTree.getLeftAliases() != null) {
      builder.append("\n leftAliases:");
      for (String alias : joinTree.getLeftAliases()) {
        builder.append("\n\t ").append(alias);
      }
    }
    if (joinTree.getRightAliases() != null) {
      builder.append("\n rightAliases:");
      for (String alias : joinTree.getRightAliases()) {
        builder.append("\n\t ").append(alias);
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
        builder.append("\n\t ").append(src);
      }
    }
    builder.append("\n noOuterJoin: ").append(joinTree.getNoOuterJoin());
    builder.append("\n noSemiJoin: ").append(joinTree.getNoSemiJoin());
    builder.append("\n mapSideJoin: ").append(joinTree.isMapSideJoin());
    if (joinTree.getJoinCond() != null) {
      builder.append("\n joinConds:");
      for (JoinCond cond : joinTree.getJoinCond()) {
        builder.append("\n\t left: ").append(cond.getLeft())
          .append(" right: ").append(cond.getRight())
          .append(" type:").append(cond.getJoinType())
          .append(" preserved:").append(cond.getPreserved());
      }
    }
  }

  public String getSelectString() {
    return HQLParser.getString(selectAST);
  }


  public void setWhereString(String whereString) {
    //NO OP
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

  @Override
  public void setJoinAST(ASTNode node) {
    //NO-OP
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

  private String getStorageStringWithAlias(StorageCandidate candidate, Map<Dimension,
      CandidateDim> dimsToQuery, String alias) {
    if (cubeTbls.get(alias) instanceof CubeInterface) {
      return candidate.getAliasForTable(alias);
    } else {
      return dimsToQuery.get(cubeTbls.get(alias)).getStorageString(alias);
    }
  }

  private String getWhereClauseWithAlias(Map<Dimension, CandidateDim> dimsToQuery, String alias) {
    return StorageUtil.getWhereClause(dimsToQuery.get(cubeTbls.get(alias)), alias);
  }

  String getQBFromString(StorageCandidate candidate, Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    String fromString;
    if (getJoinAST() == null) {
      if (candidate != null) {
        if (dimensions.size() > 0) {
          throw new LensException(LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE.getLensErrorInfo());
        }
        fromString = candidate.getAliasForTable(getAliasForTableName(cube.getName()));
      } else {
        if (dimensions.size() != 1) {
          throw new LensException(LensCubeErrorCode.NO_JOIN_CONDITION_AVAILABLE.getLensErrorInfo());
        }
        Dimension dim = dimensions.iterator().next();
        fromString = dimsToQuery.get(dim).getStorageString(getAliasForTableName(dim.getName()));
      }
    } else {
      StringBuilder builder = new StringBuilder();
      getQLString(qb.getQbJoinTree(), builder, candidate, dimsToQuery);
      fromString = builder.toString();
    }
    return fromString;
  }


  private void getQLString(QBJoinTree joinTree, StringBuilder builder, StorageCandidate candidate,
    Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    List<String> joiningTables = new ArrayList<>();
    if (joinTree.getBaseSrc()[0] == null) {
      if (joinTree.getJoinSrc() != null) {
        getQLString(joinTree.getJoinSrc(), builder, candidate, dimsToQuery);
      }
    } else { // (joinTree.getBaseSrc()[0] != null){
      String alias = joinTree.getBaseSrc()[0].toLowerCase();
      builder.append(getStorageStringWithAlias(candidate, dimsToQuery, alias));
      joiningTables.add(alias);
    }
    if (joinTree.getJoinCond() != null) {
      builder.append(JoinUtils.getJoinTypeStr(joinTree.getJoinCond()[0].getJoinType()));
      builder.append(" JOIN ");
    }
    if (joinTree.getBaseSrc()[1] == null) {
      if (joinTree.getJoinSrc() != null) {
        getQLString(joinTree.getJoinSrc(), builder, candidate, dimsToQuery);
      }
    } else { // (joinTree.getBaseSrc()[1] != null){
      String alias = joinTree.getBaseSrc()[1].toLowerCase();
      builder.append(getStorageStringWithAlias(candidate, dimsToQuery, alias));
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
      ObjectMapper mapper = new ObjectMapper();
      try (ByteArrayOutputStream out = new ByteArrayOutputStream();){
        mapper.writeValue(out, nonExistingParts);
        conf.set(NON_EXISTING_PARTITIONS, out.toString("UTF-8"));
      } catch (Exception e) {
        throw new LensException("Error writing non existing parts", e);
      }
    } else {
      conf.unset(NON_EXISTING_PARTITIONS);
    }
  }

  String getNonExistingParts() {
    return conf.get(NON_EXISTING_PARTITIONS);
  }

  Map<Dimension, CandidateDim> pickCandidateDimsToQuery(Set<Dimension> dimensions) throws LensException {
    Map<Dimension, CandidateDim> dimsToQuery = new HashMap<Dimension, CandidateDim>();
    if (!dimensions.isEmpty()) {
      for (Dimension dim : dimensions) {
        if (candidateDims.get(dim) != null && candidateDims.get(dim).size() > 0) {
          CandidateDim cdim = candidateDims.get(dim).iterator().next();
          log.info("Available candidate dims are:{}, picking up {} for querying.", candidateDims.get(dim),
            cdim.dimtable);
          dimsToQuery.put(dim, cdim);
        } else {
          if (dimPruningMsgs.get(dim) != null && !dimPruningMsgs.get(dim).isEmpty()) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
              ObjectMapper mapper = new ObjectMapper();
              mapper.writeValue(out, dimPruningMsgs.get(dim).getJsonObject());
              log.info("No candidate dim found because: {}", out.toString("UTF-8"));
            } catch (Exception e) {
              throw new LensException("Error writing dim pruning messages", e);
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

  Candidate pickCandidateToQuery() throws LensException {
    Candidate cand;
    if (hasCubeInQuery()) {
      Iterator<Candidate> iter = candidates.iterator();
      if (pickedCandidate == null && iter.hasNext()) {
        cand = iter.next();
        log.info("Available Candidates:{}, picking up Candidate: {} for querying", candidates, cand);
        pickedCandidate = cand;
        // Answerable common measures in JoinCandidate should be answered by one of the children, otherwise
        // measure numbers will be added multiple times in the final union query.
        Set<Integer> measureIndices = getQueriedPhrases().stream().filter(x -> x.hasMeasures(this))
          .map(QueriedPhraseContext::getPosition).collect(toSet());
        pickedCandidate.decideMeasurePhrasesToAnswer(measureIndices);
      }
      if (pickedCandidate == null) {
        throwNoCandidateFactException();
      }
    }
    return pickedCandidate;
  }
  void throwNoCandidateFactException() throws LensException {
    log.error("Query rewrite failed due to NO_CANDIDATE_FACT_AVAILABLE, Cause {}", storagePruningMsgs.toJsonString());
    throw new NoCandidateFactAvailableException(this);
  }
  @Getter
  private QueryWriterContext queryWriterContext;
  private QueryWriter queryWriter;

  @Getter
  private Candidate pickedCandidate;
  @Getter
  private Collection<CandidateDim> pickedDimTables;

  void addRangeClauses(StorageCandidateHQLContext sc) throws LensException {
    if (sc != null) {
      // resolve timerange positions and replace it by corresponding where clause
      for (TimeRange range : getTimeRanges()) {
        String rangeWhere = sc.getStorageCandidate().getTimeRangeWhereClasue(rangeWriter, range);
        if (!StringUtils.isBlank(rangeWhere)) {
          ASTNode updatedRangeAST = HQLParser.parseExpr(rangeWhere, conf);
          updateTimeRangeNode(sc.getQueryAst().getWhereAST(), range.getAstNode(), updatedRangeAST);
        }
      }
    }
  }


  /**
   * Find the appropriate time range node in the AST and update it with "updatedTimeRange".
   * Time Range node looks like this
   * time_range_in(dt, '2017', '2018') ->
   * TOK_FUNCTION [TOK_FUNCTION] (l5c2p37) {
   * time_range_in [Identifier] (l6c1p37)$
   * TOK_TABLE_OR_COL [TOK_TABLE_OR_COL] (l6c2p51) {
   * dt [Identifier] (l7c1p51)$
   * }
   * '2017' [StringLiteral] (l6c3p55)$
   * '2018' [StringLiteral] (l6c4p63)$
   }
   * @param root
   * @param timeRangeFuncNode
   * @param updatedTimeRange
   */
  private void updateTimeRangeNode(ASTNode root, ASTNode timeRangeFuncNode, ASTNode updatedTimeRange) {
    ASTNode childNode;
    if (root.getChildCount() == 0) {
      return;
    }
    for (Node child : root.getChildren()) {
      childNode = (ASTNode) child;
      if (childNode.getType() == timeRangeFuncNode.getType()
        && childNode.getChildCount() == timeRangeFuncNode.getChildCount()
        && childNode.getChild(0).getText().equalsIgnoreCase(timeRangeFuncNode.getChild(0).getText())) {
        //Found the "time_range_in" function node. Check the details further as there can be more than one time ranges
        if (HQLParser.getString(timeRangeFuncNode).equalsIgnoreCase(HQLParser.getString(childNode))) {
          //This is the correct time range node . Replace it with "updatedTimeRange"
          childNode.getParent().setChild(childNode.getChildIndex(), updatedTimeRange);
          return;
        }
      }
      updateTimeRangeNode(childNode, timeRangeFuncNode, updatedTimeRange);
    }
  }

  public QueryWriterContext getQueryWriterContext(Candidate cand, Map<Dimension, CandidateDim> dimsToQuery)
    throws LensException {
    if (cand == null) {
      return new DimOnlyHQLContext(dimsToQuery, this);
    } else {
      return cand.toQueryWriterContext(dimsToQuery, this);
    }
  }

  QueryWriter getQueryWriter() throws LensException {
    if (queryWriter == null) {
      Candidate cand = pickCandidateToQuery();
      Map<Dimension, CandidateDim> dimsToQuery = pickCandidateDimsToQuery(dimensions);
      log.info("Candidate: {}, DimsToQuery: {}", cand, dimsToQuery);
      queryWriterContext = getQueryWriterContext(cand, dimsToQuery);

      if (cand != null && autoJoinCtx != null) {
        // prune join paths for picked fact and dimensions
        autoJoinCtx.pruneAllPaths(cube, cand.getColumns(), dimsToQuery);
      }
      // pick dimension tables required during expression expansion for the picked fact and dimensions
      queryWriterContext.addExpressionDims();
      // pick denorm tables for the picked fact and dimensions
      queryWriterContext.addDenormDims();
      // Prune join paths once denorm tables are picked
      if (cand != null && autoJoinCtx != null) {
        // prune join paths for picked fact and dimensions
        autoJoinCtx.pruneAllPaths(cube, cand.getColumns(), dimsToQuery);
      }
      queryWriterContext.addAutoJoinDims();
      pickedDimTables = dimsToQuery.values();

      //Set From string and time range clause
      queryWriterContext.updateFromString();
      //update dim filter with fact filter
      queryWriterContext.updateDimFilterWithFactFilter();
      queryWriter = queryWriterContext.toQueryWriter();
    }
    return queryWriter;
  }
  private String asHQL = null;
  public String toHQL() throws LensException {
    if (asHQL == null) {
      asHQL = getQueryWriter().toHQL();
    }
    return asHQL;
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

  Set<String> getColumnsQueriedForTable(String tblName) {
    return getColumnsQueried(getAliasForTableName(tblName));
  }

  void addColumnsQueriedWithTimeDimCheck(QueriedPhraseContext qur, String alias, String timeDimColumn) {

    if (!shouldReplaceTimeDimWithPart()) {
      qur.addColumnsQueried(alias, timeDimColumn);
    }
  }

  boolean isCubeMeasure(String col) {
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
      assert tabident != null;
      tabname = tabident.getText();
    }

    String msrname = StringUtils.isBlank(tabname) ? colname : tabname + "." + colname;

    return isCubeMeasure(msrname);
  }

  boolean hasAggregates() {
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

  void setJoinCond(QBJoinTree qb, String cond) {
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

  String getInsertClause() {
    ASTNode destTree = qb.getParseInfo().getDestForClause(clauseName);
    if (destTree != null && ((ASTNode) (destTree.getChild(0))).getToken().getType() != TOK_TMP_FILE) {
      return "INSERT OVERWRITE " + HQLParser.getString(destTree) + " ";
    }
    return "";
  }

  Set<Aliased<Dimension>> getOptionalDimensions() {
    return optionalDimensionMap.keySet();
  }

  public boolean shouldReplaceTimeDimWithPart() {
    return getConf().getBoolean(REPLACE_TIMEDIM_WITH_PART_COL, DEFAULT_REPLACE_TIMEDIM_WITH_PART_COL);
  }

  boolean shouldReplaceDimFilterWithFactFilter() {
    return getConf().getBoolean(REWRITE_DIM_FILTER_TO_FACT_FILTER, DEFAULT_REWRITE_DIM_FILTER_TO_FACT_FILTER);
  }

  String getPartitionColumnOfTimeDim(String timeDimName) {
    return getPartitionColumnOfTimeDim(cube, timeDimName);
  }

  private static String getPartitionColumnOfTimeDim(CubeInterface cube, String timeDimName) {
    if (cube == null) {
      return timeDimName;
    }
    if (cube instanceof DerivedCube) {
      return ((DerivedCube) cube).getParent().getPartitionColumnOfTimeDim(timeDimName);
    } else {
      return ((Cube) cube).getPartitionColumnOfTimeDim(timeDimName);
    }
  }

  String getTimeDimOfPartitionColumn(String partCol) {
    return getTimeDimOfPartitionColumn(cube, partCol);
  }

  private static String getTimeDimOfPartitionColumn(CubeInterface cube, String partCol) {
    if (cube == null) {
      return partCol;
    }
    if (cube instanceof DerivedCube) {
      return ((DerivedCube) cube).getParent().getTimeDimOfPartitionColumn(partCol);
    } else {
      return ((Cube) cube).getTimeDimOfPartitionColumn(partCol);
    }
  }

  void addQueriedMsrs(Set<String> msrs) {
    queriedMsrs.addAll(msrs);
  }

  void addQueriedExprs(Set<String> exprs) {
    queriedExprs.addAll(exprs);
  }

  void addQueriedExprsWithMeasures(Set<String> exprs) {
    queriedExprsWithMeasures.addAll(exprs);
  }

  void addQueriedTimeDimensionCols(final String timeDimColName) {

    checkArgument(StringUtils.isNotBlank(timeDimColName));
    this.queriedTimeDimCols.add(timeDimColName);
  }

  ImmutableSet<String> getQueriedTimeDimCols() {
    return ImmutableSet.copyOf(this.queriedTimeDimCols);
  }

  String getWhere(StorageCandidateHQLContext sc, AutoJoinContext autoJoinCtx, String cubeAlias,
    boolean shouldReplaceDimFilter, Map<Dimension, CandidateDim> dimToQuery) throws LensException {
    String whereString;
    if (autoJoinCtx != null && shouldReplaceDimFilter) {
      List<String> allfilters = new ArrayList<>();
      getAllFilters(sc.getQueryAst().getWhereAST(), cubeAlias, allfilters,
        autoJoinCtx.getJoinClause(sc.getStorageCandidate()), dimToQuery);
      whereString = StringUtils.join(allfilters, " and ");
    } else {
      whereString = HQLParser.getString(sc.getQueryAst().getWhereAST());
    }
    return whereString;
  }

  protected static void getAllFilters(ASTNode node, String cubeAlias, List<String> allFilters, JoinClause joinClause,
    Map<Dimension, CandidateDim> dimToQuery) throws LensException {
    if (node.getToken().getType() == HiveParser.KW_AND || node.getToken().getType() == HiveParser.TOK_WHERE) {
      for (int i = 0; i < node.getChildCount(); i++) {
        ASTNode child = (ASTNode) node.getChild(i);
        getAllFilters(child, cubeAlias, allFilters, joinClause, dimToQuery);
      }
    } else {
      String table = getTableFromFilterAST(node);
      allFilters.add(getFilter(table, cubeAlias, node, joinClause, dimToQuery));
    }
  }

  private static String getFilter(String table, String cubeAlias, ASTNode node,  JoinClause joinClause,
                           Map<Dimension, CandidateDim> dimToQuery)
    throws LensException{
    String filter;
    if (table != null && !table.equals(cubeAlias) && joinClause.getStarJoin(table) != null) {
      //rewrite dim filter to fact filter if its a star join with fact
      filter = buildFactSubqueryFromDimFilter(joinClause.getStarJoin(table), node, table, dimToQuery, cubeAlias);
    } else {
      filter = HQLParser.getString(node);
    }
    return filter;
  }

  private static String getTableFromFilterAST(ASTNode node) {

    if (node.getToken().getType() == HiveParser.DOT) {
      ASTNode n = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL, Identifier);
      assert n != null;
      return n.getText();
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

  private static String buildFactSubqueryFromDimFilter(TableRelationship tabRelation, ASTNode dimFilter,
                                                String dimAlias, Map<Dimension, CandidateDim> dimToQuery,
                                                String cubeAlias)
    throws LensException {
    StringBuilder builder = new StringBuilder();
    CandidateDim dim = dimToQuery.get(tabRelation.getToTable());
    String storageClause = dim.getWhereClause();

    builder.append(cubeAlias)
        .append(".")
        .append(tabRelation.getFromColumn())
        .append(" in ( ")
        .append("select ")
        .append(tabRelation.getToColumn())
        .append(" from ")
        .append(dim.getStorageString(dimAlias))
        .append(" where ")
        .append(HQLParser.getString(dimFilter));
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
