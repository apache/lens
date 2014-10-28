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

import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TMP_FILE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.JoinCond;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CubeTableCause;
import org.codehaus.jackson.map.ObjectMapper;

public class CubeQueryContext {
  public static final String TIME_RANGE_FUNC = "time_range_in";
  public static final String NOW = "now";
  public static final String DEFAULT_TABLE = "_default_";
  public static Log LOG = LogFactory.getLog(CubeQueryContext.class.getName());
  private final ASTNode ast;
  private final QB qb;
  private String clauseName = null;
  private final HiveConf conf;

  private final List<TimeRange> timeRanges;

  // metadata
  private CubeInterface cube;
  // Dimensions accessed in the query
  protected Set<Dimension> dimensions = new HashSet<Dimension>();
  private final Set<String> queriedDimAttrs = new HashSet<String>();
  private final Set<String> queriedMsrs = new HashSet<String>();
  private final Set<Set<CandidateFact>> candidateFactSets = new HashSet<Set<CandidateFact>>();

  // would be added through join chains and de-normalized resolver
  protected Map<Dimension, OptionalDimCtx> optionalDimensions = new HashMap<Dimension, OptionalDimCtx>();

  // Alias to table object mapping of tables accessed in this query
  private final Map<String, AbstractCubeTable> cubeTbls = new HashMap<String, AbstractCubeTable>();
  // Alias name to fields queried
  private final Map<String, Set<String>> tblAliasToColumns = new HashMap<String, Set<String>>();
  // Mapping of an expression to its column alias in the query
  private final Map<String, String> exprToAlias = new HashMap<String, String>();
  private final List<String> selectAliases = new ArrayList<String>();
  private final List<String> selectFinalAliases = new ArrayList<String>();
  // All aggregate expressions in the query
  private final Set<String> aggregateExprs = new HashSet<String>();
  // Join conditions used in all join expressions
  private final Map<QBJoinTree, String> joinConds = new HashMap<QBJoinTree, String>();

  // storage specific
  protected final Set<CandidateFact> candidateFacts = new HashSet<CandidateFact>();
  protected final Map<Dimension, Set<CandidateDim>> candidateDims = new HashMap<Dimension, Set<CandidateDim>>();

  // query trees
  private ASTNode havingAST;
  private ASTNode selectAST;
  private ASTNode whereAST;
  private ASTNode orderByAST;
  private ASTNode groupByAST;
  private CubeMetastoreClient client;
  private JoinResolver.AutoJoinContext autoJoinCtx;
  private DenormalizationResolver.DenormalizationContext deNormCtx;
  private Map<CubeFactTable, List<CandidateTablePruneCause>> factPruningMsgs =
      new HashMap<CubeFactTable, List<CandidateTablePruneCause>>();
  private Map<Dimension, Map<CubeDimensionTable, List<CandidateTablePruneCause>>> dimPruningMsgs =
      new HashMap<Dimension, Map<CubeDimensionTable, List<CandidateTablePruneCause>>>();

  public CubeQueryContext(ASTNode ast, QB qb, HiveConf conf) throws SemanticException {
    this.ast = ast;
    this.qb = qb;
    this.conf = conf;
    this.clauseName = getClause();
    this.timeRanges = new ArrayList<TimeRange>();
    try {
      client = CubeMetastoreClient.getInstance(conf);
    } catch (HiveException e) {
      throw new SemanticException(e);
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

    for (ASTNode aggrTree : qb.getParseInfo().getAggregationExprsForClause(clauseName).values()) {
      String aggr = HQLParser.getString(aggrTree);
      aggregateExprs.add(aggr);
    }

    extractMetaTables();
  }

  public boolean hasCubeInQuery() {
    return cube != null;
  }

  public boolean hasDimensionInQuery() {
    return dimensions != null && !dimensions.isEmpty();
  }

  private void extractMetaTables() throws SemanticException {
    try {
      List<String> tabAliases = new ArrayList<String>(qb.getTabAliases());
      for (String alias : tabAliases) {
        addQueriedTable(alias);
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  public void addQueriedTable(String alias) throws SemanticException {
    alias = alias.toLowerCase();
    if (cubeTbls.containsKey(alias)) {
      return;
    }
    String tblName = qb.getTabNameForAlias(alias);
    if (tblName == null) {
      tblName = alias;
    }
    try {
      if (client.isCube(tblName)) {
        if (cube != null) {
          if (!cube.getName().equalsIgnoreCase(tblName)) {
            throw new SemanticException(ErrorMsg.MORE_THAN_ONE_CUBE, cube.getName(), tblName);
          }
        }
        cube = client.getCube(tblName);
        cubeTbls.put(alias, (AbstractCubeTable) cube);
      } else if (client.isDimension(tblName)) {
        Dimension dim = client.getDimension(tblName);
        dimensions.add(dim);
        cubeTbls.put(alias, dim);
      } else {
        throw new SemanticException(ErrorMsg.NEITHER_CUBE_NOR_DIMENSION);
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  // Holds the context of optional dimension
  // A dimension is optional if it is not queried directly by the user, but is
  // required by a candidate table to get a denormalized field from reference
  // or required in a join chain
  static class OptionalDimCtx {
    OptionalDimCtx() {
    }

    Set<String> colQueried = new HashSet<String>();
    Set<CandidateTable> requiredForCandidates = new HashSet<CandidateTable>();
    boolean isRequiredInJoinChain = false;
  }

  public void addOptionalDimTable(String alias, String col, CandidateTable candidate, boolean isRequiredInJoin)
      throws SemanticException {
    alias = alias.toLowerCase();
    String tblName = qb.getTabNameForAlias(alias);
    if (tblName == null) {
      tblName = alias;
    }
    try {
      LOG.info("Adding optional dimension:" + alias);
      if (client.isDimension(tblName)) {
        Dimension dim;
        if (cubeTbls.containsKey(alias)) {
          dim = (Dimension) cubeTbls.get(alias);
        } else {
          dim = client.getDimension(tblName);
          cubeTbls.put(alias, dim);
        }
        OptionalDimCtx optDim = optionalDimensions.get(dim);
        if (optDim == null) {
          optDim = new OptionalDimCtx();
          optionalDimensions.put(dim, optDim);
        }
        if (col != null && candidate != null) {
          optDim.colQueried.add(col);
          optDim.requiredForCandidates.add(candidate);
        }
        if (!optDim.isRequiredInJoinChain) {
          optDim.isRequiredInJoinChain = isRequiredInJoin;
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
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

  public CubeInterface getCube() {
    return cube;
  }

  public QB getQB() {
    return qb;
  }

  public Set<CandidateFact> getCandidateFactTables() {
    return candidateFacts;
  }

  public Map<Dimension, Set<CandidateDim>> getCandidateDimTables() {
    return candidateDims;
  }

  public Map<CubeFactTable, List<CandidateTablePruneCause>> getFactPruningMsgs() {
    return factPruningMsgs;
  }

  public void addFactPruningMsgs(CubeFactTable fact, CandidateTablePruneCause factPruningMsg) {
    List<CandidateTablePruneCause> pruneMsgs = factPruningMsgs.get(fact);
    if (pruneMsgs == null) {
      pruneMsgs = new ArrayList<CandidateTablePruneCause>();
      factPruningMsgs.put(fact, pruneMsgs);
    }
    pruneMsgs.add(factPruningMsg);
  }

  public void addDimPruningMsgs(Dimension dim, CubeDimensionTable dimtable, CandidateTablePruneCause msg) {
    Map<CubeDimensionTable, List<CandidateTablePruneCause>> dimMsgs = dimPruningMsgs.get(dimtable);
    if (dimMsgs == null) {
      dimMsgs = new HashMap<CubeDimensionTable, List<CandidateTablePruneCause>>();
      dimPruningMsgs.put(dim, dimMsgs);
    }
    List<CandidateTablePruneCause> pruneMsgs = dimMsgs.get(dimtable);
    if (pruneMsgs == null) {
      pruneMsgs = new ArrayList<CandidateTablePruneCause>();
      dimMsgs.put(dimtable, pruneMsgs);
    }
    pruneMsgs.add(msg);
  }

  public Set<Dimension> getDimensions() {
    return dimensions;
  }

  public String getAliasForTabName(String tabName) {
    for (String alias : qb.getTabAliases()) {
      String table = qb.getTabNameForAlias(alias);
      if (table != null && table.equalsIgnoreCase(tabName)) {
        return alias;
      }
    }
    return tabName;
  }

  public void print() {
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
    LOG.info(builder.toString());
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

  public String getSelectTree() {
    return HQLParser.getString(selectAST);
  }

  public String getWhereTree() {
    if (whereAST != null) {
      return HQLParser.getString(whereAST);
    }
    return null;
  }

  // Will be set after the Fact is picked and time ranges replaced
  public void setWhereAST(ASTNode ast) {
    this.whereAST = ast;
  }

  public String getGroupByTree() {
    if (groupByAST != null) {
      return HQLParser.getString(groupByAST);
    }
    return null;
  }

  public String getHavingTree() {
    if (havingAST != null) {
      return HQLParser.getString(havingAST);
    }
    return null;
  }

  public ASTNode getJoinTree() {
    return qb.getParseInfo().getJoinExpr();
  }

  public QBJoinTree getQBJoinTree() {
    return qb.getQbJoinTree();
  }

  public String getOrderByTree() {
    if (orderByAST != null) {
      return HQLParser.getString(orderByAST);
    }
    return null;
  }

  public Integer getLimitValue() {
    return qb.getParseInfo().getDestLimit(getClause());
  }

  private String getStorageStringWithAlias(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery, String alias) {
    if (cubeTbls.get(alias) instanceof CubeInterface) {
      return fact.getStorageString(alias);
    } else {
      return dimsToQuery.get(cubeTbls.get(alias)).getStorageString(alias);
    }
  }

  private String getWhereClauseWithAlias(Map<Dimension, CandidateDim> dimsToQuery, String alias) {
    Dimension dim = (Dimension) cubeTbls.get(alias);
    return dimsToQuery.get(dim).whereClause.replace(getAliasForTabName(dim.getName()), alias);
  }

  String getQBFromString(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery) throws SemanticException {
    String fromString = null;
    if (getJoinTree() == null) {
      if (cube != null) {
        fromString = fact.getStorageString(getAliasForTabName(cube.getName()));
      } else {
        if (dimensions.size() != 1) {
          throw new SemanticException(ErrorMsg.NO_JOIN_CONDITION_AVAIABLE);
        }
        Dimension dim = dimensions.iterator().next();
        fromString = dimsToQuery.get(dim).getStorageString(getAliasForTabName(dim.getName()));
      }
    } else {
      StringBuilder builder = new StringBuilder();
      getQLString(qb.getQbJoinTree(), builder, fact, dimsToQuery);
      fromString = builder.toString();
    }
    return fromString;
  }

  private void getQLString(QBJoinTree joinTree, StringBuilder builder, CandidateFact fact,
      Map<Dimension, CandidateDim> dimsToQuery) throws SemanticException {
    String joiningTable = null;
    if (joinTree.getBaseSrc()[0] == null) {
      if (joinTree.getJoinSrc() != null) {
        getQLString(joinTree.getJoinSrc(), builder, fact, dimsToQuery);
      }
    } else { // (joinTree.getBaseSrc()[0] != null){
      String alias = joinTree.getBaseSrc()[0].toLowerCase();
      builder.append(getStorageStringWithAlias(fact, dimsToQuery, alias));
      if (joinTree.getJoinCond()[0].getJoinType().equals(JoinType.RIGHTOUTER)) {
        joiningTable = alias;
      }
    }
    if (joinTree.getJoinCond() != null) {
      builder.append(JoinResolver.getJoinTypeStr(joinTree.getJoinCond()[0].getJoinType()));
      builder.append(" JOIN ");
    }
    if (joinTree.getBaseSrc()[1] == null) {
      if (joinTree.getJoinSrc() != null) {
        getQLString(joinTree.getJoinSrc(), builder, fact, dimsToQuery);
      }
    } else { // (joinTree.getBaseSrc()[1] != null){
      String alias = joinTree.getBaseSrc()[1].toLowerCase();
      builder.append(getStorageStringWithAlias(fact, dimsToQuery, alias));
      if (joinTree.getJoinCond()[0].getJoinType().equals(JoinType.LEFTOUTER)) {
        joiningTable = alias;
      }
    }

    String joinCond = joinConds.get(joinTree);
    if (joinCond != null) {
      builder.append(" ON ");
      builder.append(joinCond);
      if (joiningTable != null) {
        // assuming the joining table to be dimension table
        DimOnlyHQLContext.appendWhereClause(builder, getWhereClauseWithAlias(dimsToQuery, joiningTable), true);
        dimsToQuery.get(cubeTbls.get(joiningTable)).setWhereClauseAdded();
      }
    } else {
      throw new SemanticException(ErrorMsg.NO_JOIN_CONDITION_AVAIABLE);
    }
  }

  void setNonexistingParts(Map<String, List<String>> nonExistingParts) throws SemanticException {
    if (!nonExistingParts.isEmpty()) {
      ByteArrayOutputStream out = null;
      String partsStr;
      try {
        ObjectMapper mapper = new ObjectMapper();
        out = new ByteArrayOutputStream();
        mapper.writeValue(out, nonExistingParts);
        partsStr = out.toString("UTF-8");
      } catch (Exception e) {
        throw new SemanticException("Error writing non existing parts", e);
      } finally {
        if (out != null) {
          try {
            out.close();
          } catch (IOException e) {
            throw new SemanticException(e);
          }
        }
      }
      conf.set(CubeQueryConfUtil.NON_EXISTING_PARTITIONS, partsStr);
    } else {
      conf.unset(CubeQueryConfUtil.NON_EXISTING_PARTITIONS);
    }
  }

  public String getNonExistingParts() {
    return conf.get(CubeQueryConfUtil.NON_EXISTING_PARTITIONS);
  }

  private Map<Dimension, CandidateDim> pickCandidateDimsToQuery(Set<Dimension> dimensions) throws SemanticException {
    Map<Dimension, CandidateDim> dimsToQuery = new HashMap<Dimension, CandidateDim>();
    if (!dimensions.isEmpty()) {
      for (Dimension dim : dimensions) {
        if (candidateDims.get(dim) != null && candidateDims.get(dim).size() > 0) {
          CandidateDim cdim = candidateDims.get(dim).iterator().next();
          LOG.info("Available candidate dims are:" + candidateDims.get(dim) + ", picking up " + cdim.dimtable
              + " for querying");
          dimsToQuery.put(dim, cdim);
        } else {
          String reason = "";
          if (dimPruningMsgs.get(dim) != null && !dimPruningMsgs.get(dim).isEmpty()) {
            ByteArrayOutputStream out = null;
            try {
              ObjectMapper mapper = new ObjectMapper();
              out = new ByteArrayOutputStream();
              mapper.writeValue(out, dimPruningMsgs.get(dim).values());
              reason = out.toString("UTF-8");
            } catch (Exception e) {
              throw new SemanticException("Error writing dim pruning messages", e);
            } finally {
              if (out != null) {
                try {
                  out.close();
                } catch (IOException e) {
                  throw new SemanticException(e);
                }
              }
            }
          }
          throw new SemanticException(ErrorMsg.NO_CANDIDATE_DIM_AVAILABLE, dim.getName(), reason);
        }
      }
    }

    return dimsToQuery;
  }

  private Set<CandidateFact> pickCandidateFactToQuery() throws SemanticException {
    Set<CandidateFact> facts = null;
    if (hasCubeInQuery()) {
      if (candidateFactSets.size() > 0) {
        facts = candidateFactSets.iterator().next();
        LOG.info("Available candidate facts:" + candidateFactSets + ", picking up " + facts + " for querying");
      } else {
        String reason = "";
        if (!factPruningMsgs.isEmpty()) {
          ByteArrayOutputStream out = null;
          try {
            ObjectMapper mapper = new ObjectMapper();
            out = new ByteArrayOutputStream();
            mapper.writeValue(out, factPruningMsgs.values());
            reason = out.toString("UTF-8");
          } catch (Exception e) {
            throw new SemanticException("Error writing fact pruning messages", e);
          } finally {
            if (out != null) {
              try {
                out.close();
              } catch (IOException e) {
                throw new SemanticException(e);
              }
            }
          }
        }
        throw new SemanticException(ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE, reason);
      }
    }

    return facts;
  }

  private HQLContextInterface hqlContext;

  public String toHQL() throws SemanticException {
    Set<CandidateFact> cfacts = pickCandidateFactToQuery();
    Map<Dimension, CandidateDim> dimsToQuery = pickCandidateDimsToQuery(dimensions);
    if (autoJoinCtx != null) {
      // prune join paths for picked fact and dimensions
      autoJoinCtx.pruneAllPaths(cube, cfacts, dimsToQuery);
    }

    Map<CandidateFact, Set<Dimension>> factDimMap = new HashMap<CandidateFact, Set<Dimension>>();
    if (cfacts != null) {
      if (cfacts.size() > 1) {
        // copy ASTs for each fact
        for (CandidateFact cfact : cfacts) {
          cfact.copyASTs(this);
          cfact.updateTimeranges(this);
          factDimMap.put(cfact, new HashSet<Dimension>(dimsToQuery.keySet()));
        }
      } else {
        SingleFactHQLContext.addRangeClauses(this, cfacts.iterator().next());
      }
    }

    // pick denorm tables for the picked fact and dimensions
    Set<Dimension> denormTables = new HashSet<Dimension>();
    if (cfacts != null) {
      for (CandidateFact cfact : cfacts) {
        Set<Dimension> factDenormTables = deNormCtx.rewriteDenormctx(cfact, dimsToQuery, cfacts.size() > 1);
        denormTables.addAll(factDenormTables);
        if (cfacts.size() > 1) {
          factDimMap.get(cfact).addAll(factDenormTables);
        }
      }
    } else {
      denormTables.addAll(deNormCtx.rewriteDenormctx(null, dimsToQuery, false));
    }
    if (dimsToQuery == null) {
      dimsToQuery = pickCandidateDimsToQuery(denormTables);
    } else {
      dimsToQuery.putAll(pickCandidateDimsToQuery(denormTables));
    }
    if (autoJoinCtx != null) {
      // add optional dims from Join resolver
      Set<Dimension> joiningTables = autoJoinCtx.pickOptionalTables(dimsToQuery, this);
      if (cfacts != null && cfacts.size() > 1) {
        // copy tables for each fact
        for (CandidateFact cfact : cfacts) {
          factDimMap.get(cfact).addAll(joiningTables);
        }
      }
      if (dimsToQuery == null) {
        dimsToQuery = pickCandidateDimsToQuery(joiningTables);
      } else {
        dimsToQuery.putAll(pickCandidateDimsToQuery(joiningTables));
      }
    }
    // Prune join paths once denorm tables and joining tables are picked
    if (autoJoinCtx != null) {
      // prune join paths for picked fact and dimensions
      autoJoinCtx.pruneAllPaths(cube, cfacts, dimsToQuery);
    }
    LOG.info("Picked Fact:" + cfacts + " dimsToQuery:" + dimsToQuery);

    if (cfacts != null) {
      if (cfacts.size() > 1) {
        // Update ASTs for each fact
        for (CandidateFact cfact : cfacts) {
          cfact.updateASTs(this);
        }
      }
    }
    hqlContext = createHQLContext(cfacts, dimsToQuery, factDimMap, this);
    return hqlContext.toHQL();
  }

  private HQLContextInterface createHQLContext(Set<CandidateFact> facts, Map<Dimension, CandidateDim> dimsToQuery,
      Map<CandidateFact, Set<Dimension>> factDimMap, CubeQueryContext query) throws SemanticException {
    if (facts == null || facts.size() == 0) {
      return new DimOnlyHQLContext(dimsToQuery, query);
    } else if (facts.size() == 1) {
      // create singlefact context
      return new SingleFactHQLContext(facts.iterator().next(), dimsToQuery, query);
    } else {
      return new MultiFactHQLContext(facts, dimsToQuery, factDimMap, query);
    }
  }

  public ASTNode toAST(Context ctx) throws SemanticException {
    String hql = toHQL();
    ParseDriver pd = new ParseDriver();
    ASTNode tree;
    try {
      LOG.info("HQL:" + hql);
      System.out.println("Rewritten HQL:" + hql);
      tree = pd.parse(hql, ctx);
    } catch (ParseException e) {
      throw new SemanticException(e);
    }
    return ParseUtils.findRootNonNullToken(tree);
  }

  public Map<String, Set<String>> getTblAlaisToColumns() {
    return tblAliasToColumns;
  }

  public Set<String> getColumnsQueried(String tblName) {
    return tblAliasToColumns.get(getAliasForTabName(tblName));
  }

  public void addColumnsQueried(AbstractCubeTable table, String column) {
    addColumnsQueried(getAliasForTabName(table.getName()), column);
  }

  public void addColumnsQueried(String alias, String column) {
    Set<String> cols = tblAliasToColumns.get(alias.toLowerCase());
    if (cols == null) {
      cols = new HashSet<String>();
      tblAliasToColumns.put(alias.toLowerCase(), cols);
    }
    cols.add(column);
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
      return cube.getMeasureNames().contains(col.trim().toLowerCase());
    } else {
      String cubeName = split[0].trim();
      String colName = split[1].trim();
      if (cubeName.equalsIgnoreCase(cube.getName()) || cubeName.equalsIgnoreCase(getAliasForTabName(cube.getName()))) {
        return cube.getMeasureNames().contains(colName.toLowerCase());
      } else {
        return false;
      }
    }
  }

  public boolean isAggregateExpr(String expr) {
    return aggregateExprs.contains(expr == null ? expr : expr.toLowerCase());
  }

  public boolean hasAggregates() {
    return !aggregateExprs.isEmpty();
  }

  public String getAlias(String expr) {
    return exprToAlias.get(expr);
  }

  public String getSelectAlias(int index) {
    return selectAliases.get(index);
  }

  public String getSelectFinalAlias(int index) {
    return selectFinalAliases.get(index);
  }

  public Map<String, String> getExprToAliasMap() {
    return exprToAlias;
  }

  public void addAggregateExpr(String expr) {
    aggregateExprs.add(expr);
  }

  public Set<String> getAggregateExprs() {
    return aggregateExprs;
  }

  public ASTNode getHavingAST() {
    return havingAST;
  }

  public ASTNode getSelectAST() {
    return selectAST;
  }

  public Map<QBJoinTree, String> getJoinConds() {
    return joinConds;
  }

  public void setJoinCond(QBJoinTree qb, String cond) {
    joinConds.put(qb, cond);
  }

  public ASTNode getWhereAST() {
    return whereAST;
  }

  public ASTNode getOrderByAST() {
    return orderByAST;
  }

  public ASTNode getGroupByAST() {
    return groupByAST;
  }

  public void setGroupByAST(ASTNode ast) {
    this.groupByAST = ast;
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
    String insertString = "";
    ASTNode destTree = qb.getParseInfo().getDestForClause(clauseName);
    if (destTree != null && ((ASTNode) (destTree.getChild(0))).getToken().getType() != TOK_TMP_FILE) {
      insertString = "INSERT OVERWRITE" + HQLParser.getString(qb.getParseInfo().getDestForClause(clauseName));
    }
    return insertString;
  }

  public CubeMetastoreClient getMetastoreClient() {
    return client;
  }

  public void addExprToAlias(ASTNode expr, ASTNode alias) {
    exprToAlias.put(HQLParser.getString(expr).trim(), alias.getText().toLowerCase());
  }

  public void addSelectAlias(String alias, String spacedAlias) {
    selectAliases.add(alias);
    if (!StringUtils.isBlank(spacedAlias)) {
      selectFinalAliases.add("`" + spacedAlias + "`");
    } else {
      selectFinalAliases.add(alias);
    }
  }

  public List<TimeRange> getTimeRanges() {
    return timeRanges;
  }

  public HiveConf getHiveConf() {
    return conf;
  }

  public void setAutoJoinCtx(JoinResolver.AutoJoinContext autoJoinCtx) {
    this.autoJoinCtx = autoJoinCtx;
  }

  public JoinResolver.AutoJoinContext getAutoJoinCtx() {
    return autoJoinCtx;
  }

  public void setDenormCtx(DenormalizationResolver.DenormalizationContext deNormCtx) {
    this.deNormCtx = deNormCtx;
  }

  public DenormalizationResolver.DenormalizationContext getDenormCtx() {
    return this.deNormCtx;
  }

  public Set<Dimension> getOptionalDimensions() {
    return optionalDimensions.keySet();
  }

  public Map<Dimension, OptionalDimCtx> getOptionalDimensionMap() {
    return optionalDimensions;
  }

  /**
   * @return the hqlContext
   */
  public HQLContextInterface getHqlContext() {
    return hqlContext;
  }

  public boolean shouldReplaceTimeDimWithPart() {
    return getHiveConf().getBoolean(CubeQueryConfUtil.REPLACE_TIMEDIM_WITH_PART_COL,
        CubeQueryConfUtil.DEFAULT_REPLACE_TIMEDIM_WITH_PART_COL);
  }

  public String getPartitionColumnOfTimeDim(String timeDimName) {
    if (!hasCubeInQuery()) {
      return timeDimName;
    }

    CubeInterface cube = getCube();
    if (cube instanceof DerivedCube) {
      return ((DerivedCube) cube).getParent().getPartitionColumnOfTimeDim(timeDimName);
    } else {
      return ((Cube) cube).getPartitionColumnOfTimeDim(timeDimName);
    }
  }

  public String getTimeDimOfPartitionColumn(String partCol) {
    if (!hasCubeInQuery()) {
      return partCol;
    }

    CubeInterface cube = getCube();
    if (cube instanceof DerivedCube) {
      return ((DerivedCube) cube).getParent().getTimeDimOfPartitionColumn(partCol);
    } else {
      return ((Cube) cube).getTimeDimOfPartitionColumn(partCol);
    }
  }

  /**
   * @return the queriedDimAttrs
   */
  public Set<String> getQueriedDimAttrs() {
    return queriedDimAttrs;
  }

  /**
   * @return the queriedMsrs
   */
  public Set<String> getQueriedMsrs() {
    return queriedMsrs;
  }

  public void addQueriedDimAttrs(Set<String> dimAttrs) {
    queriedDimAttrs.addAll(dimAttrs);
  }

  public void addQueriedMsrs(Set<String> msrs) {
    queriedMsrs.addAll(msrs);
  }

  /**
   * @return the candidateFactSets
   */
  public Set<Set<CandidateFact>> getCandidateFactSets() {
    return candidateFactSets;
  }

  /**
   * Prune candidate fact sets with respect to available candidate facts.
   * 
   * Prune a candidate set, if any of the fact is missing.
   * 
   * @param pruneCause
   */
  public void pruneCandidateFactSet(CubeTableCause pruneCause) {
    // remove candidate fact sets that have missing facts
    for (Iterator<Set<CandidateFact>> i = candidateFactSets.iterator(); i.hasNext();) {
      Set<CandidateFact> cfacts = i.next();
      if (!candidateFacts.containsAll(cfacts)) {
        LOG.info("Not considering fact table set:" + cfacts
            + " as they have non candidate tables and facts missing because of" + pruneCause);
        i.remove();
      }
    }
  }

  /**
   * Prune candidate fact with respect to available candidate fact sets.
   * 
   * If candidate fact is not present in any of the candidate fact sets, remove
   * it.
   * 
   * @param pruneCause
   */
  public void pruneCandidateFactWithCandidateSet(CubeTableCause pruneCause) {
    // remove candidate facts that are not part of any covering set
    Set<CandidateFact> allCoveringFacts = new HashSet<CandidateFact>();
    for (Set<CandidateFact> set : candidateFactSets) {
      allCoveringFacts.addAll(set);
    }
    for (Iterator<CandidateFact> i = candidateFacts.iterator(); i.hasNext();) {
      CandidateFact cfact = i.next();
      if (!allCoveringFacts.contains(cfact)) {
        LOG.info("Not considering fact table:" + cfact + " as " + pruneCause);
        addFactPruningMsgs(cfact.fact, new CandidateTablePruneCause(cfact.getName(), pruneCause));
        i.remove();
      }
    }
  }
}
