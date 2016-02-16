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
package org.apache.lens.cube.parse.join;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.join.JoinPath;
import org.apache.lens.cube.metadata.join.TableRelationship;
import org.apache.lens.cube.parse.*;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.parse.JoinType;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Store join chain information resolved by join resolver
 */
@Slf4j
public class AutoJoinContext {
  // Map of a joined table to list of all possible paths from that table to
  // the target
  private final Map<Aliased<Dimension>, List<JoinPath>> allPaths;
  private Set<Dimension> requiredDimensions;
  @Getter
  // Map of joined table to the join type (if provided by user)
  private final Map<AbstractCubeTable, JoinType> tableJoinTypeMap;

  // True if joins were resolved automatically
  private boolean joinsResolved;
  // Target table for the auto join resolver
  private final AbstractCubeTable autoJoinTarget;
  // Configuration string to control join type
  private String joinTypeCfg;

  // Map of a joined table to its columns which are part of any of the join
  // paths. This is used in candidate table resolver
  @Getter
  private Map<Aliased<Dimension>, Map<AbstractCubeTable, List<String>>> joinPathFromColumns = new HashMap<>();

  @Getter
  private Map<Aliased<Dimension>, Map<AbstractCubeTable, List<String>>> joinPathToColumns = new HashMap<>();

  // there can be separate join clause for each fact in-case of multi fact queries
  @Getter
  Map<CandidateFact, JoinClause> factClauses = new HashMap<>();
  @Getter
  @Setter
  JoinClause minCostClause;
  private final boolean flattenBridgeTables;
  private final String bridgeTableFieldAggr;

  public AutoJoinContext(Map<Aliased<Dimension>, List<JoinPath>> allPaths,
                         Set<Dimension> requiredDimensions,
                         Map<AbstractCubeTable, JoinType> tableJoinTypeMap,
                         AbstractCubeTable autoJoinTarget, String joinTypeCfg, boolean joinsResolved,
                         boolean flattenBridgeTables, String bridgeTableFieldAggr) {
    this.allPaths = allPaths;
    this.requiredDimensions = requiredDimensions;
    initJoinPathColumns();
    this.tableJoinTypeMap = tableJoinTypeMap;
    this.autoJoinTarget = autoJoinTarget;
    this.joinTypeCfg = joinTypeCfg;
    this.joinsResolved = joinsResolved;
    this.flattenBridgeTables = flattenBridgeTables;
    this.bridgeTableFieldAggr = bridgeTableFieldAggr;
    log.debug("All join paths:{}", allPaths);
    log.debug("Join path from columns:{}", joinPathFromColumns);
    log.debug("Join path to columns:{}", joinPathToColumns);
  }

  public AbstractCubeTable getAutoJoinTarget() {
    return autoJoinTarget;
  }

  private JoinClause getJoinClause(CandidateFact fact) {
    if (fact == null || !factClauses.containsKey(fact)) {
      return minCostClause;
    }
    return factClauses.get(fact);
  }

  // Populate map of tables to their columns which are present in any of the
  // join paths
  private void initJoinPathColumns() {
    for (List<JoinPath> paths : allPaths.values()) {
      for (int i = 0; i < paths.size(); i++) {
        JoinPath jp = paths.get(i);
        jp.initColumnsForTable();
      }
    }
    refreshJoinPathColumns();
  }

  public void refreshJoinPathColumns() {
    joinPathFromColumns.clear();
    joinPathToColumns.clear();
    for (Map.Entry<Aliased<Dimension>, List<JoinPath>> joinPathEntry : allPaths.entrySet()) {
      List<JoinPath> joinPaths = joinPathEntry.getValue();
      Map<AbstractCubeTable, List<String>> fromColPaths = joinPathFromColumns.get(joinPathEntry.getKey().getObject());
      Map<AbstractCubeTable, List<String>> toColPaths = joinPathToColumns.get(joinPathEntry.getKey().getObject());
      if (fromColPaths == null) {
        fromColPaths = new HashMap<>();
        joinPathFromColumns.put(joinPathEntry.getKey(), fromColPaths);
      }

      if (toColPaths == null) {
        toColPaths = new HashMap<>();
        joinPathToColumns.put(joinPathEntry.getKey(), toColPaths);
      }
      populateJoinPathCols(joinPaths, fromColPaths, toColPaths);
    }
  }

  private void populateJoinPathCols(List<JoinPath> joinPaths,
    Map<AbstractCubeTable, List<String>> fromPathColumns, Map<AbstractCubeTable, List<String>> toPathColumns) {
    for (JoinPath path : joinPaths) {
      for (TableRelationship edge : path.getEdges()) {
        AbstractCubeTable fromTable = edge.getFromTable();
        String fromColumn = edge.getFromColumn();
        List<String> columnsOfFromTable = fromPathColumns.get(fromTable);
        if (columnsOfFromTable == null) {
          columnsOfFromTable = new ArrayList<>();
          fromPathColumns.put(fromTable, columnsOfFromTable);
        }
        columnsOfFromTable.add(fromColumn);

        // Similarly populate for the 'to' table
        AbstractCubeTable toTable = edge.getToTable();
        String toColumn = edge.getToColumn();
        List<String> columnsOfToTable = toPathColumns.get(toTable);
        if (columnsOfToTable == null) {
          columnsOfToTable = new ArrayList<>();
          toPathColumns.put(toTable, columnsOfToTable);
        }
        columnsOfToTable.add(toColumn);
      }
    }
  }

  public void removeJoinedTable(Aliased<Dimension> dim) {
    allPaths.remove(dim);
    joinPathFromColumns.remove(dim);
  }

  public String getFromString(String fromTable, CandidateFact fact, Set<Dimension> qdims,
    Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext cubeql) throws LensException {
    String fromString = fromTable;
    log.info("All paths dump:{} Queried dims:{}", cubeql.getAutoJoinCtx().getAllPaths(), qdims);
    if (qdims == null || qdims.isEmpty()) {
      return fromString;
    }
    // Compute the merged join clause string for the min cost joinClause
    String clause = getMergedJoinClause(cubeql, cubeql.getAutoJoinCtx().getJoinClause(fact), dimsToQuery);

    fromString += clause;
    return fromString;
  }

  // Some refactoring needed to account for multiple join paths
  public String getMergedJoinClause(CubeQueryContext cubeql, JoinClause joinClause,
                                    Map<Dimension, CandidateDim> dimsToQuery) {
    Set<String> clauses = new LinkedHashSet<>();
    String joinTypeStr = "";
    JoinType joinType = JoinType.INNER;

    if (StringUtils.isNotBlank(joinTypeCfg)) {
      joinType = JoinType.valueOf(joinTypeCfg.toUpperCase());
      joinTypeStr = JoinUtils.getJoinTypeStr(joinType);
    }

    Iterator<JoinTree> iter = joinClause.getJoinTree().dft();
    boolean hasBridgeTable = false;
    boolean initedBridgeClauses = false;
    StringBuilder bridgeSelectClause = new StringBuilder();
    StringBuilder bridgeFromClause = new StringBuilder();
    StringBuilder bridgeFilterClause = new StringBuilder();
    StringBuilder bridgeJoinClause = new StringBuilder();
    StringBuilder bridgeGroupbyClause = new StringBuilder();

    while (iter.hasNext()) {
      JoinTree cur = iter.next();
      TableRelationship rel = cur.parentRelationship;
      String toAlias, fromAlias;
      fromAlias = cur.parent.getAlias();
      toAlias = cur.getAlias();
      hasBridgeTable = flattenBridgeTables && (hasBridgeTable || rel.isMapsToMany());
      // We have to push user specified filters for the joined tables
      String userFilter = null;
      // Partition condition on the tables also needs to be pushed depending
      // on the join
      String storageFilter = null;

      if (JoinType.INNER == joinType || JoinType.LEFTOUTER == joinType || JoinType.LEFTSEMI == joinType) {
        // For inner and left joins push filter of right table
        storageFilter = getStorageFilter(dimsToQuery, rel.getToTable(), toAlias);
        dimsToQuery.get(rel.getToTable()).setWhereClauseAdded(toAlias);
      } else if (JoinType.RIGHTOUTER == joinType) {
        // For right outer joins, push filters of left table
        if (rel.getFromTable() instanceof Dimension) {
          storageFilter = getStorageFilter(dimsToQuery, rel.getFromTable(), fromAlias);
          dimsToQuery.get(rel.getFromTable()).setWhereClauseAdded(fromAlias);
        }
      } else if (JoinType.FULLOUTER == joinType) {
        // For full outer we need to push filters of both left and right
        // tables in the join clause
        String leftFilter = null, rightFilter = null;
        String leftStorageFilter = null, rightStorgeFilter = null;

        if (rel.getFromTable() instanceof Dimension) {
          leftStorageFilter = getStorageFilter(dimsToQuery, rel.getFromTable(), fromAlias);
          if (StringUtils.isNotBlank((leftStorageFilter))) {
            dimsToQuery.get(rel.getFromTable()).setWhereClauseAdded(fromAlias);
          }
        }

        rightStorgeFilter = getStorageFilter(dimsToQuery, rel.getToTable(), toAlias);
        if (StringUtils.isNotBlank(rightStorgeFilter)) {
          if (StringUtils.isNotBlank((leftStorageFilter))) {
            leftStorageFilter += " and ";
          }
          dimsToQuery.get(rel.getToTable()).setWhereClauseAdded(toAlias);
        }

        userFilter = (leftFilter == null ? "" : leftFilter) + (rightFilter == null ? "" : rightFilter);
        storageFilter =
          (leftStorageFilter == null ? "" : leftStorageFilter)
            + (rightStorgeFilter == null ? "" : rightStorgeFilter);
      }
      StringBuilder clause = new StringBuilder();

      // if a bridge table is present in the path
      if (hasBridgeTable) {
        // if any relation has bridge table, the clause becomes the following :
        // join (" select " + joinkey + " aggr over fields from bridge table + from bridgeTable + [where user/storage
        // filters] + groupby joinkey) on joincond"
        // Or
        // " join (select " + joinkey + " aggr over fields from table reached through bridge table + from bridge table
        // join <next tables> on join condition + [and user/storage filters] + groupby joinkey) on joincond
        if (!initedBridgeClauses) {
          // we just found a bridge table in the path we need to initialize the clauses for subquery required for
          // aggregating fields of bridge table
          // initiliaze select clause with join key
          bridgeSelectClause.append(" (select ").append(toAlias).append(".").append(rel.getToColumn()).append(" as ")
          .append(rel.getToColumn());
          // group by join key
          bridgeGroupbyClause.append(" group by ").append(toAlias).append(".").append(rel.getToColumn());
          // from clause with bridge table
          bridgeFromClause.append(" from ").append(dimsToQuery.get(rel.getToTable()).getStorageString(toAlias));
          // we need to initialize filter clause with user filter clause or storage filter if applicable
          if (StringUtils.isNotBlank(userFilter)) {
            bridgeFilterClause.append(userFilter);
          }
          if (StringUtils.isNotBlank(storageFilter)) {
            if (StringUtils.isNotBlank(bridgeFilterClause.toString())) {
              bridgeFilterClause.append(" and ");
            }
            bridgeFilterClause.append(storageFilter);
          }
          // initialize final join clause
          bridgeJoinClause.append(" on ").append(fromAlias).append(".")
            .append(rel.getFromColumn()).append(" = ").append("%s")
            .append(".").append(rel.getToColumn());
          initedBridgeClauses = true;
        } else {
          // if bridge clauses are already inited, this is a next table getting joined with bridge table
          // we will append a simple join clause
          bridgeFromClause.append(" join ");
          bridgeFromClause.append(dimsToQuery.get(rel.getToTable()).getStorageString(toAlias));
          bridgeFromClause.append(" on ").append(fromAlias).append(".")
            .append(rel.getFromColumn()).append(" = ").append(toAlias)
            .append(".").append(rel.getToColumn());

          if (StringUtils.isNotBlank(userFilter)) {
            bridgeFromClause.append(" and ").append(userFilter);
          }
          if (StringUtils.isNotBlank(storageFilter)) {
            bridgeFromClause.append(" and ").append(storageFilter);
          }
        }
        if (cubeql.getTblAliasToColumns().get(toAlias) != null
          && !cubeql.getTblAliasToColumns().get(toAlias).isEmpty()) {
          // there are fields selected from this table after seeing bridge table in path
          // we should make subQuery for this selection
          clause.append(joinTypeStr).append(" join ");
          clause.append(bridgeSelectClause.toString());
          for (String col : cubeql.getTblAliasToColumns().get(toAlias)) {
            clause.append(",").append(bridgeTableFieldAggr).append("(").append(toAlias)
              .append(".").append(col)
              .append(")")
              .append(" as ").append(col);
          }
          String bridgeFrom = bridgeFromClause.toString();
          clause.append(bridgeFrom);
          String bridgeFilter = bridgeFilterClause.toString();
          if (StringUtils.isNotBlank(bridgeFilter)) {
            if (bridgeFrom.contains(" join ")) {
              clause.append(" and ");
            } else {
              clause.append(" where");
            }
            clause.append(bridgeFilter);
          }
          clause.append(bridgeGroupbyClause.toString());
          clause.append(") ").append(toAlias);
          clause.append(String.format(bridgeJoinClause.toString(), toAlias));
          clauses.add(clause.toString());
        }
        if (cur.getSubtrees().isEmpty()) {
          // clear bridge flags and builders, as there are no more clauses in this tree.
          hasBridgeTable = false;
          initedBridgeClauses = false;
          bridgeSelectClause.setLength(0);
          bridgeFromClause.setLength(0);
          bridgeFilterClause.setLength(0);
          bridgeJoinClause.setLength(0);
          bridgeGroupbyClause.setLength(0);
        }
      } else {
        // Simple join clause is :
        // joinType + " join " + destTable + " on " + joinCond + [" and" + userFilter] + ["and" + storageFilter]
        clause.append(joinTypeStr).append(" join ");
        //Add storage table name followed by alias
        clause.append(dimsToQuery.get(rel.getToTable()).getStorageString(toAlias));
        clause.append(" on ").append(fromAlias).append(".")
          .append(rel.getFromColumn()).append(" = ").append(toAlias)
          .append(".").append(rel.getToColumn());

        if (StringUtils.isNotBlank(userFilter)) {
          clause.append(" and ").append(userFilter);
        }
        if (StringUtils.isNotBlank(storageFilter)) {
          clause.append(" and ").append(storageFilter);
        }
        clauses.add(clause.toString());
      }
    }
    return StringUtils.join(clauses, "");
  }

  public Set<Dimension> getDimsOnPath(Map<Aliased<Dimension>, List<TableRelationship>> joinChain,
    Set<Dimension> qdims) {
    Set<Dimension> dimsOnPath = new HashSet<>();
    for (Map.Entry<Aliased<Dimension>, List<TableRelationship>> entry : joinChain.entrySet()) {
      List<TableRelationship> chain = entry.getValue();
      Dimension table = entry.getKey().getObject();

      // check if join with this dimension is required
      if (!qdims.contains(table)) {
        continue;
      }

      for (int i = chain.size() - 1; i >= 0; i--) {
        TableRelationship rel = chain.get(i);
        dimsOnPath.add((Dimension) rel.getToTable());
      }
    }
    return dimsOnPath;
  }

  private String getStorageFilter(Map<Dimension, CandidateDim> dimsToQuery, AbstractCubeTable table, String alias) {
    String whereClause = "";
    if (dimsToQuery != null && dimsToQuery.get(table) != null) {
      if (StringUtils.isNotBlank(dimsToQuery.get(table).getWhereClause())) {
        whereClause = dimsToQuery.get(table).getWhereClause();
        if (alias != null) {
          whereClause = StorageUtil.getWhereClause(whereClause, alias);
        }
      }
    }
    return whereClause;
  }

  /**
   * @return the joinsResolved
   */
  public boolean isJoinsResolved() {
    return joinsResolved;
  }

  // Includes both queried join paths and optional join paths
  public Set<String> getAllJoinPathColumnsOfTable(AbstractCubeTable table) {
    Set<String> allPaths = new HashSet<>();
    for (Map<AbstractCubeTable, List<String>> optPaths : joinPathFromColumns.values()) {
      if (optPaths.get(table) != null) {
        allPaths.addAll(optPaths.get(table));
      }
    }

    for (Map<AbstractCubeTable, List<String>> optPaths : joinPathToColumns.values()) {
      if (optPaths.get(table) != null) {
        allPaths.addAll(optPaths.get(table));
      }
    }

    return allPaths;
  }

  public void pruneAllPaths(CubeInterface cube, final Set<CandidateFact> cfacts,
    final Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    // Remove join paths which cannot be satisfied by the resolved candidate
    // fact and dimension tables
    if (cfacts != null) {
      // include columns from all picked facts
      Set<String> factColumns = new HashSet<>();
      for (CandidateFact cFact : cfacts) {
        factColumns.addAll(cFact.getColumns());
      }

      for (List<JoinPath> paths : allPaths.values()) {
        for (int i = 0; i < paths.size(); i++) {
          JoinPath jp = paths.get(i);
          List<String> cubeCols = jp.getColumnsForTable((AbstractCubeTable) cube);
          if (cubeCols != null && !factColumns.containsAll(cubeCols)) {
            // This path requires some columns from the cube which are not
            // present in the candidate fact
            // Remove this path
            log.info("Removing join path:{} as columns :{} dont exist", jp, cubeCols);
            paths.remove(i);
            i--;
          }
        }
      }
      pruneEmptyPaths(allPaths);
    }
    pruneAllPaths(dimsToQuery);
  }

  /**
   * Prunes allPaths by removing paths which contain columns that are not present in any candidate dims.
   *
   * @param candidateDims candidate dimensions
   */
  public void pruneAllPathsForCandidateDims(Map<Dimension, Set<CandidateDim>> candidateDims) throws LensException {
    Map<Dimension, Set<String>> dimColumns = new HashMap<>();
    // populate all columns present in candidate dims for each dimension
    for (Map.Entry<Dimension, Set<CandidateDim>> entry : candidateDims.entrySet()) {
      Dimension dim = entry.getKey();
      Set<String> allColumns = new HashSet<>();
      for (CandidateDim cdim : entry.getValue()) {
        allColumns.addAll(cdim.getColumns());
      }
      dimColumns.put(dim, allColumns);
    }
    for (List<JoinPath> paths : allPaths.values()) {
      for (int i = 0; i < paths.size(); i++) {
        JoinPath jp = paths.get(i);
        for (AbstractCubeTable refTable : jp.getAllTables()) {
          List<String> cols = jp.getColumnsForTable(refTable);
          if (refTable instanceof Dimension) {
            if (cols != null && (dimColumns.get(refTable) == null || !dimColumns.get(refTable).containsAll(cols))) {
              // This path requires some columns from the cube which are not present in any candidate dim
              // Remove this path
              log.info("Removing join path:{} as columns :{} don't exist", jp, cols);
              paths.remove(i);
              i--;
              break;
            }
          }
        }
      }
    }
    pruneEmptyPaths(allPaths);
  }

  private void pruneEmptyPaths(Map<Aliased<Dimension>, List<JoinPath>> allPaths) throws LensException {
    Iterator<Map.Entry<Aliased<Dimension>, List<JoinPath>>> iter = allPaths.entrySet().iterator();
    Set<Dimension> noPathDims = new HashSet<>();
    while (iter.hasNext()) {
      Map.Entry<Aliased<Dimension>, List<JoinPath>> entry = iter.next();
      if (entry.getValue().isEmpty()) {
        noPathDims.add(entry.getKey().getObject());
        iter.remove();
      }
    }
    noPathDims.retainAll(requiredDimensions);

    if (!noPathDims.isEmpty()) {
      throw new LensException(LensCubeErrorCode.NO_JOIN_PATH.getLensErrorInfo(), autoJoinTarget.getName(),
        noPathDims.toString());
    }
  }

  private Map<Aliased<Dimension>, List<JoinPath>> pruneFactPaths(CubeInterface cube,
    final CandidateFact cFact) throws LensException {
    Map<Aliased<Dimension>, List<JoinPath>> prunedPaths = new HashMap<>();
    // Remove join paths which cannot be satisfied by the candidate fact
    for (Map.Entry<Aliased<Dimension>, List<JoinPath>> ppaths : allPaths.entrySet()) {
      prunedPaths.put(ppaths.getKey(), new ArrayList<>(ppaths.getValue()));
      List<JoinPath> paths = prunedPaths.get(ppaths.getKey());
      for (int i = 0; i < paths.size(); i++) {
        JoinPath jp = paths.get(i);
        List<String> cubeCols = jp.getColumnsForTable((AbstractCubeTable) cube);
        if (cubeCols != null && !cFact.getColumns().containsAll(cubeCols)) {
          // This path requires some columns from the cube which are not
          // present in the candidate fact
          // Remove this path
          log.info("Removing join path:{} as columns :{} don't exist", jp, cubeCols);
          paths.remove(i);
          i--;
        }
      }
    }
    pruneEmptyPaths(prunedPaths);
    return prunedPaths;
  }

  private void pruneAllPaths(final Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    // Remove join paths which cannot be satisfied by the resolved dimension
    // tables
    if (dimsToQuery != null && !dimsToQuery.isEmpty()) {
      for (CandidateDim candidateDim : dimsToQuery.values()) {
        Set<String> dimCols = candidateDim.getTable().getAllFieldNames();
        for (List<JoinPath> paths : allPaths.values()) {
          for (int i = 0; i < paths.size(); i++) {
            JoinPath jp = paths.get(i);
            List<String> candidateDimCols = jp.getColumnsForTable(candidateDim.getBaseTable());
            if (candidateDimCols != null && !dimCols.containsAll(candidateDimCols)) {
              // This path requires some columns from the dimension which are
              // not present in the candidate dim
              // Remove this path
              log.info("Removing join path:{} as columns :{} dont exist", jp, candidateDimCols);
              paths.remove(i);
              i--;
            }
          }
        }
      }
      pruneEmptyPaths(allPaths);
    }
  }

  /**
   * There can be multiple join paths between a dimension and the target. Set of all possible join clauses is the
   * cartesian product of join paths of all dimensions
   */
  private Iterator<JoinClause> getJoinClausesForAllPaths(final CandidateFact fact,
    final Set<Dimension> qDims, final CubeQueryContext cubeql) throws LensException {
    Map<Aliased<Dimension>, List<JoinPath>> allPaths;
    // if fact is passed only look at paths possible from fact to dims
    if (fact != null) {
      allPaths = pruneFactPaths(cubeql.getCube(), fact);
    } else {
      allPaths = new LinkedHashMap<>(this.allPaths);
    }
    // prune allPaths with qdims
    pruneAllPathsWithQueriedDims(allPaths, qDims);

    // Number of paths in each path set
    final int[] groupSizes = new int[allPaths.values().size()];
    // Total number of elements in the cartesian product
    int numSamples = 1;
    // All path sets
    final List<List<JoinPath>> pathSets = new ArrayList<>();
    // Dimension corresponding to the path sets
    final List<Aliased<Dimension>> dimensions = new ArrayList<>(groupSizes.length);

    int i = 0;
    for (Map.Entry<Aliased<Dimension>, List<JoinPath>> entry : allPaths.entrySet()) {
      dimensions.add(entry.getKey());
      List<JoinPath> group = entry.getValue();
      pathSets.add(group);
      groupSizes[i] = group.size();
      numSamples *= groupSizes[i];
      i++;
    }

    final int[] selection = new int[groupSizes.length];
    final int MAX_SAMPLE_COUNT = numSamples;

    // Return a lazy iterator over all possible join chains
    return new Iterator<JoinClause>() {
      int sample = 0;

      @Override
      public boolean hasNext() {
        return sample < MAX_SAMPLE_COUNT;
      }

      @Override
      public JoinClause next() {
        Map<Aliased<Dimension>, List<TableRelationship>> chain = new LinkedHashMap<>();
        //generate next permutation.
        for (int i = groupSizes.length - 1, base = sample; i >= 0; base /= groupSizes[i], i--) {
          selection[i] = base % groupSizes[i];
        }
        for (int i = 0; i < selection.length; i++) {
          int selectedPath = selection[i];
          List<TableRelationship> path = pathSets.get(i).get(selectedPath).getEdges();
          chain.put(dimensions.get(i), path);
        }

        Set<Dimension> dimsOnPath = getDimsOnPath(chain, qDims);

        sample++;
        // Cost of join = number of tables joined in the clause
        return new JoinClause(cubeql, chain, dimsOnPath);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Cannot remove elements!");
      }
    };
  }

  /**
   * Given allPaths, it will remove entries where key is a non-join chain dimension and not contained in qdims
   *
   * @param allPaths All join paths
   * @param qDims queried dimensions
   */
  private void pruneAllPathsWithQueriedDims(Map<Aliased<Dimension>, List<JoinPath>> allPaths,
    Set<Dimension> qDims) {
    Iterator<Map.Entry<Aliased<Dimension>, List<JoinPath>>> iterator = allPaths.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Aliased<Dimension>, List<JoinPath>> cur = iterator.next();
      if (!qDims.contains(cur.getKey().getObject())) {
        log.info("removing from allPaths: {}", cur);
        iterator.remove();
      }
    }
  }

  public Set<Dimension> pickOptionalTables(final CandidateFact fact,
    Set<Dimension> qdims, CubeQueryContext cubeql) throws LensException {
    // Find the min cost join clause and add dimensions in the clause as optional dimensions
    Set<Dimension> joiningOptionalTables = new HashSet<>();
    if (qdims == null) {
      return joiningOptionalTables;
    }
    // find least cost path
    Iterator<JoinClause> itr = getJoinClausesForAllPaths(fact, qdims, cubeql);
    JoinClause minCostClause = null;
    while (itr.hasNext()) {
      JoinClause clause = itr.next();
      if (minCostClause == null || minCostClause.getCost() > clause.getCost()) {
        minCostClause = clause;
      }
    }

    if (minCostClause == null) {
      throw new LensException(LensCubeErrorCode.NO_JOIN_PATH.getLensErrorInfo(),
          qdims.toString(), autoJoinTarget.getName());
    }

    log.info("Fact: {} minCostClause:{}", fact, minCostClause);
    if (fact != null) {
      cubeql.getAutoJoinCtx().getFactClauses().put(fact, minCostClause);
    } else {
      cubeql.getAutoJoinCtx().setMinCostClause(minCostClause);
    }
    for (Dimension dim : minCostClause.getDimsInPath()) {
      if (!qdims.contains(dim)) {
        joiningOptionalTables.add(dim);
      }
    }

    minCostClause.initChainColumns();
    // prune candidate dims of joiningOptionalTables wrt joining columns
    for (Dimension dim : joiningOptionalTables) {
      for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator(); i.hasNext();) {
        CandidateDim cDim = i.next();
        if (!cDim.getColumns().containsAll(minCostClause.chainColumns.get(dim))) {
          i.remove();
          log.info("Not considering dimTable:{} as its columns are not part of any join paths. Join columns:{}",
            cDim.getTable(), minCostClause.chainColumns.get(dim));
          cubeql.addDimPruningMsgs(dim, cDim.getTable(),
            CandidateTablePruneCause.noColumnPartOfAJoinPath(minCostClause.chainColumns.get(dim)));
        }
      }
      if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
        throw new LensException(LensCubeErrorCode.NO_DIM_HAS_COLUMN.getLensErrorInfo(), dim.getName(),
          minCostClause.chainColumns.get(dim).toString());
      }
    }

    return joiningOptionalTables;
  }

  public Map<Aliased<Dimension>, List<JoinPath>> getAllPaths() {
    return allPaths;
  }

  public boolean isReachableDim(Dimension dim) {
    Aliased<Dimension> aliased = Aliased.create(dim);
    return isReachableDim(aliased);
  }

  public boolean isReachableDim(Dimension dim, String alias) {
    Aliased<Dimension> aliased = Aliased.create(dim, alias);
    return isReachableDim(aliased);
  }

  private boolean isReachableDim(Aliased<Dimension> aliased) {
    return allPaths.containsKey(aliased) && !allPaths.get(aliased).isEmpty();
  }
}
