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
import org.apache.lens.cube.metadata.SchemaGraph.TableRelationship;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.cube.parse.CubeQueryContext.OptionalDimCtx;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.*;

import lombok.*;

/**
 * JoinResolver.
 */
class JoinResolver implements ContextRewriter {

  private static final Log LOG = LogFactory.getLog(JoinResolver.class);

  @ToString
  public static class JoinClause implements Comparable<JoinClause> {
    private final int cost;
    // all dimensions in path except target
    private final Set<Dimension> dimsInPath;
    private CubeQueryContext cubeql;
    private final Map<Aliased<Dimension>, List<TableRelationship>> chain;
    private final JoinTree joinTree;
    transient Map<AbstractCubeTable, Set<String>> chainColumns = new HashMap<AbstractCubeTable, Set<String>>();

    public JoinClause(CubeQueryContext cubeql, Map<Aliased<Dimension>,
      List<TableRelationship>> chain, Set<Dimension> dimsInPath) {
      this.cubeql = cubeql;
      this.chain = chain;
      this.joinTree = mergeJoinChains(chain);
      this.cost = joinTree.getNumEdges();
      this.dimsInPath = dimsInPath;
    }

    void initChainColumns() {
      for (List<TableRelationship> path : chain.values()) {
        for (TableRelationship edge : path) {
          Set<String> fcols = chainColumns.get(edge.getFromTable());
          if (fcols == null) {
            fcols = new HashSet<String>();
            chainColumns.put(edge.getFromTable(), fcols);
          }
          fcols.add(edge.getFromColumn());

          Set<String> tocols = chainColumns.get(edge.getToTable());
          if (tocols == null) {
            tocols = new HashSet<String>();
            chainColumns.put(edge.getToTable(), tocols);
          }
          tocols.add(edge.getToColumn());
        }
      }
    }

    public int getCost() {
      return cost;
    }

    @Override
    public int compareTo(JoinClause joinClause) {
      return cost - joinClause.getCost();
    }

    /**
     * Takes chains and merges them in the form of a tree. If two chains have some common path till some table and
     * bifurcate from there, then in the chain, both paths will have the common path but the resultant tree will have
     * single path from root(cube) to that table and paths will bifurcate from there.
     * <p/>
     * For example, citystate   =   [basecube.cityid=citydim.id], [citydim.stateid=statedim.id]
     *              cityzip     =   [basecube.cityid=citydim.id], [citydim.zipcode=zipdim.code]
     * <p/>
     * Without merging, the behaviour is like this:
     * <p/>
     * <p/>
     *                  (basecube.cityid=citydim.id)          (citydim.stateid=statedim.id)
     *                  _____________________________citydim____________________________________statedim
     *                 |
     *   basecube------|
     *                 |_____________________________citydim____________________________________zipdim
     *
     *                  (basecube.cityid=citydim.id)          (citydim.zipcode=zipdim.code)
     *
     * <p/>
     * Merging will result in a tree like following
     * <p/>                                                  (citydim.stateid=statedim.id)
     * <p/>                                                ________________________________ statedim
     *             (basecube.cityid=citydim.id)           |
     * basecube-------------------------------citydim---- |
     *                                                    |________________________________  zipdim
     *
     *                                                       (citydim.zipcode=zipdim.code)
     *
     * <p/>
     * Doing this will reduce the number of joins wherever possible.
     *
     * @param chain Joins in Linear format.
     * @return Joins in Tree format
     */
    public JoinTree mergeJoinChains(Map<Aliased<Dimension>, List<TableRelationship>> chain) {
      Map<String, Integer> aliasUsage = new HashMap<String, Integer>();
      JoinTree root = JoinTree.createRoot();
      for (Map.Entry<Aliased<Dimension>, List<TableRelationship>> entry : chain.entrySet()) {
        JoinTree current = root;
        // Last element in this list is link from cube to first dimension
        for (int i = entry.getValue().size() - 1; i >= 0; i--) {
          // Adds a child if needed, or returns a child already existing corresponding to the given link.
          current = current.addChild(entry.getValue().get(i), cubeql, aliasUsage);
          if (cubeql.getAutoJoinCtx().partialJoinChains) {
            JoinType joinType = cubeql.getAutoJoinCtx().tableJoinTypeMap.get(entry.getKey().getObject());
            //This ensures if (sub)paths are same, but join type is not same, merging will not happen.
            current.setJoinType(joinType);
          }
        }
        // This is a destination table. Decide alias separately. e.g. chainname
        // nullcheck is necessary because dimensions can be destinations too. In that case getAlias() == null
        if (entry.getKey().getAlias() != null) {
          current.setAlias(entry.getKey().getAlias());
        }
      }
      if (root.getSubtrees().size() > 0) {
        root.setAlias(cubeql.getAliasForTabName(
          root.getSubtrees().keySet().iterator().next().getFromTable().getName()));
      }
      return root;
    }
  }

  @Data
  @ToString(exclude = "parent")
  @EqualsAndHashCode(exclude = "parent")
  public static class JoinTree {
    //parent of the node
    JoinTree parent;
    // current table is parentRelationship.destTable;
    TableRelationship parentRelationship;
    // Alias for the join clause
    String alias;
    private Map<TableRelationship, JoinTree> subtrees = new LinkedHashMap<TableRelationship, JoinTree>();
    // Number of nodes from root to this node. depth of root is 0. Unused for now.
    private int depthFromRoot;
    // join type of the current table.
    JoinType joinType;

    public static JoinTree createRoot() {
      return new JoinTree(null, null, 0);
    }

    public JoinTree(JoinTree parent, TableRelationship tableRelationship,
      int depthFromRoot) {
      this.parent = parent;
      this.parentRelationship = tableRelationship;
      this.depthFromRoot = depthFromRoot;
    }

    public JoinTree addChild(TableRelationship tableRelationship,
      CubeQueryContext cubeql, Map<String, Integer> aliasUsage) {
      if (getSubtrees().get(tableRelationship) == null) {
        JoinTree current = new JoinTree(this, tableRelationship,
          this.depthFromRoot + 1);
        // Set alias. Need to compute only when new node is being created.
        // The following code ensures that For intermediate tables, aliases are given
        // in the order citydim, citydim_0, citydim_1, ...
        // And for destination tables, an alias will be decided from here but might be
        // overridden outside this function.
        AbstractCubeTable destTable = tableRelationship.getToTable();
        current.setAlias(cubeql.getAliasForTabName(destTable.getName()));
        if (aliasUsage.get(current.getAlias()) == null) {
          aliasUsage.put(current.getAlias(), 0);
        } else {
          aliasUsage.put(current.getAlias(), aliasUsage.get(current.getAlias()) + 1);
          current.setAlias(current.getAlias() + "_" + (aliasUsage.get(current.getAlias()) - 1));
        }
        getSubtrees().put(tableRelationship, current);
      }
      return getSubtrees().get(tableRelationship);
    }

    // Recursive computation of number of edges.
    public int getNumEdges() {
      int ret = 0;
      for (JoinTree tree : getSubtrees().values()) {
        ret += 1;
        ret += tree.getNumEdges();
      }
      return ret;
    }

    public boolean isLeaf() {
      return getSubtrees().isEmpty();
    }

    // Breadth First Traversal. Unused currently.
    public Iterator<JoinTree> bft() {
      return new Iterator<JoinTree>() {
        List<JoinTree> remaining = new ArrayList<JoinTree>() {
          {
            addAll(getSubtrees().values());
          }
        };

        @Override
        public boolean hasNext() {
          return remaining.isEmpty();
        }

        @Override
        public JoinTree next() {
          JoinTree retval = remaining.remove(0);
          remaining.addAll(retval.getSubtrees().values());
          return retval;
        }

        @Override
        public void remove() {
          throw new RuntimeException("Not implemented");
        }
      };
    }

    // Depth first traversal of the tree. Used in forming join string.
    public Iterator<JoinTree> dft() {
      return new Iterator<JoinTree>() {
        Stack<JoinTree> joinTreeStack = new Stack<JoinTree>() {
          {
            addAll(getSubtrees().values());
          }
        };

        @Override
        public boolean hasNext() {
          return !joinTreeStack.isEmpty();
        }

        @Override
        public JoinTree next() {
          JoinTree retval = joinTreeStack.pop();
          joinTreeStack.addAll(retval.getSubtrees().values());
          return retval;
        }

        @Override
        public void remove() {
          throw new RuntimeException("Not implemented");
        }
      };
    }

    public Set<JoinTree> leaves() {
      Set<JoinTree> leaves = new HashSet<JoinTree>();
      Iterator<JoinTree> dft = dft();
      while (dft.hasNext()) {
        JoinTree cur = dft.next();
        if (cur.isLeaf()) {
          leaves.add(cur);
        }
      }
      return leaves;
    }
  }

  /**
   * Store join chain information resolved by join resolver
   */
  public static class AutoJoinContext {
    // Map of a joined table to list of all possible paths from that table to
    // the target
    private final Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> allPaths;
    // User supplied partial join conditions
    private final Map<AbstractCubeTable, String> partialJoinConditions;
    // True if the query contains user supplied partial join conditions
    private final boolean partialJoinChains;
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
    private Map<Dimension, Map<AbstractCubeTable, List<String>>> joinPathFromColumns =
      new HashMap<Dimension, Map<AbstractCubeTable, List<String>>>();

    @Getter
    private Map<Dimension, Map<AbstractCubeTable, List<String>>> joinPathToColumns =
      new HashMap<Dimension, Map<AbstractCubeTable, List<String>>>();

    // there can be separate join clause for each fact incase of multi fact queries
    @Getter
    Map<CandidateFact, JoinClause> factClauses = new HashMap<CandidateFact, JoinClause>();
    @Getter
    @Setter
    JoinClause minCostClause;

    public AutoJoinContext(Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> allPaths,
      Map<Dimension, OptionalDimCtx> optionalDimensions, Map<AbstractCubeTable, String> partialJoinConditions,
      boolean partialJoinChains, Map<AbstractCubeTable, JoinType> tableJoinTypeMap, AbstractCubeTable autoJoinTarget,
      String joinTypeCfg, boolean joinsResolved) {
      this.allPaths = allPaths;
      initJoinPathColumns();
      this.partialJoinConditions = partialJoinConditions;
      this.partialJoinChains = partialJoinChains;
      this.tableJoinTypeMap = tableJoinTypeMap;
      this.autoJoinTarget = autoJoinTarget;
      this.joinTypeCfg = joinTypeCfg;
      this.joinsResolved = joinsResolved;
      LOG.debug("All join paths:" + allPaths);
      LOG.debug("Join path from columns:" + joinPathFromColumns);
      LOG.debug("Join path to columns:" + joinPathToColumns);
    }

    public AbstractCubeTable getAutoJoinTarget() {
      return autoJoinTarget;
    }

    private JoinClause getJoinClause(CandidateFact fact) {
      if (fact == null) {
        return minCostClause;
      }
      return factClauses.get(fact);
    }

    // Populate map of tables to their columns which are present in any of the
    // join paths
    private void initJoinPathColumns() {
      for (List<SchemaGraph.JoinPath> paths : allPaths.values()) {
        for (int i = 0; i < paths.size(); i++) {
          SchemaGraph.JoinPath jp = paths.get(i);
          jp.initColumnsForTable();
        }
      }
      refreshJoinPathColumns();
    }

    public void refreshJoinPathColumns() {
      joinPathFromColumns.clear();
      joinPathToColumns.clear();
      for (Map.Entry<Aliased<Dimension>, List<SchemaGraph.JoinPath>> joinPathEntry : allPaths.entrySet()) {
        List<SchemaGraph.JoinPath> joinPaths = joinPathEntry.getValue();
        Map<AbstractCubeTable, List<String>> fromColPaths = joinPathFromColumns.get(joinPathEntry.getKey().getObject());
        Map<AbstractCubeTable, List<String>> toColPaths = joinPathToColumns.get(joinPathEntry.getKey().getObject());
        if (fromColPaths == null) {
          fromColPaths = new HashMap<AbstractCubeTable, List<String>>();
          joinPathFromColumns.put(joinPathEntry.getKey().getObject(), fromColPaths);
        }

        if (toColPaths == null) {
          toColPaths = new HashMap<AbstractCubeTable, List<String>>();
          joinPathToColumns.put(joinPathEntry.getKey().getObject(), toColPaths);
        }
        populateJoinPathCols(joinPaths, fromColPaths, toColPaths);
      }
    }

    private void populateJoinPathCols(List<SchemaGraph.JoinPath> joinPaths,
      Map<AbstractCubeTable, List<String>> fromPathColumns, Map<AbstractCubeTable, List<String>> toPathColumns) {
      for (SchemaGraph.JoinPath path : joinPaths) {
        for (TableRelationship edge : path.getEdges()) {
          AbstractCubeTable fromTable = edge.getFromTable();
          String fromColumn = edge.getFromColumn();
          List<String> columnsOfFromTable = fromPathColumns.get(fromTable);
          if (columnsOfFromTable == null) {
            columnsOfFromTable = new ArrayList<String>();
            fromPathColumns.put(fromTable, columnsOfFromTable);
          }
          columnsOfFromTable.add(fromColumn);

          // Similarly populate for the 'to' table
          AbstractCubeTable toTable = edge.getToTable();
          String toColumn = edge.getToColumn();
          List<String> columnsOfToTable = toPathColumns.get(toTable);
          if (columnsOfToTable == null) {
            columnsOfToTable = new ArrayList<String>();
            toPathColumns.put(toTable, columnsOfToTable);
          }
          columnsOfToTable.add(toColumn);
        }
      }
    }

    public void printAllPaths(String src) {
      LOG.info(src + " All paths" + allPaths);
    }

    public void removeJoinedTable(Dimension dim) {
      allPaths.remove(Aliased.create(dim));
      joinPathFromColumns.remove(dim);
    }

    public Map<AbstractCubeTable, String> getPartialJoinConditions() {
      return partialJoinConditions;
    }

    public String getFromString(String fromTable, CandidateFact fact, Set<Dimension> qdims,
      Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext cubeql) throws SemanticException {
      String fromString = fromTable;
      LOG.info("All paths dump:" + cubeql.getAutoJoinCtx().getAllPaths());
      if (qdims == null || qdims.isEmpty()) {
        return fromString;
      }
      // Compute the merged join clause string for the min cost joinclause
      String clause = getMergedJoinClause(cubeql.getAutoJoinCtx().getJoinClause(fact), dimsToQuery);

      fromString += clause;
      return fromString;
    }

    // Some refactoring needed to account for multiple join paths
    public String getMergedJoinClause(JoinClause joinClause, Map<Dimension, CandidateDim> dimsToQuery) {
      Set<String> clauses = new LinkedHashSet<String>();
      String joinTypeStr = "";
      JoinType joinType = JoinType.INNER;

      // this flag is set to true if user has specified a partial join chain
      if (!partialJoinChains) {
        // User has not specified any join conditions. In this case, we rely on
        // configuration for the join type
        if (StringUtils.isNotBlank(joinTypeCfg)) {
          joinType = JoinType.valueOf(joinTypeCfg.toUpperCase());
          joinTypeStr = getJoinTypeStr(joinType);
        }
      }

      Iterator<JoinTree> iter = joinClause.joinTree.dft();
      while (iter.hasNext()) {
        JoinTree cur = iter.next();
        if (partialJoinChains) {
          joinType = cur.getJoinType();
          joinTypeStr = getJoinTypeStr(joinType);
        }
        TableRelationship rel = cur.parentRelationship;
        String toAlias, fromAlias;
        fromAlias = cur.parent.getAlias();
        toAlias = cur.getAlias();
        StringBuilder clause = new StringBuilder(joinTypeStr).append(" join ");
        // Add storage table name followed by alias
        clause.append(dimsToQuery.get(rel.getToTable()).getStorageString(toAlias));

        clause.append(" on ").append(fromAlias).append(".")
          .append(rel.getFromColumn()).append(" = ").append(toAlias)
          .append(".").append(rel.getToColumn());

        // We have to push user specified filters for the joined tables
        String userFilter = null;
        // Partition condition on the tables also needs to be pushed depending
        // on the join
        String storageFilter = null;

        if (JoinType.INNER == joinType || JoinType.LEFTOUTER == joinType || JoinType.LEFTSEMI == joinType) {
          // For inner and left joins push filter of right table
          userFilter = partialJoinConditions.get(rel.getToTable());
          if (partialJoinConditions.containsKey(rel.getFromTable())) {
            if (StringUtils.isNotBlank(userFilter)) {
              userFilter += (" AND " + partialJoinConditions.get(rel.getFromTable()));
            } else {
              userFilter = partialJoinConditions.get(rel.getFromTable());
            }
          }
          storageFilter = getStorageFilter(dimsToQuery, rel.getToTable(), toAlias);
          dimsToQuery.get(rel.getToTable()).setWhereClauseAdded();
        } else if (JoinType.RIGHTOUTER == joinType) {
          // For right outer joins, push filters of left table
          userFilter = partialJoinConditions.get(rel.getFromTable());
          if (partialJoinConditions.containsKey(rel.getToTable())) {
            if (StringUtils.isNotBlank(userFilter)) {
              userFilter += (" AND " + partialJoinConditions.get(rel.getToTable()));
            } else {
              userFilter = partialJoinConditions.get(rel.getToTable());
            }
          }
          if (rel.getFromTable() instanceof Dimension) {
            storageFilter = getStorageFilter(dimsToQuery, rel.getFromTable(), fromAlias);
            dimsToQuery.get(rel.getFromTable()).setWhereClauseAdded();
          }
        } else if (JoinType.FULLOUTER == joinType) {
          // For full outer we need to push filters of both left and right
          // tables in the join clause
          String leftFilter = null, rightFilter = null;
          String leftStorageFilter = null, rightStorgeFilter = null;

          if (StringUtils.isNotBlank(partialJoinConditions.get(rel.getFromTable()))) {
            leftFilter = partialJoinConditions.get(rel.getFromTable()) + " and ";
          }

          if (rel.getFromTable() instanceof Dimension) {
            leftStorageFilter = getStorageFilter(dimsToQuery, rel.getFromTable(), fromAlias);
            if (StringUtils.isNotBlank((leftStorageFilter))) {
              dimsToQuery.get(rel.getFromTable()).setWhereClauseAdded();
            }
          }

          if (StringUtils.isNotBlank(partialJoinConditions.get(rel.getToTable()))) {
            rightFilter = partialJoinConditions.get(rel.getToTable());
          }

          rightStorgeFilter = getStorageFilter(dimsToQuery, rel.getToTable(), toAlias);
          if (StringUtils.isNotBlank(rightStorgeFilter)) {
            if (StringUtils.isNotBlank((leftStorageFilter))) {
              leftStorageFilter += " and ";
            }
            dimsToQuery.get(rel.getToTable()).setWhereClauseAdded();
          }

          userFilter = (leftFilter == null ? "" : leftFilter) + (rightFilter == null ? "" : rightFilter);
          storageFilter =
            (leftStorageFilter == null ? "" : leftStorageFilter)
              + (rightStorgeFilter == null ? "" : rightStorgeFilter);
        }

        if (StringUtils.isNotBlank(userFilter)) {
          clause.append(" and ").append(userFilter);
        }
        if (StringUtils.isNotBlank(storageFilter)) {
          clause.append(" and ").append(storageFilter);
        }
        clauses.add(clause.toString());
      }
      return StringUtils.join(clauses, "");
    }

    public Set<Dimension> getDimsOnPath(Map<Aliased<Dimension>, List<TableRelationship>> joinChain,
      Set<Dimension> qdims) {
      Set<Dimension> dimsOnPath = new HashSet<Dimension>();
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
        if (StringUtils.isNotBlank(dimsToQuery.get(table).whereClause)) {
          whereClause = dimsToQuery.get(table).whereClause;
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
      Set<String> allPaths = new HashSet<String>();
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
      final Map<Dimension, CandidateDim> dimsToQuery) {
      // Remove join paths which cannot be satisfied by the resolved candidate
      // fact and dimension tables
      if (cfacts != null) {
        // include columns from all picked facts
        Set<String> factColumns = new HashSet<String>();
        for (CandidateFact cfact : cfacts) {
          factColumns.addAll(cfact.getColumns());
        }

        for (List<SchemaGraph.JoinPath> paths : allPaths.values()) {
          for (int i = 0; i < paths.size(); i++) {
            SchemaGraph.JoinPath jp = paths.get(i);
            List<String> cubeCols = jp.getColumnsForTable((AbstractCubeTable) cube);
            if (cubeCols != null && !factColumns.containsAll(cubeCols)) {
              // This path requires some columns from the cube which are not
              // present in the candidate fact
              // Remove this path
              LOG.info("Removing join path:" + jp + " as columns :" + cubeCols + " dont exist");
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
     * @param candidateDims
     */
    public void pruneAllPathsForCandidateDims(Map<Dimension, Set<CandidateDim>> candidateDims) {
      Map<Dimension, Set<String>> dimColumns = new HashMap<Dimension, Set<String>>();
      // populate all columns present in candidate dims for each dimension
      for (Map.Entry<Dimension, Set<CandidateDim>> entry : candidateDims.entrySet()) {
        Dimension dim = entry.getKey();
        Set<String> allColumns = new HashSet<String>();
        for (CandidateDim cdim : entry.getValue()) {
          allColumns.addAll(cdim.getColumns());
        }
        dimColumns.put(dim, allColumns);
      }
      for (List<SchemaGraph.JoinPath> paths : allPaths.values()) {
        for (int i = 0; i < paths.size(); i++) {
          SchemaGraph.JoinPath jp = paths.get(i);
          for (AbstractCubeTable refTable : jp.getAllTables()) {
            List<String> cols = jp.getColumnsForTable(refTable);
            if (refTable instanceof Dimension) {
              if (cols != null && (dimColumns.get(refTable) == null || !dimColumns.get(refTable).containsAll(cols))) {
                // This path requires some columns from the cube which are not present in any candidate dim
                // Remove this path
                LOG.info("Removing join path:" + jp + " as columns :" + cols + " dont exist");
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

    private void pruneEmptyPaths(Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> allPaths) {
      Iterator<Map.Entry<Aliased<Dimension>, List<SchemaGraph.JoinPath>>> iter = allPaths.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<Aliased<Dimension>, List<SchemaGraph.JoinPath>> entry = iter.next();
        if (entry.getValue().isEmpty()) {
          iter.remove();
        }
      }
    }

    private Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> pruneFactPaths(CubeInterface cube,
      final CandidateFact cfact) {
      Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> prunedPaths
        = new HashMap<Aliased<Dimension>, List<SchemaGraph.JoinPath>>();
      // Remove join paths which cannot be satisfied by the candidate fact
      for (Map.Entry<Aliased<Dimension>, List<SchemaGraph.JoinPath>> ppaths : allPaths.entrySet()) {
        prunedPaths.put(ppaths.getKey(), new ArrayList<SchemaGraph.JoinPath>(ppaths.getValue()));
        List<SchemaGraph.JoinPath> paths = prunedPaths.get(ppaths.getKey());
        for (int i = 0; i < paths.size(); i++) {
          SchemaGraph.JoinPath jp = paths.get(i);
          List<String> cubeCols = jp.getColumnsForTable((AbstractCubeTable) cube);
          if (cubeCols != null && !cfact.getColumns().containsAll(cubeCols)) {
            // This path requires some columns from the cube which are not
            // present in the candidate fact
            // Remove this path
            LOG.info("Removing join path:" + jp + " as columns :" + cubeCols + " dont exist");
            paths.remove(i);
            i--;
          }
        }
      }
      pruneEmptyPaths(prunedPaths);
      return prunedPaths;
    }

    private void pruneAllPaths(final Map<Dimension, CandidateDim> dimsToQuery) {
      // Remove join paths which cannot be satisfied by the resolved dimension
      // tables
      if (dimsToQuery != null && !dimsToQuery.isEmpty()) {
        for (CandidateDim candidateDim : dimsToQuery.values()) {
          Set<String> dimCols = candidateDim.dimtable.getAllFieldNames();
          for (List<SchemaGraph.JoinPath> paths : allPaths.values()) {
            for (int i = 0; i < paths.size(); i++) {
              SchemaGraph.JoinPath jp = paths.get(i);
              List<String> candidateDimCols = jp.getColumnsForTable(candidateDim.getBaseTable());
              if (candidateDimCols != null && !dimCols.containsAll(candidateDimCols)) {
                // This path requires some columns from the dimension which are
                // not present in the candidate dim
                // Remove this path
                LOG.info("Removing join path:" + jp + " as columns :" + candidateDimCols + " dont exist");
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
      final Set<Dimension> qdims, final CubeQueryContext cubeql) {
      Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> allPaths;
      // if fact is passed only look at paths possible from fact to dims
      if (fact != null) {
        allPaths = pruneFactPaths(cubeql.getCube(), fact);
      } else {
        allPaths = new LinkedHashMap<Aliased<Dimension>, List<SchemaGraph.JoinPath>>(this.allPaths);
      }
      // prune allPaths with qdims
      LOG.info("pruning allPaths before generating all permutations.");
      LOG.info("allPaths: " + allPaths);
      LOG.info("qdims: " + qdims);
      pruneAllPathsWithQueriedDims(allPaths, qdims);

      // Number of paths in each path set
      final int[] groupSizes = new int[allPaths.values().size()];
      // Total number of elements in the cartesian product
      int numSamples = 1;
      // All path sets
      final List<List<SchemaGraph.JoinPath>> pathSets = new ArrayList<List<SchemaGraph.JoinPath>>();
      // Dimension corresponding to the path sets
      final List<Aliased<Dimension>> dimensions = new ArrayList<Aliased<Dimension>>(groupSizes.length);

      int i = 0;
      for (Map.Entry<Aliased<Dimension>, List<SchemaGraph.JoinPath>> entry : allPaths.entrySet()) {
        dimensions.add(entry.getKey());
        List<SchemaGraph.JoinPath> group = entry.getValue();
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
          Map<Aliased<Dimension>, List<TableRelationship>> chain
            = new LinkedHashMap<Aliased<Dimension>, List<TableRelationship>>();
          //generate next permutation.
          for (int i = groupSizes.length - 1, base = sample; i >= 0; base /= groupSizes[i], i--) {
            selection[i] = base % groupSizes[i];
          }
          for (int i = 0; i < selection.length; i++) {
            int selectedPath = selection[i];
            List<TableRelationship> path = pathSets.get(i).get(selectedPath).getEdges();
            chain.put(dimensions.get(i), path);
          }

          Set<Dimension> dimsOnPath = getDimsOnPath(chain, qdims);

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
     * @param allPaths
     * @param qdims
     */
    private void pruneAllPathsWithQueriedDims(Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> allPaths,
      Set<Dimension> qdims) {
      Iterator<Map.Entry<Aliased<Dimension>, List<SchemaGraph.JoinPath>>> iter = allPaths.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<Aliased<Dimension>, List<SchemaGraph.JoinPath>> cur = iter.next();
        if (!qdims.contains(cur.getKey().getObject())) {
          LOG.info("removing from allPaths: " + cur);
          iter.remove();
        }
      }
    }

    public Set<Dimension> pickOptionalTables(final CandidateFact fact,
      Set<Dimension> qdims, CubeQueryContext cubeql) throws SemanticException {
      // Find the min cost join clause and add dimensions in the clause as optional dimensions
      Set<Dimension> joiningOptionalTables = new HashSet<Dimension>();
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
        throw new SemanticException(ErrorMsg.NO_JOIN_PATH, qdims.toString(), autoJoinTarget.getName());
      }

      LOG.info("Fact:" + fact + " minCostClause:" + minCostClause);
      if (fact != null) {
        cubeql.getAutoJoinCtx().getFactClauses().put(fact, minCostClause);
      } else {
        cubeql.getAutoJoinCtx().setMinCostClause(minCostClause);
      }
      for (Dimension dim : minCostClause.dimsInPath) {
        if (!qdims.contains(dim)) {
          joiningOptionalTables.add(dim);
        }
      }

      minCostClause.initChainColumns();
      // prune candidate dims of joiningOptionalTables wrt joinging columns
      for (Dimension dim : joiningOptionalTables) {
        for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator(); i.hasNext();) {
          CandidateDim cdim = i.next();
          CubeDimensionTable dimtable = cdim.dimtable;
          if (!cdim.getColumns().containsAll(minCostClause.chainColumns.get(dim))) {
            i.remove();
            LOG.info("Not considering dimtable:" + dimtable + " as its columns are"
              + " not part of any join paths. Join columns:" + minCostClause.chainColumns.get(dim));
            cubeql.addDimPruningMsgs(dim, cdim.dimtable,
              CandidateTablePruneCause.noColumnPartOfAJoinPath(minCostClause.chainColumns.get(dim)));
            break;
          }
        }
        if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
          throw new SemanticException(ErrorMsg.NO_DIM_HAS_COLUMN, dim.getName(),
            minCostClause.chainColumns.get(dim).toString());
        }
      }

      return joiningOptionalTables;
    }

    public Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> getAllPaths() {
      return allPaths;
    }

    public boolean isReachableDim(Dimension dim) {
      Aliased<Dimension> aliased = Aliased.create(dim);
      return allPaths.containsKey(aliased) && !allPaths.get(aliased).isEmpty();
    }

  }

  static String getJoinTypeStr(JoinType joinType) {
    if (joinType == null) {
      return "";
    }
    switch (joinType) {
    case FULLOUTER:
      return " full outer";
    case INNER:
      return " inner";
    case LEFTOUTER:
      return " left outer";
    case LEFTSEMI:
      return " left semi";
    case UNIQUE:
      return " unique";
    case RIGHTOUTER:
      return " right outer";
    default:
      return "";
    }
  }

  private Map<AbstractCubeTable, String> partialJoinConditions;
  private Map<AbstractCubeTable, JoinType> tableJoinTypeMap;
  private boolean partialJoinChain;
  private AbstractCubeTable target;
  private HiveConf conf;
  private HashMap<Dimension, List<JoinChain>> dimensionInJoinChain = new HashMap<Dimension, List<JoinChain>>();


  public JoinResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    partialJoinConditions = new HashMap<AbstractCubeTable, String>();
    tableJoinTypeMap = new HashMap<AbstractCubeTable, JoinType>();
    try {
      conf = cubeql.getHiveConf();
      resolveJoins(cubeql);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  private void resolveJoins(CubeQueryContext cubeql) throws HiveException {
    QB cubeQB = cubeql.getQB();
    boolean joinResolverDisabled =
      conf.getBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, CubeQueryConfUtil.DEFAULT_DISABLE_AUTO_JOINS);
    if (joinResolverDisabled) {
      if (cubeql.getJoinTree() != null) {
        cubeQB.setQbJoinTree(genJoinTree(cubeQB, cubeql.getJoinTree(), cubeql));
      }
    } else {
      autoResolveJoins(cubeql);
    }
  }

  private void processJoinChains(CubeQueryContext cubeql) throws HiveException {
    for (JoinChain chain : cubeql.getJoinchains().values()) {
      Set<String> dims = chain.getIntermediateDimensions();

      dims.add(chain.getDestTable());
      for (String dim : dims) {
        Dimension dimension = cubeql.getMetastoreClient().getDimension(dim);
        if (dimensionInJoinChain.get(dimension) == null) {
          dimensionInJoinChain.put(dimension, new ArrayList<JoinChain>());
        }
        dimensionInJoinChain.get(dimension).add(chain);
      }
    }
  }

  /**
   * Resolve joins automatically for the given query.
   *
   * @param cubeql
   * @throws SemanticException
   */
  private void autoResolveJoins(CubeQueryContext cubeql) throws HiveException {
    // Check if this query needs a join -
    // A join is needed if there is a cube and at least one dimension, or, 0
    // cubes and more than one
    // dimensions
    processJoinChains(cubeql);
    Set<Dimension> dimensions = cubeql.getNonChainedDimensions();
    // Add dimensions specified in the partial join tree
    ASTNode joinClause = cubeql.getQB().getParseInfo().getJoinExpr();
    if (joinClause == null) {
      // Only cube in the query
      if (cubeql.hasCubeInQuery()) {
        target = (AbstractCubeTable) cubeql.getCube();
      } else {
        String targetDimAlias = cubeql.getQB().getTabAliases().iterator().next();
        String targetDimTable = cubeql.getQB().getTabNameForAlias(targetDimAlias);
        if (targetDimTable == null) {
          LOG.warn("Null table for alias " + targetDimAlias);
        }
        target = cubeql.getMetastoreClient().getDimension(targetDimTable);
      }
    }
    searchDimensionTables(cubeql.getMetastoreClient(), joinClause);
    if (target == null) {
      LOG.warn("Can't resolve joins for null target");
      return;
    }

    Set<Dimension> dimTables = new HashSet<Dimension>(dimensions);
    for (AbstractCubeTable partiallyJoinedTable : partialJoinConditions.keySet()) {
      dimTables.add((Dimension) partiallyJoinedTable);
    }

    for (JoinChain chain : cubeql.getJoinchains().values()) {
      for (String dimName : chain.getIntermediateDimensions()) {
        cubeql.addOptionalDimTable(dimName, null, null, true);
      }
    }

    // Remove target
    dimTables.remove(target);
    if (dimTables.isEmpty() && cubeql.getJoinchains().isEmpty()) {
      // Joins not required
      LOG.info("No dimension tables to resolve and no join chains present!");
      return;
    }


    SchemaGraph graph = cubeql.getMetastoreClient().getSchemaGraph();
    Map<Aliased<Dimension>, List<SchemaGraph.JoinPath>> multipleJoinPaths =
      new LinkedHashMap<Aliased<Dimension>, List<SchemaGraph.JoinPath>>();
    Map<Dimension, String> dimensionAliasMap = new HashMap<Dimension, String>();

    // Resolve join path for each dimension accessed in the query
    for (Dimension joinee : dimTables) {
      if (dimensionInJoinChain.get(joinee) == null) {
        // Find all possible join paths
        SchemaGraph.GraphSearch search = new SchemaGraph.GraphSearch(joinee, target, graph);
        List<SchemaGraph.JoinPath> joinPaths = search.findAllPathsToTarget();
        if (joinPaths != null && !joinPaths.isEmpty()) {
          Aliased<Dimension> aliasedJoinee = Aliased.create(joinee);
          multipleJoinPaths.put(aliasedJoinee, new ArrayList<SchemaGraph.JoinPath>(search.findAllPathsToTarget()));
          addOptionalTables(cubeql, multipleJoinPaths.get(aliasedJoinee), cubeql.getDimensions().contains(joinee));
        } else {
          // No link to cube from this dim, can't proceed with query
          if (LOG.isDebugEnabled()) {
            graph.print();
          }
          LOG.warn("No join path between " + joinee.getName() + " and " + target.getName());
          if (cubeql.getDimensions().contains(joinee)) {
            throw new SemanticException(ErrorMsg.NO_JOIN_PATH, joinee.getName(), target.getName());
          } else {
            // if joinee is optional dim table, remove those candidate facts
            Set<CandidateTable> candidates = cubeql.getOptionalDimensionMap().get(joinee).requiredForCandidates;
            for (CandidateTable candidate : candidates) {
              if (candidate instanceof CandidateFact) {
                if (cubeql.getCandidateFactTables().contains(candidate)) {
                  LOG.info("Not considering fact:" + candidate + " as there is no join path to " + joinee);
                  cubeql.getCandidateFactTables().remove(candidate);
                  cubeql.addFactPruningMsgs(((CandidateFact) candidate).fact, new CandidateTablePruneCause(
                    CandidateTablePruneCode.COLUMN_NOT_FOUND));
                }
              } else if (cubeql.getCandidateDimTables().containsKey(((CandidateDim) candidate).getBaseTable())) {
                LOG.info("Not considering dimtable:" + candidate + " as there is no join path to " + joinee);
                cubeql.getCandidateDimTables().get(((CandidateDim) candidate).getBaseTable()).remove(candidate);
                cubeql.addDimPruningMsgs(
                  (Dimension) candidate.getBaseTable(), (CubeDimensionTable) candidate.getTable(),
                  new CandidateTablePruneCause(CandidateTablePruneCode.COLUMN_NOT_FOUND)
                );
              }
            }
          }
        }
      } else if (dimensionInJoinChain.get(joinee).size() > 1) {
        throw new SemanticException("Table " + joinee.getName() + " has "
          +dimensionInJoinChain.get(joinee).size() + " different paths through joinchains "
          +"(" + dimensionInJoinChain.get(joinee) + ")"
          +" used in query. Couldn't determine which one to use");
      } else {
        // the case when dimension is used only once in all joinchains.
        if (isJoinchainDestination(cubeql, joinee)) {
          throw new SemanticException("Table " + joinee.getName() + " is getting accessed via two different names: "
            + "[" + dimensionInJoinChain.get(joinee).get(0).getName() + ", " + joinee.getName() + "]");
        }
      }
    }
    // populate paths from joinchains
    for (JoinChain chain : cubeql.getJoinchains().values()) {
      Dimension dimension = cubeql.getMetastoreClient().getDimension(chain.getDestTable());
      Aliased<Dimension> aliasedDimension = Aliased.create(dimension, chain.getName());
      if (multipleJoinPaths.get(aliasedDimension) == null) {
        multipleJoinPaths.put(aliasedDimension, new ArrayList<SchemaGraph.JoinPath>());
      }
      multipleJoinPaths.get(aliasedDimension).addAll(
        chain.getRelationEdges(cubeql.getMetastoreClient()));
    }
    AutoJoinContext joinCtx =
      new AutoJoinContext(multipleJoinPaths, cubeql.optionalDimensions, partialJoinConditions, partialJoinChain,
        tableJoinTypeMap, target, conf.get(CubeQueryConfUtil.JOIN_TYPE_KEY), true);
    cubeql.setAutoJoinCtx(joinCtx);
  }

  private boolean isJoinchainDestination(CubeQueryContext cubeql, Dimension dimension) {
    for (JoinChain chain : cubeql.getJoinchains().values()) {
      if (chain.getDestTable().equalsIgnoreCase(dimension.getName())) {
        return true;
      }
    }
    return false;
  }

  private void addOptionalTables(CubeQueryContext cubeql, List<SchemaGraph.JoinPath> joinPathList, boolean required)
    throws SemanticException {
    for (SchemaGraph.JoinPath joinPath : joinPathList) {
      for (TableRelationship rel : joinPath.getEdges()) {
        // Add the joined tables to the queries table sets so that they are
        // resolved in candidate resolver
        cubeql.addOptionalDimTable(rel.getToTable().getName(), null, null, required);
      }
    }
  }

  private void setTarget(CubeMetastoreClient metastore, ASTNode node) throws HiveException {
    String targetTableName = HQLParser.getString(HQLParser.findNodeByPath(node, TOK_TABNAME, Identifier));
    if (metastore.isDimension(targetTableName)) {
      target = metastore.getDimension(targetTableName);
    } else if (metastore.isCube(targetTableName)) {
      target = (AbstractCubeTable) metastore.getCube(targetTableName);
    } else {
      throw new SemanticException(ErrorMsg.JOIN_TARGET_NOT_CUBE_TABLE, targetTableName);
    }
  }

  private void searchDimensionTables(CubeMetastoreClient metastore, ASTNode node) throws HiveException {
    if (node == null) {
      return;
    }
    // User has specified join conditions partially. We need to store join
    // conditions as well as join types
    partialJoinChain = true;
    if (isJoinToken(node)) {
      ASTNode left = (ASTNode) node.getChild(0);
      ASTNode right = (ASTNode) node.getChild(1);
      // Get table name and

      String tableName = HQLParser.getString(HQLParser.findNodeByPath(right, TOK_TABNAME, Identifier));

      Dimension dimension = metastore.getDimension(tableName);
      String joinCond = "";
      if (node.getChildCount() > 2) {
        // User has specified a join condition for filter pushdown.
        joinCond = HQLParser.getString((ASTNode) node.getChild(2));
      }
      partialJoinConditions.put(dimension, joinCond);
      tableJoinTypeMap.put(dimension, getJoinType(node));
      if (isJoinToken(left)) {
        searchDimensionTables(metastore, left);
      } else {
        if (left.getToken().getType() == TOK_TABREF) {
          setTarget(metastore, left);
        }
      }
    } else if (node.getToken().getType() == TOK_TABREF) {
      setTarget(metastore, node);
    }

  }

  private JoinType getJoinType(ASTNode node) {
    switch (node.getToken().getType()) {
    case TOK_LEFTOUTERJOIN:
      return JoinType.LEFTOUTER;
    case TOK_LEFTSEMIJOIN:
      return JoinType.LEFTSEMI;
    case TOK_RIGHTOUTERJOIN:
      return JoinType.RIGHTOUTER;
    case TOK_FULLOUTERJOIN:
      return JoinType.FULLOUTER;
    case TOK_JOIN:
      return JoinType.INNER;
    case TOK_UNIQUEJOIN:
      return JoinType.UNIQUE;
    default:
      return JoinType.INNER;
    }
  }

  // Recursively find out join conditions
  private QBJoinTree genJoinTree(QB qb, ASTNode joinParseTree, CubeQueryContext cubeql) throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];

    // Figure out join condition descriptor
    switch (joinParseTree.getToken().getType()) {
    case TOK_LEFTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTOUTER);
      break;
    case TOK_RIGHTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.RIGHTOUTER);
      break;
    case TOK_FULLOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.FULLOUTER);
      break;
    case TOK_LEFTSEMIJOIN:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTSEMI);
      break;
    default:
      condn[0] = new JoinCond(0, 1, JoinType.INNER);
      joinTree.setNoOuterJoin(true);
      break;
    }

    joinTree.setJoinCond(condn);

    ASTNode left = (ASTNode) joinParseTree.getChild(0);
    ASTNode right = (ASTNode) joinParseTree.getChild(1);

    // Left subtree is table or a subquery
    if ((left.getToken().getType() == TOK_TABREF) || (left.getToken().getType() == TOK_SUBQUERY)) {
      String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName((ASTNode) left.getChild(0)).toLowerCase();
      String alias =
        left.getChildCount() == 1 ? tableName : SemanticAnalyzer.unescapeIdentifier(left
          .getChild(left.getChildCount() - 1).getText().toLowerCase());

      joinTree.setLeftAlias(alias);

      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);

      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);

    } else if (isJoinToken(left)) {
      // Left subtree is join token itself, so recurse down
      QBJoinTree leftTree = genJoinTree(qb, left, cubeql);

      joinTree.setJoinSrc(leftTree);

      String[] leftChildAliases = leftTree.getLeftAliases();
      String[] leftAliases = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++) {
        leftAliases[i] = leftChildAliases[i];
      }
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);

    } else {
      assert (false);
    }

    if ((right.getToken().getType() == TOK_TABREF) || (right.getToken().getType() == TOK_SUBQUERY)) {
      String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName((ASTNode) right.getChild(0)).toLowerCase();
      String alias =
        right.getChildCount() == 1 ? tableName : SemanticAnalyzer.unescapeIdentifier(right
          .getChild(right.getChildCount() - 1).getText().toLowerCase());
      String[] rightAliases = new String[1];
      rightAliases[0] = alias;
      joinTree.setRightAliases(rightAliases);
      String[] children = joinTree.getBaseSrc();
      if (children == null) {
        children = new String[2];
      }
      children[1] = alias;
      joinTree.setBaseSrc(children);
      // remember rhs table for semijoin
      if (!joinTree.getNoSemiJoin()) {
        joinTree.addRHSSemijoin(alias);
      }
    } else {
      assert false;
    }

    ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);
    if (joinCond != null) {
      cubeql.setJoinCond(joinTree, HQLParser.getString(joinCond));
    } else {
      // No join condition specified. this should be an error
      new SemanticException(ErrorMsg.NO_JOIN_CONDITION_AVAIABLE);
    }
    return joinTree;
  }

  private boolean isJoinToken(ASTNode node) {
    if ((node.getToken().getType() == TOK_JOIN) || (node.getToken().getType() == TOK_LEFTOUTERJOIN)
      || (node.getToken().getType() == TOK_RIGHTOUTERJOIN) || (node.getToken().getType() == TOK_FULLOUTERJOIN)
      || (node.getToken().getType() == TOK_LEFTSEMIJOIN) || (node.getToken().getType() == TOK_UNIQUEJOIN)) {
      return true;
    }
    return false;
  }
}
