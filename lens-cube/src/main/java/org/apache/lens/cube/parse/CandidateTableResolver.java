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

import java.util.*;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.cube.parse.CubeQueryContext.OptionalDimCtx;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * This resolver prunes the candidate tables for following cases
 * <p/>
 * 1. If queried dim attributes are not present. Also Figures out if queried column is not part of candidate table, but
 * is a denormalized field which can reached through a reference 2. Finds all the candidate fact sets containing queried
 * measures. Prunes facts which do not contain any of the queried measures. 3. Required join columns are not part of
 * candidate tables 4. Required source columns(join columns) for reaching a denormalized field, are not part of
 * candidate tables 5. Required denormalized fields are not part of refered tables, there by all the candidates which
 * are using denormalized fields.
 */
class CandidateTableResolver implements ContextRewriter {

  private static final Log LOG = LogFactory.getLog(CandidateTableResolver.class.getName());
  private boolean qlEnabledMultiTableSelect;
  private boolean checkForQueriedColumns = true;

  public CandidateTableResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    qlEnabledMultiTableSelect =
      cubeql.getConf().getBoolean(CubeQueryConfUtil.ENABLE_MULTI_TABLE_SELECT,
        CubeQueryConfUtil.DEFAULT_MULTI_TABLE_SELECT);
    if (checkForQueriedColumns) {
      LOG.debug("Dump queried columns:" + cubeql.getTblAliasToColumns());
      populateCandidateTables(cubeql);
      resolveCandidateFactTables(cubeql);
      resolveCandidateDimTables(cubeql);
      // remove optional dims added whom requiredForCandidates have been removed
      pruneOptionalDims(cubeql);
      checkForQueriedColumns = false;
    } else {
      // populate optional tables
      for (Dimension dim : cubeql.getOptionalDimensions()) {
        LOG.info("Populating optional dim:" + dim);
        populateDimTables(dim, cubeql, true);
      }
      if (cubeql.getAutoJoinCtx() != null) {
        // Before checking for candidate table columns, prune join paths containing non existing columns
        // in populated candidate tables
        cubeql.getAutoJoinCtx().pruneAllPaths(cubeql.getCube(), cubeql.getCandidateFactTables(), null);
        cubeql.getAutoJoinCtx().pruneAllPathsForCandidateDims(cubeql.getCandidateDimTables());
        cubeql.getAutoJoinCtx().refreshJoinPathColumns();
      }
      checkForSourceReachabilityForDenormCandidates(cubeql);
      // check for joined columns and denorm columns on refered tables
      resolveCandidateFactTablesForJoins(cubeql);
      resolveCandidateDimTablesForJoinsAndDenorms(cubeql);
      cubeql.pruneCandidateFactSet(CandidateTablePruneCode.INVALID_DENORM_TABLE);
      checkForQueriedColumns = true;
    }
  }

  private void populateCandidateTables(CubeQueryContext cubeql) throws SemanticException {
    try {
      if (cubeql.getCube() != null) {
        List<CubeFactTable> factTables = cubeql.getMetastoreClient().getAllFactTables(cubeql.getCube());
        if (factTables.isEmpty()) {
          throw new SemanticException(ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE, cubeql.getCube().getName()
            + " does not have any facts");
        }
        for (CubeFactTable fact : factTables) {
          CandidateFact cfact = new CandidateFact(fact, cubeql.getCube());
          cfact.enabledMultiTableSelect = qlEnabledMultiTableSelect;
          cubeql.getCandidateFactTables().add(cfact);
        }
        LOG.info("Populated candidate facts:" + cubeql.getCandidateFactTables());
      }

      if (cubeql.getDimensions().size() != 0) {
        for (Dimension dim : cubeql.getDimensions()) {
          populateDimTables(dim, cubeql, false);
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  private void populateDimTables(Dimension dim, CubeQueryContext cubeql, boolean optional) throws SemanticException {
    if (cubeql.getCandidateDimTables().get(dim) != null) {
      return;
    }
    try {
      Set<CandidateDim> candidates = new HashSet<CandidateDim>();
      cubeql.getCandidateDimTables().put(dim, candidates);
      List<CubeDimensionTable> dimtables = cubeql.getMetastoreClient().getAllDimensionTables(dim);
      if (dimtables.isEmpty()) {
        if (!optional) {
          throw new SemanticException(ErrorMsg.NO_CANDIDATE_DIM_AVAILABLE, dim.getName(),
            "Dimension tables do not exist");
        } else {
          LOG.info("Not considering optional dimension " + dim + " as," + " No dimension tables exist");
          removeOptionalDim(cubeql, dim);
        }
      }
      for (CubeDimensionTable dimtable : dimtables) {
        CandidateDim cdim = new CandidateDim(dimtable, dim);
        candidates.add(cdim);
      }
      LOG.info("Populated candidate dims:" + cubeql.getCandidateDimTables().get(dim) + " for " + dim);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  private void pruneOptionalDims(CubeQueryContext cubeql) {
    Set<Dimension> tobeRemoved = new HashSet<Dimension>();
    Set<CandidateTable> allCandidates = new HashSet<CandidateTable>();
    allCandidates.addAll(cubeql.getCandidateFactTables());
    for (Set<CandidateDim> cdims : cubeql.getCandidateDimTables().values()) {
      allCandidates.addAll(cdims);
    }
    Set<CandidateTable> removedCandidates = new HashSet<CandidateTable>();
    for (Map.Entry<Dimension, OptionalDimCtx> optdimEntry : cubeql.getOptionalDimensionMap().entrySet()) {
      Dimension dim = optdimEntry.getKey();
      OptionalDimCtx optdim = optdimEntry.getValue();
      Iterator<CandidateTable> iter = optdim.requiredForCandidates.iterator();
      while (iter.hasNext()) {
        CandidateTable candidate = iter.next();
        if (!allCandidates.contains(candidate)) {
          LOG.info("Removing candidate " + candidate + " from requiredForCandidates of " + dim + ", as it is no more"
            + " candidate");
          iter.remove();
          removedCandidates.add(candidate);
        }
      }
    }
    Set<CandidateTable> candidatesReachableThroughRefs = new HashSet<CandidateTable>();
    for (Map.Entry<Dimension, OptionalDimCtx> optdimEntry : cubeql.getOptionalDimensionMap().entrySet()) {
      Dimension dim = optdimEntry.getKey();
      OptionalDimCtx optdim = optdimEntry.getValue();
      candidatesReachableThroughRefs.addAll(optdim.requiredForCandidates);
      if ((!optdim.colQueried.isEmpty() && optdim.requiredForCandidates.isEmpty()) && !optdim.isRequiredInJoinChain) {
        LOG.info("Not considering optional dimension " + dim + " as all requiredForCandidates are removed");
        tobeRemoved.add(dim);
      }
    }
    for (Dimension dim : tobeRemoved) {
      removeOptionalDim(cubeql, dim);
    }
  }

  private void removeOptionalDim(CubeQueryContext cubeql, Dimension dim) {
    OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().remove(dim);
    // remove all the depending candidate table as well
    for (CandidateTable candidate : optdim.requiredForCandidates) {
      if (candidate instanceof CandidateFact) {
        LOG.info("Not considering fact:" + candidate + " as refered table does not have any valid dimtables");
        cubeql.getCandidateFactTables().remove(candidate);
        cubeql.addFactPruningMsgs(((CandidateFact) candidate).fact, new CandidateTablePruneCause(
          CandidateTablePruneCode.INVALID_DENORM_TABLE));
      } else {
        LOG.info("Not considering dimtable:" + candidate + " as refered table does not have any valid dimtables");
        cubeql.getCandidateDimTables().get(((CandidateDim) candidate).getBaseTable()).remove(candidate);
        cubeql.addDimPruningMsgs((Dimension) candidate.getBaseTable(), (CubeDimensionTable) candidate.getTable(),
          new CandidateTablePruneCause(CandidateTablePruneCode.INVALID_DENORM_TABLE));
      }
    }
    // remove join paths corresponding to the dim
    if (cubeql.getAutoJoinCtx() != null) {
      cubeql.getAutoJoinCtx().removeJoinedTable(dim);
    }
  }

  private void resolveCandidateFactTables(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getCube() != null) {
      String str = cubeql.getConf().get(CubeQueryConfUtil.getValidFactTablesKey(cubeql.getCube().getName()));
      List<String> validFactTables =
        StringUtils.isBlank(str) ? null : Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
      Set<String> queriedDimAttrs = cubeql.getQueriedDimAttrs();
      Set<String> queriedMsrs = cubeql.getQueriedMsrs();

      // Remove fact tables based on columns in the query
      for (Iterator<CandidateFact> i = cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
        CandidateFact cfact = i.next();

        if (validFactTables != null) {
          if (!validFactTables.contains(cfact.getName().toLowerCase())) {
            LOG.info("Not considering fact table:" + cfact + " as it is" + " not a valid fact");
            cubeql
              .addFactPruningMsgs(cfact.fact, new CandidateTablePruneCause(CandidateTablePruneCode.INVALID));
            i.remove();
            continue;
          }
        }

        // go over the columns accessed in the query and find out which tables
        // can answer the query
        // the candidate facts should have all the dimensions queried and
        // atleast
        // one measure
        boolean toRemove = false;
        for (String col : queriedDimAttrs) {
          if (!cfact.getColumns().contains(col.toLowerCase())) {
            // check if it available as reference, if not remove the candidate
            if (!cubeql.getDeNormCtx().addRefUsage(cfact, col, cubeql.getCube().getName())) {
              LOG.info("Not considering fact table:" + cfact + " as column " + col + " is not available");
              cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.columnNotFound(col));
              toRemove = true;
              break;
            }
          }
        }

        // go over join chains and prune facts that dont have any of the columns in each chain
        for (JoinChain chain : cubeql.getJoinchains().values()) {
          OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(cubeql.getCubeTbls().get(chain.getName()));
          if (!checkForColumnExists(cfact, chain.getSourceColumns())) {
            // check if chain is optional or not
            if (optdim == null || optdim.isRequiredInJoinChain
              || (optdim != null && optdim.requiredForCandidates.contains(cfact))) {
              LOG.info("Not considering fact table:" + cfact + " as columns " + chain.getSourceColumns()
                + " are not available");
              cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.columnNotFound(chain.getSourceColumns()));
              toRemove = true;
              break;
            }
          }
        }
        // check if the candidate fact has atleast one measure queried
        if (!checkForColumnExists(cfact, queriedMsrs)) {
          LOG.info("Not considering fact table:" + cfact + " as columns " + queriedMsrs + " is not available");
          cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.columnNotFound(queriedMsrs));
          toRemove = true;
        }
        if (toRemove) {
          i.remove();
        }
      }
      // Find out candidate fact table sets which contain all the measures
      // queried
      List<CandidateFact> cfacts = new ArrayList<CandidateFact>(cubeql.getCandidateFactTables());
      Set<Set<CandidateFact>> cfactset = findCoveringSets(cfacts, queriedMsrs);
      LOG.info("Measure covering fact sets :" + cfactset);
      if (cfactset.isEmpty()) {
        throw new SemanticException(ErrorMsg.NO_FACT_HAS_COLUMN, queriedMsrs.toString());
      }
      cubeql.getCandidateFactSets().addAll(cfactset);
      cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCode.COLUMN_NOT_FOUND);

      if (cubeql.getCandidateFactTables().size() == 0) {
        throw new SemanticException(ErrorMsg.NO_FACT_HAS_COLUMN, queriedDimAttrs.toString());
      }
    }
  }

  static Set<Set<CandidateFact>> findCoveringSets(List<CandidateFact> cfactsPassed, Set<String> msrs) {
    Set<Set<CandidateFact>> cfactset = new HashSet<Set<CandidateFact>>();
    List<CandidateFact> cfacts = new ArrayList<CandidateFact>(cfactsPassed);
    for (Iterator<CandidateFact> i = cfacts.iterator(); i.hasNext();) {
      CandidateFact cfact = i.next();
      i.remove();
      if (!checkForColumnExists(cfact, msrs)) {
        // check if fact contains any of the maeasures
        // if not ignore the fact
        continue;
      } else if (cfact.getColumns().containsAll(msrs)) {
        // return single set
        Set<CandidateFact> one = new LinkedHashSet<CandidateFact>();
        one.add(cfact);
        cfactset.add(one);
      } else {
        // find the remaining measures in other facts
        Set<String> remainingMsrs = new HashSet<String>(msrs);
        remainingMsrs.removeAll(cfact.getColumns());
        Set<Set<CandidateFact>> coveringSets = findCoveringSets(cfacts, remainingMsrs);
        if (!coveringSets.isEmpty()) {
          for (Set<CandidateFact> set : coveringSets) {
            set.add(cfact);
            cfactset.add(set);
          }
        } else {
          LOG.info("Couldnt find any set containing remaining measures:" + remainingMsrs);
        }
      }
    }
    return cfactset;
  }

  private void resolveCandidateDimTablesForJoinsAndDenorms(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getAutoJoinCtx() == null) {
      return;
    }
    Set<Dimension> allDims = new HashSet<Dimension>(cubeql.getDimensions());
    allDims.addAll(cubeql.getOptionalDimensions());
    for (Dimension dim : allDims) {
      if (cubeql.getCandidateDimTables().get(dim) != null && !cubeql.getCandidateDimTables().get(dim).isEmpty()) {
        for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator(); i.hasNext();) {
          CandidateDim cdim = i.next();
          CubeDimensionTable dimtable = cdim.dimtable;
          // go over the join columns accessed in the query and find out which tables
          // can participate in join
          // for each join path check for columns involved in path
          boolean removed = false;
          for (Map.Entry<Dimension, Map<AbstractCubeTable, List<String>>> joincolumnsEntry : cubeql.getAutoJoinCtx()
            .getJoinPathFromColumns().entrySet()) {
            Dimension reachableDim = joincolumnsEntry.getKey();
            OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(reachableDim);
            Collection<String> colSet = joincolumnsEntry.getValue().get((AbstractCubeTable) dim);

            if (!checkForColumnExists(cdim, colSet)) {
              if (optdim == null || optdim.isRequiredInJoinChain
                || (optdim != null && optdim.requiredForCandidates.contains(cdim))) {
                i.remove();
                removed = true;
                LOG.info("Not considering dimtable:" + dimtable + " as its columns are"
                  + " not part of any join paths. Join columns:" + colSet);
                cubeql.addDimPruningMsgs(dim, dimtable, CandidateTablePruneCause.noColumnPartOfAJoinPath(colSet));
                break;
              }
            }
          }
          if (!removed) {
            // check for to columns
            for (Map.Entry<Dimension, Map<AbstractCubeTable, List<String>>> joincolumnsEntry : cubeql.getAutoJoinCtx()
              .getJoinPathToColumns().entrySet()) {
              Dimension reachableDim = joincolumnsEntry.getKey();
              OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(reachableDim);
              Collection<String> colSet = joincolumnsEntry.getValue().get((AbstractCubeTable) dim);

              if (!checkForColumnExists(cdim, colSet)) {
                if (optdim == null || optdim.isRequiredInJoinChain
                  || (optdim != null && optdim.requiredForCandidates.contains(cdim))) {
                  i.remove();
                  removed = true;
                  LOG.info("Not considering dimtable:" + dimtable + " as its columns are"
                    + " not part of any join paths. Join columns:" + colSet);
                  cubeql.addDimPruningMsgs(dim, dimtable, CandidateTablePruneCause.noColumnPartOfAJoinPath(colSet));
                  break;
                }
              }
            }
          }
          if (!removed) {
            // go over the referenced columns accessed in the query and find out which tables can participate
            if (cubeql.getOptionalDimensionMap().get(dim) != null
              && !checkForColumnExists(cdim, cubeql.getOptionalDimensionMap().get(dim).colQueried)) {
              i.remove();
              LOG.info("Not considering optional dimtable:" + dimtable + " as its denorm fields do not exist."
                + " Denorm fields:" + cubeql.getOptionalDimensionMap().get(dim).colQueried);
              cubeql.addDimPruningMsgs(dim, dimtable,
                CandidateTablePruneCause.noColumnPartOfAJoinPath(cubeql.getOptionalDimensionMap().get(dim).colQueried));
            }
          }
        }
        if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
          OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(dim);
          if ((cubeql.getDimensions() != null && cubeql.getDimensions().contains(dim))
            || (optdim != null && optdim.isRequiredInJoinChain)) {
            throw new SemanticException(ErrorMsg.NO_DIM_HAS_COLUMN, dim.getName(), cubeql.getAutoJoinCtx()
              .getAllJoinPathColumnsOfTable(dim).toString());
          } else {
            // remove it from optional tables
            LOG.info("Not considering optional dimension " + dim + " as,"
              + " No dimension table has the queried columns:" + optdim.colQueried
              + " Clearing the required for candidates:" + optdim.requiredForCandidates);
            removeOptionalDim(cubeql, dim);
          }
        }
      }
    }
  }

  private void resolveCandidateFactTablesForJoins(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getAutoJoinCtx() == null) {
      return;
    }
    Collection<String> colSet = null;
    if (cubeql.getCube() != null && !cubeql.getCandidateFactTables().isEmpty()) {
      for (Iterator<CandidateFact> i = cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
        CandidateFact cfact = i.next();
        CubeFactTable fact = cfact.fact;

        // for each join path check for columns involved in path
        for (Map.Entry<Dimension, Map<AbstractCubeTable, List<String>>> joincolumnsEntry : cubeql.getAutoJoinCtx()
          .getJoinPathFromColumns().entrySet()) {
          Dimension reachableDim = joincolumnsEntry.getKey();
          OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(reachableDim);
          colSet = joincolumnsEntry.getValue().get((AbstractCubeTable) cubeql.getCube());

          if (!checkForColumnExists(cfact, colSet)) {
            if (optdim == null || optdim.isRequiredInJoinChain
              || (optdim != null && optdim.requiredForCandidates.contains(cfact))) {
              i.remove();
              LOG.info("Not considering fact table:" + fact + " as it does not have columns"
                + " in any of the join paths. Join columns:" + colSet);
              cubeql.addFactPruningMsgs(fact, CandidateTablePruneCause.noColumnPartOfAJoinPath(colSet));
              break;
            }
          }
        }
      }
      if (cubeql.getCandidateFactTables().size() == 0) {
        throw new SemanticException(ErrorMsg.NO_FACT_HAS_COLUMN, colSet == null ? "NULL" : colSet.toString());
      }
    }
  }

  /**
   * This method checks if the source columns(resolved through automatic join resolver) for reaching the references are
   * available in candidate tables that want to use references
   */
  private void checkForSourceReachabilityForDenormCandidates(CubeQueryContext cubeql) {
    if (cubeql.getOptionalDimensionMap().isEmpty()) {
      return;
    }
    if (cubeql.getAutoJoinCtx() == null) {
      Set<Dimension> optionaldims = new HashSet<Dimension>(cubeql.getOptionalDimensions());
      for (Dimension dim : optionaldims) {
        LOG.info("Not considering optional dimension " + dim + " as," + " automatic join resolver is disbled ");
        removeOptionalDim(cubeql, dim);
      }
      return;
    }
    // check for source columns for denorm columns
    Map<CandidateTable, List<String>> removedCandidates = new HashMap<CandidateTable, List<String>>();
    for (Map.Entry<Dimension, OptionalDimCtx> optdimEntry : cubeql.getOptionalDimensionMap().entrySet()) {
      Dimension dim = optdimEntry.getKey();
      OptionalDimCtx optdim = optdimEntry.getValue();
      Iterator<CandidateTable> iter = optdim.requiredForCandidates.iterator();
      while (iter.hasNext()) {
        CandidateTable candidate = iter.next();
        List<String> colSet = cubeql.getAutoJoinCtx().getJoinPathFromColumns().get(dim).get(candidate.getBaseTable());
        if (!checkForColumnExists(candidate, colSet)) {
          LOG.info("Removing candidate" + candidate + " from requiredForCandidates of" + dim + ", as columns:" + colSet
            + " do not exist");
          iter.remove();
          removedCandidates.put(candidate, colSet);
        }
      }
    }
    Set<CandidateTable> candidatesReachableThroughRefs = new HashSet<CandidateTable>();
    Set<Dimension> tobeRemoved = new HashSet<Dimension>();
    for (Map.Entry<Dimension, OptionalDimCtx> optdimEntry : cubeql.getOptionalDimensionMap().entrySet()) {
      Dimension dim = optdimEntry.getKey();
      OptionalDimCtx optdim = optdimEntry.getValue();
      candidatesReachableThroughRefs.addAll(optdim.requiredForCandidates);
      if ((!optdim.colQueried.isEmpty() && optdim.requiredForCandidates.isEmpty()) && !optdim.isRequiredInJoinChain) {
        LOG.info("Not considering optional dimension " + dim + " as all requiredForCandidates are removed");
        tobeRemoved.add(dim);
      }
    }
    for (Dimension dim : tobeRemoved) {
      removeOptionalDim(cubeql, dim);
    }
    for (CandidateTable candidate : removedCandidates.keySet()) {
      if (!candidatesReachableThroughRefs.contains(candidate)) {
        if (candidate instanceof CandidateFact) {
          if (cubeql.getCandidateFactTables().contains(candidate)) {
            LOG.info("Not considering fact:" + candidate + " as is not reachable through any optional dim");
            cubeql.getCandidateFactTables().remove(candidate);
            cubeql.addFactPruningMsgs(((CandidateFact) candidate).fact,
              CandidateTablePruneCause.columnNotFound(removedCandidates.get(candidate)));
          }
        } else if (cubeql.getCandidateDimTables().containsKey(((CandidateDim) candidate).getBaseTable())) {
          LOG.info("Not considering dimtable:" + candidate + " as is not reachable through any optional dim");
          cubeql.getCandidateDimTables().get(((CandidateDim) candidate).getBaseTable()).remove(candidate);
          cubeql.addDimPruningMsgs((Dimension) candidate.getBaseTable(), (CubeDimensionTable) candidate.getTable(),
            CandidateTablePruneCause.columnNotFound(removedCandidates.get(candidate)));
        }
      }
    }
  }

  private void resolveCandidateDimTables(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getDimensions().size() != 0) {
      for (Dimension dim : cubeql.getDimensions()) {
        // go over the columns accessed in the query and find out which tables
        // can answer the query
        for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator(); i.hasNext();) {
          CandidateDim cdim = i.next();
          if (cubeql.getColumnsQueried(dim.getName()) != null) {
            for (String col : cubeql.getColumnsQueried(dim.getName())) {
              if (!cdim.getColumns().contains(col.toLowerCase())) {
                // check if it available as reference, if not remove the
                // candidate
                if (!cubeql.getDeNormCtx().addRefUsage(cdim, col, dim.getName())) {
                  LOG.info("Not considering dimtable:" + cdim + " as column " + col + " is not available");
                  cubeql.addDimPruningMsgs(dim, cdim.getTable(), CandidateTablePruneCause.columnNotFound(col));
                  i.remove();
                  break;
                }
              }
            }
          }
        }

        if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
          throw new SemanticException(ErrorMsg.NO_DIM_HAS_COLUMN, dim.getName(), cubeql
            .getColumnsQueried(dim.getName()).toString());
        }
      }
    }
  }

  // The candidate table contains atleast one column in the colSet
  static boolean checkForColumnExists(CandidateTable table, Collection<String> colSet) {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    for (String column : colSet) {
      if (table.getColumns().contains(column)) {
        return true;
      }
    }
    return false;
  }
}
