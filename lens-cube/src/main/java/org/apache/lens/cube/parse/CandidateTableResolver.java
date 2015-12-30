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

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.cube.parse.CubeQueryContext.OptionalDimCtx;
import org.apache.lens.cube.parse.CubeQueryContext.QueriedExprColumn;
import org.apache.lens.cube.parse.ExpressionResolver.ExprSpecContext;
import org.apache.lens.cube.parse.ExpressionResolver.ExpressionContext;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

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
@Slf4j
class CandidateTableResolver implements ContextRewriter {

  private boolean checkForQueriedColumns = true;

  public CandidateTableResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    if (checkForQueriedColumns) {
      log.debug("Dump queried columns:{}", cubeql.getTblAliasToColumns());
      populateCandidateTables(cubeql);
      resolveCandidateFactTables(cubeql);
      resolveCandidateDimTables(cubeql);
      // remove optional dims added whom requiredForCandidates have been removed
      pruneOptionalDims(cubeql);
      checkForQueriedColumns = false;
    } else {
      // populate optional tables
      for (Dimension dim : cubeql.getOptionalDimensions()) {
        log.info("Populating optional dim:{}", dim);
        populateDimTables(dim, cubeql, true);
      }
      if (cubeql.getAutoJoinCtx() != null) {
        // Before checking for candidate table columns, prune join paths containing non existing columns
        // in populated candidate tables
        cubeql.getAutoJoinCtx().pruneAllPaths(cubeql.getCube(), cubeql.getCandidateFacts(), null);
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

  private void populateCandidateTables(CubeQueryContext cubeql) throws LensException {
    try {
      if (cubeql.getCube() != null) {
        List<CubeFactTable> factTables = cubeql.getMetastoreClient().getAllFacts(cubeql.getCube());
        if (factTables.isEmpty()) {
          throw new LensException(LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo(),
              cubeql.getCube().getName() + " does not have any facts");
        }
        for (CubeFactTable fact : factTables) {
          CandidateFact cfact = new CandidateFact(fact, cubeql.getCube());
          cubeql.getCandidateFacts().add(cfact);
        }
        log.info("Populated candidate facts: {}", cubeql.getCandidateFacts());
      }

      if (cubeql.getDimensions().size() != 0) {
        for (Dimension dim : cubeql.getDimensions()) {
          populateDimTables(dim, cubeql, false);
        }
      }
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  private void populateDimTables(Dimension dim, CubeQueryContext cubeql, boolean optional) throws LensException {
    if (cubeql.getCandidateDimTables().get(dim) != null) {
      return;
    }
    try {
      Set<CandidateDim> candidates = new HashSet<CandidateDim>();
      cubeql.getCandidateDimTables().put(dim, candidates);
      List<CubeDimensionTable> dimtables = cubeql.getMetastoreClient().getAllDimensionTables(dim);
      if (dimtables.isEmpty()) {
        if (!optional) {
          throw new LensException(LensCubeErrorCode.NO_CANDIDATE_DIM_AVAILABLE.getLensErrorInfo(), dim.getName(),
            "Dimension tables do not exist");
        } else {
          log.info("Not considering optional dimension {}  as, No dimension tables exist", dim);
          removeOptionalDim(cubeql, dim);
        }
      }
      for (CubeDimensionTable dimtable : dimtables) {
        CandidateDim cdim = new CandidateDim(dimtable, dim);
        candidates.add(cdim);
      }
      log.info("Populated candidate dims: {} for {}", cubeql.getCandidateDimTables().get(dim), dim);
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  private void pruneOptionalDims(CubeQueryContext cubeql) {
    Set<Dimension> tobeRemoved = new HashSet<Dimension>();
    Set<CandidateTable> allCandidates = new HashSet<CandidateTable>();
    allCandidates.addAll(cubeql.getCandidateFacts());
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
          log.info("Removing candidate {} from requiredForCandidates of {}, as it is no more candidate", candidate,
            dim);
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
        log.info("Not considering optional dimension {} as all requiredForCandidates are removed", dim);
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
        log.info("Not considering fact:{} as refered table does not have any valid dimtables", candidate);
        cubeql.getCandidateFacts().remove(candidate);
        cubeql.addFactPruningMsgs(((CandidateFact) candidate).fact, new CandidateTablePruneCause(
          CandidateTablePruneCode.INVALID_DENORM_TABLE));
      } else {
        log.info("Not considering dimtable:{} as refered table does not have any valid dimtables", candidate);
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

  private void resolveCandidateFactTables(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() != null) {
      String str = cubeql.getConf().get(CubeQueryConfUtil.getValidFactTablesKey(cubeql.getCube().getName()));
      List<String> validFactTables =
        StringUtils.isBlank(str) ? null : Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
      Set<String> queriedDimAttrs = cubeql.getQueriedDimAttrs();
      Set<String> queriedMsrs = cubeql.getQueriedMsrs();

      // Remove fact tables based on columns in the query
      for (Iterator<CandidateFact> i = cubeql.getCandidateFacts().iterator(); i.hasNext();) {
        CandidateFact cfact = i.next();

        if (validFactTables != null) {
          if (!validFactTables.contains(cfact.getName().toLowerCase())) {
            log.info("Not considering fact table:{} as it is not a valid fact", cfact);
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
              log.info("Not considering fact table:{} as column {} is not available", cfact, col);
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
            if (optdim == null) {
              log.info("Not considering fact table:{} as columns {} are not available", cfact,
                chain.getSourceColumns());
              cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.columnNotFound(chain.getSourceColumns()));
              toRemove = true;
              break;
            }
          }
        }
        // go over expressions queried
        // if expression has no measures, prune facts which cannot evaluate expression
        // if expression has measures, they should be considered along with other measures and see if the fact can be
        // part of measure covering set
        for (String expr : cubeql.getQueriedExprs()) {
          cubeql.getExprCtx().updateEvaluables(expr, cfact);
          if (!cubeql.getQueriedExprsWithMeasures().contains(expr) && !cubeql.getExprCtx().isEvaluable(expr, cfact)) {
            // if expression has no measures, prune facts which cannot evaluate expression
            log.info("Not considering fact table:{} as expression {} is not evaluatable", cfact, expr);
            cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.expressionNotEvaluable(expr));
            toRemove = true;
            break;
          }
        }
        // check if the candidate fact has atleast one measure queried
        // if expression has measures, they should be considered along with other measures and see if the fact can be
        // part of measure covering set
        if (!checkForColumnExists(cfact, queriedMsrs)
          && (cubeql.getQueriedExprsWithMeasures().isEmpty()
            || cubeql.getExprCtx().allNotEvaluable(cubeql.getQueriedExprsWithMeasures(), cfact))) {
          log.info("Not considering fact table:{} as columns {},{} is not available", cfact, queriedMsrs,
                  cubeql.getQueriedExprsWithMeasures());
          cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.columnNotFound(queriedMsrs,
            cubeql.getQueriedExprsWithMeasures()));
          toRemove = true;
        }
        if (toRemove) {
          i.remove();
        }
      }
      Set<String> dimExprs = new HashSet<String>(cubeql.getQueriedExprs());
      dimExprs.removeAll(cubeql.getQueriedExprsWithMeasures());
      if (cubeql.getCandidateFacts().size() == 0) {
        throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(),
          (!queriedDimAttrs.isEmpty() ? queriedDimAttrs.toString() : "")
          +  (!dimExprs.isEmpty() ? dimExprs.toString() : ""));
      }
      Set<Set<CandidateFact>> cfactset;
      if (queriedMsrs.isEmpty() && cubeql.getQueriedExprsWithMeasures().isEmpty()) {
        // if no measures are queried, add all facts individually as single covering sets
        cfactset = new HashSet<Set<CandidateFact>>();
        for (CandidateFact cfact : cubeql.getCandidateFacts()) {
          Set<CandidateFact> one = new LinkedHashSet<CandidateFact>();
          one.add(cfact);
          cfactset.add(one);
        }
        cubeql.getCandidateFactSets().addAll(cfactset);
      } else {
        // Find out candidate fact table sets which contain all the measures
        // queried
        List<CandidateFact> cfacts = new ArrayList<CandidateFact>(cubeql.getCandidateFacts());
        cfactset = findCoveringSets(cubeql, cfacts, queriedMsrs,
          cubeql.getQueriedExprsWithMeasures());
        log.info("Measure covering fact sets :{}", cfactset);
        String msrString = (!queriedMsrs.isEmpty() ? queriedMsrs.toString() : "")
          + (!cubeql.getQueriedExprsWithMeasures().isEmpty() ? cubeql.getQueriedExprsWithMeasures().toString() : "");
        if (cfactset.isEmpty()) {
          throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(), msrString);
        }
        cubeql.getCandidateFactSets().addAll(cfactset);
        cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCause.columnNotFound(queriedMsrs,
          cubeql.getQueriedExprsWithMeasures()));

        if (cubeql.getCandidateFacts().size() == 0) {
          throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(), msrString);
        }
      }
    }
  }

  static Set<Set<CandidateFact>> findCoveringSets(CubeQueryContext cubeql, List<CandidateFact> cfactsPassed,
    Set<String> msrs, Set<String> exprsWithMeasures) {
    Set<Set<CandidateFact>> cfactset = new HashSet<Set<CandidateFact>>();
    List<CandidateFact> cfacts = new ArrayList<CandidateFact>(cfactsPassed);
    for (Iterator<CandidateFact> i = cfacts.iterator(); i.hasNext();) {
      CandidateFact cfact = i.next();
      i.remove();
      // cfact does not contain any of msrs and none of exprsWithMeasures are evaluable.
      if ((msrs.isEmpty() || !checkForColumnExists(cfact, msrs))
        && (exprsWithMeasures.isEmpty() || cubeql.getExprCtx().allNotEvaluable(exprsWithMeasures, cfact))) {
        // ignore the fact
        continue;
      } else if (cfact.getColumns().containsAll(msrs) && cubeql.getExprCtx().allEvaluable(cfact, exprsWithMeasures)) {
        // return single set
        Set<CandidateFact> one = new LinkedHashSet<CandidateFact>();
        one.add(cfact);
        cfactset.add(one);
      } else {
        // find the remaining measures in other facts
        if (i.hasNext()) {
          Set<String> remainingMsrs = new HashSet<String>(msrs);
          Set<String> remainingExprs = new HashSet<String>(exprsWithMeasures);
          remainingMsrs.removeAll(cfact.getColumns());
          remainingExprs.removeAll(cubeql.getExprCtx().coveringExpressions(exprsWithMeasures, cfact));
          Set<Set<CandidateFact>> coveringSets = findCoveringSets(cubeql, cfacts, remainingMsrs, remainingExprs);
          if (!coveringSets.isEmpty()) {
            for (Set<CandidateFact> set : coveringSets) {
              set.add(cfact);
              cfactset.add(set);
            }
          } else {
            log.info("Couldnt find any set containing remaining measures:{} {} in {}", remainingMsrs, remainingExprs,
              cfactsPassed);
          }
        }
      }
    }
    return cfactset;
  }

  private void resolveCandidateDimTablesForJoinsAndDenorms(CubeQueryContext cubeql) throws LensException {
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
                log.info("Not considering dimtable:{} as its columns are not part of any join paths. Join columns:{}",
                  dimtable, colSet);
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
                  log.info("Not considering dimtable:{} as its columns are not part of any join paths. Join columns:{}",
                    dimtable, colSet);
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
              log.info("Not considering optional dimtable:{} as its denorm fields do not exist. Denorm fields:{}",
                dimtable, cubeql.getOptionalDimensionMap().get(dim).colQueried);
              cubeql.addDimPruningMsgs(dim, dimtable,
                CandidateTablePruneCause.noColumnPartOfAJoinPath(cubeql.getOptionalDimensionMap().get(dim).colQueried));
            }
          }
        }
        if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
          OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(dim);
          if ((cubeql.getDimensions() != null && cubeql.getDimensions().contains(dim))
            || (optdim != null && optdim.isRequiredInJoinChain)) {
            throw new LensException(LensCubeErrorCode.NO_DIM_HAS_COLUMN.getLensErrorInfo(), dim.getName(),
                cubeql.getAutoJoinCtx().getAllJoinPathColumnsOfTable(dim).toString());
          } else {
            // remove it from optional tables
            log.info("Not considering optional dimension {} as, No dimension table has the queried columns:{}"
              + " Clearing the required for candidates:{}", dim, optdim.colQueried, optdim.requiredForCandidates);
            removeOptionalDim(cubeql, dim);
          }
        }
      }
    }
  }

  private void resolveCandidateFactTablesForJoins(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getAutoJoinCtx() == null) {
      return;
    }
    Collection<String> colSet = null;
    if (cubeql.getCube() != null && !cubeql.getCandidateFacts().isEmpty()) {
      for (Iterator<CandidateFact> i = cubeql.getCandidateFacts().iterator(); i.hasNext();) {
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
              log.info("Not considering fact table:{} as it does not have columns in any of the join paths."
                + " Join columns:{}", fact, colSet);
              cubeql.addFactPruningMsgs(fact, CandidateTablePruneCause.noColumnPartOfAJoinPath(colSet));
              break;
            }
          }
        }
      }
      if (cubeql.getCandidateFacts().size() == 0) {
        throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(),
            colSet == null ? "NULL" : colSet.toString());
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
        log.info("Not considering optional dimension {} as, automatic join resolver is disbled ", dim);
        removeOptionalDim(cubeql, dim);
      }
      return;
    }
    // check for source columns for denorm columns
    Map<Dimension, Set<CandidateTable>> removedCandidates = new HashMap<Dimension, Set<CandidateTable>>();
    for (Map.Entry<Dimension, OptionalDimCtx> optdimEntry : cubeql.getOptionalDimensionMap().entrySet()) {
      Dimension dim = optdimEntry.getKey();
      OptionalDimCtx optdim = optdimEntry.getValue();
      Iterator<CandidateTable> iter = optdim.requiredForCandidates.iterator();
      // remove candidates from each optional dim if the dimension is not reachable from candidate
      Set<CandidateTable> cTableSet = Sets.newHashSet();
      removedCandidates.put(dim, cTableSet);
      while (iter.hasNext()) {
        CandidateTable candidate = iter.next();
        boolean remove = false;
        if (cubeql.getAutoJoinCtx().getJoinPathFromColumns().get(dim) == null) {
          log.info("Removing candidate {} from requiredForCandidates of {}, as no join paths exist", candidate, dim);
          remove = true;
        } else {
          List<String> colSet = cubeql.getAutoJoinCtx().getJoinPathFromColumns().get(dim).get(candidate.getBaseTable());
          if (!checkForColumnExists(candidate, colSet)) {
            log.info("Removing candidate {} from requiredForCandidates of {}, as columns:{} do not exist", candidate,
              dim, colSet);
            remove = true;
          }
        }
        if (remove) {
          iter.remove();
          cTableSet.add(candidate);
        }
      }
    }
    // For each ref column required in cube query
    // remove candidates which are not reachable
    // For example:
    // CTable | Col1 | Col2
    // F1 | reachable through D1 | Not reachable
    // F2 | Reachable through D2 |  Reachable through D5
    // F3 | Not reachable | Not reachable
    // F4 | Not reachable | Reachable through D6
    // F5 | Directly available | Directly available
    // F6 | Directly available | Not reachable
    // F3 and F4 will get pruned while iterating over col1 and F1, F6 will get pruned while iterating over col2.
    for (Map.Entry<String, Set<Dimension>> dimColEntry : cubeql.getRefColToDim().entrySet()) {
      Set<CandidateTable> candidatesReachableThroughRefs = new HashSet<CandidateTable>();
      String col = dimColEntry.getKey();
      Set<Dimension> dimSet = dimColEntry.getValue();
      for (Dimension dim : dimSet) {
        OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(dim);
        if (optdim != null) {
          candidatesReachableThroughRefs.addAll(optdim.requiredForCandidates);
        }
      }
      for (Dimension dim : dimSet) {
        if (removedCandidates.get(dim) != null) {
          for (CandidateTable candidate : removedCandidates.get(dim)) {
            if (!candidatesReachableThroughRefs.contains(candidate)) {
              if (candidate instanceof CandidateFact) {
                if (cubeql.getCandidateFacts().contains(candidate)) {
                  log.info("Not considering fact:{} as its required optional dims are not reachable", candidate);
                  cubeql.getCandidateFacts().remove(candidate);
                  cubeql.addFactPruningMsgs(((CandidateFact) candidate).fact,
                    CandidateTablePruneCause.columnNotFound(col));
                }
              } else if (cubeql.getCandidateDimTables().containsKey(((CandidateDim) candidate).getBaseTable())) {
                log.info("Not considering dimtable:{} as its required optional dims are not reachable", candidate);
                cubeql.getCandidateDimTables().get(((CandidateDim) candidate).getBaseTable()).remove(candidate);
                cubeql.addDimPruningMsgs((Dimension) candidate.getBaseTable(),
                  (CubeDimensionTable) candidate.getTable(),
                  CandidateTablePruneCause.columnNotFound(col));
              }
            }
          }
        }
      }
    }

    // For each expression column required in cube query
    // remove candidates in which expressions are not evaluable - for expression column all dimensions accessed in
    // expression should be reachable from candidate.
    // For example:
    // CTable | Col1 | Col2
    // F1 | evaluable through D1, D3 | Not evaluable
    // F2 | evaluable through D2, D4 |  evaluable through D5
    // F3 | Not evaluable | Not evaluable
    // F4 | Not evaluable | evaluable through D6
    // F5 | Directly available | Directly available
    // F6 | Directly available | Not evaluable
    for (Map.Entry<QueriedExprColumn, Set<Dimension>> exprColEntry : cubeql.getExprColToDim().entrySet()) {
      QueriedExprColumn col = exprColEntry.getKey();
      Set<Dimension> dimSet = exprColEntry.getValue();
      ExpressionContext ec = cubeql.getExprCtx().getExpressionContext(col.getExprCol(), col.getAlias());
      for (Dimension dim : dimSet) {
        if (removedCandidates.get(dim) != null) {
          for (CandidateTable candidate : removedCandidates.get(dim)) {
            // check if evaluable expressions of this candidate are no more evaluable because dimension is not reachable
            // if no evaluable expressions exist, then remove the candidate
            if (ec.getEvaluableExpressions().get(candidate) != null) {
              Iterator<ExprSpecContext> escIter = ec.getEvaluableExpressions().get(candidate).iterator();
              while (escIter.hasNext()) {
                ExprSpecContext esc = escIter.next();
                if (esc.getExprDims().contains(dim)) {
                  escIter.remove();
                }
              }
            }
            if (cubeql.getExprCtx().isEvaluable(col.getExprCol(), candidate)) {
              // candidate has other evaluable expressions
              continue;
            }
            if (candidate instanceof CandidateFact) {
              if (cubeql.getCandidateFacts().contains(candidate)) {
                log.info("Not considering fact:{} as is not reachable through any optional dim", candidate);
                cubeql.getCandidateFacts().remove(candidate);
                cubeql.addFactPruningMsgs(((CandidateFact) candidate).fact,
                  CandidateTablePruneCause.expressionNotEvaluable(col.getExprCol()));
              }
            } else if (cubeql.getCandidateDimTables().containsKey(((CandidateDim) candidate).getBaseTable())) {
              log.info("Not considering dimtable:{} as is not reachable through any optional dim", candidate);
              cubeql.getCandidateDimTables().get(((CandidateDim) candidate).getBaseTable()).remove(candidate);
              cubeql.addDimPruningMsgs((Dimension) candidate.getBaseTable(), (CubeDimensionTable) candidate.getTable(),
                CandidateTablePruneCause.expressionNotEvaluable(col.getExprCol()));
            }
          }
        }
      }
    }

    // remove optional dims which are not required any more.
    Set<Dimension> tobeRemoved = new HashSet<Dimension>();
    for (Map.Entry<Dimension, OptionalDimCtx> optdimEntry : cubeql.getOptionalDimensionMap().entrySet()) {
      Dimension dim = optdimEntry.getKey();
      OptionalDimCtx optdim = optdimEntry.getValue();
      if ((!optdim.colQueried.isEmpty() && optdim.requiredForCandidates.isEmpty()) && !optdim.isRequiredInJoinChain) {
        log.info("Not considering optional dimension {} as all requiredForCandidates are removed", dim);
        tobeRemoved.add(dim);
      }
    }
    for (Dimension dim : tobeRemoved) {
      removeOptionalDim(cubeql, dim);
    }
  }

  private void resolveCandidateDimTables(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getDimensions().size() != 0) {
      for (Dimension dim : cubeql.getDimensions()) {
        // go over the columns accessed in the query and find out which tables
        // can answer the query
        for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator(); i.hasNext();) {
          CandidateDim cdim = i.next();
          if (cubeql.getColumnsQueried(dim.getName()) != null) {
            for (String col : cubeql.getColumnsQueried(dim.getName())) {
              if (!cdim.getColumns().contains(col.toLowerCase())) {
                // check if the column is an expression
                if (cdim.getBaseTable().getExpressionNames().contains(col)) {
                  cubeql.getExprCtx().updateEvaluables(col, cdim);
                  // check if the expression is evaluable
                  if (!cubeql.getExprCtx().isEvaluable(col, cdim)) {
                    log.info("Not considering dimtable: {} as expression {} is not evaluable", cdim, col);
                    cubeql.addDimPruningMsgs(dim, cdim.getTable(), CandidateTablePruneCause.expressionNotEvaluable(
                      col));
                    i.remove();
                    break;
                  }
                } else if (!cubeql.getDeNormCtx().addRefUsage(cdim, col, dim.getName())) {
                  // check if it available as reference, if not remove the
                  // candidate
                  log.info("Not considering dimtable: {} as column {} is not available", cdim, col);
                  cubeql.addDimPruningMsgs(dim, cdim.getTable(), CandidateTablePruneCause.columnNotFound(col));
                  i.remove();
                  break;
                }
              }
            }
          }
        }

        if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
          throw new LensException(LensCubeErrorCode.NO_DIM_HAS_COLUMN.getLensErrorInfo(), dim.getName(), cubeql
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
