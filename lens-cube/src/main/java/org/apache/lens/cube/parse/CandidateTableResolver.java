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

import com.google.common.collect.Sets;

import lombok.NonNull;
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

  public CandidateTableResolver(Configuration ignored) {
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
      for (Aliased<Dimension> dim : cubeql.getOptionalDimensions()) {
        log.info("Populating optional dim:{}", dim);
        populateDimTables(dim.getObject(), cubeql, true);
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
  }

  private void populateDimTables(Dimension dim, CubeQueryContext cubeql, boolean optional) throws LensException {
    if (cubeql.getCandidateDimTables().get(dim) != null) {
      return;
    }
    Set<CandidateDim> candidates = new HashSet<>();
    cubeql.getCandidateDimTables().put(dim, candidates);
    List<CubeDimensionTable> dimtables = cubeql.getMetastoreClient().getAllDimensionTables(dim);
    if (dimtables.isEmpty()) {
      if (!optional) {
        throw new LensException(LensCubeErrorCode.NO_CANDIDATE_DIM_AVAILABLE.getLensErrorInfo(),
                dim.getName().concat(" has no dimension tables"));
      } else {
        log.info("Not considering optional dimension {}  as, No dimension tables exist", dim);
        removeOptionalDimWithoutAlias(cubeql, dim);
      }
    }
    for (CubeDimensionTable dimtable : dimtables) {
      CandidateDim cdim = new CandidateDim(dimtable, dim);
      candidates.add(cdim);
    }
    log.info("Populated candidate dims: {} for {}", cubeql.getCandidateDimTables().get(dim), dim);
  }

  private void removeOptionalDimWithoutAlias(CubeQueryContext cubeql, Dimension dim) {
    for (Aliased<Dimension> aDim : cubeql.getOptionalDimensions()) {
      if (aDim.getName().equals(dim.getName())) {
        removeOptionalDim(cubeql, aDim);
      }
    }
  }

  private void pruneOptionalDims(CubeQueryContext cubeql) {
    Set<Aliased<Dimension>> tobeRemoved = new HashSet<>();
    for (Map.Entry<Aliased<Dimension>, OptionalDimCtx> optdimEntry : cubeql.getOptionalDimensionMap().entrySet()) {
      Aliased<Dimension> dim = optdimEntry.getKey();
      OptionalDimCtx optdim = optdimEntry.getValue();
      if ((!optdim.colQueried.isEmpty() && optdim.requiredForCandidates.isEmpty()) && !optdim.isRequiredInJoinChain) {
        log.info("Not considering optional dimension {} as all requiredForCandidates are removed", dim);
        tobeRemoved.add(dim);
      }
    }
    for (Aliased<Dimension> dim : tobeRemoved) {
      removeOptionalDim(cubeql, dim);
    }
  }

  private void removeOptionalDim(CubeQueryContext cubeql, Aliased<Dimension> dim) {
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

  public static boolean isColumnAvailableInRange(final TimeRange range, Date startTime, Date endTime) {
    return (isColumnAvailableFrom(range.getFromDate(), startTime)
        && isColumnAvailableTill(range.getToDate(), endTime));
  }

  public static boolean isColumnAvailableFrom(@NonNull final Date date, Date startTime) {
    return (startTime == null) ? true : date.equals(startTime) || date.after(startTime);
  }

  public static boolean isColumnAvailableTill(@NonNull final Date date, Date endTime) {
    return (endTime == null) ? true : date.equals(endTime) || date.before(endTime);
  }

  public static boolean isFactColumnValidForRange(CubeQueryContext cubeql, CandidateTable cfact, String col) {
    for(TimeRange range : cubeql.getTimeRanges()) {
      if (!isColumnAvailableInRange(range, getFactColumnStartTime(cfact, col), getFactColumnEndTime(cfact, col))) {
        return false;
      }
    }
    return true;
  }

  public static Date getFactColumnStartTime(CandidateTable table, String factCol) {
    Date startTime = null;
    if (table instanceof CandidateFact) {
      for (String key : ((CandidateFact) table).fact.getProperties().keySet()) {
        if (key.contains(MetastoreConstants.FACT_COL_START_TIME_PFX)) {
          String propCol = StringUtils.substringAfter(key, MetastoreConstants.FACT_COL_START_TIME_PFX);
          if (factCol.equals(propCol)) {
            startTime = ((CandidateFact) table).fact.getDateFromProperty(key, false, true);
          }
        }
      }
    }
    return startTime;
  }

  public static Date getFactColumnEndTime(CandidateTable table, String factCol) {
    Date endTime = null;
    if (table instanceof CandidateFact) {
      for (String key : ((CandidateFact) table).fact.getProperties().keySet()) {
        if (key.contains(MetastoreConstants.FACT_COL_END_TIME_PFX)) {
          String propCol = StringUtils.substringAfter(key, MetastoreConstants.FACT_COL_END_TIME_PFX);
          if (factCol.equals(propCol)) {
            endTime = ((CandidateFact) table).fact.getDateFromProperty(key, false, true);
          }
        }
      }
    }
    return endTime;
  }

  private void resolveCandidateFactTables(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() != null) {
      String str = cubeql.getConf().get(CubeQueryConfUtil.getValidFactTablesKey(cubeql.getCube().getName()));
      List<String> validFactTables =
        StringUtils.isBlank(str) ? null : Arrays.asList(StringUtils.split(str.toLowerCase(), ","));

      Set<QueriedPhraseContext> queriedMsrs = new HashSet<>();
      Set<QueriedPhraseContext> dimExprs = new HashSet<>();
      for (QueriedPhraseContext qur : cubeql.getQueriedPhrases()) {
        if (qur.hasMeasures(cubeql)) {
          queriedMsrs.add(qur);
        } else {
          dimExprs.add(qur);
        }
      }
      // Remove fact tables based on whether they are valid or not.
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

        // update expression evaluability for this fact
        for (String expr : cubeql.getQueriedExprs()) {
          cubeql.getExprCtx().updateEvaluables(expr, cfact);
        }

        // go over the columns accessed in the query and find out which tables
        // can answer the query
        // the candidate facts should have all the dimensions queried and
        // atleast
        // one measure
        boolean toRemove = false;
        for (QueriedPhraseContext qur : dimExprs) {
          if (!qur.isEvaluable(cubeql, cfact)) {
            log.info("Not considering fact table:{} as columns {} are not available", cfact, qur.getColumns());
            cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.columnNotFound(qur.getColumns()));
            toRemove = true;
            break;
          }
        }

        // check if the candidate fact has atleast one measure queried
        // if expression has measures, they should be considered along with other measures and see if the fact can be
        // part of measure covering set
        if (!checkForFactColumnExistsAndValidForRange(cfact, queriedMsrs, cubeql)) {
          Set<String> columns = getColumns(queriedMsrs);

          log.info("Not considering fact table:{} as columns {} is not available", cfact, columns);
          cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.columnNotFound(columns));
          toRemove = true;
        }
        // go over join chains and prune facts that dont have any of the columns in each chain
        for (JoinChain chain : cubeql.getJoinchains().values()) {
          OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(Aliased.create((Dimension)cubeql.getCubeTbls()
            .get(chain.getName()), chain.getName()));
          if (!checkForFactColumnExistsAndValidForRange(cfact, chain.getSourceColumns(), cubeql)) {
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

        if (toRemove) {
          i.remove();
        }
      }
      if (cubeql.getCandidateFacts().size() == 0) {
        throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(),
          getColumns(cubeql.getQueriedPhrases()).toString());
      }
      Set<Set<CandidateFact>> cfactset;
      if (queriedMsrs.isEmpty()) {
        // if no measures are queried, add all facts individually as single covering sets
        cfactset = new HashSet<>();
        for (CandidateFact cfact : cubeql.getCandidateFacts()) {
          Set<CandidateFact> one = new LinkedHashSet<>();
          one.add(cfact);
          cfactset.add(one);
        }
        cubeql.getCandidateFactSets().addAll(cfactset);
      } else {
        // Find out candidate fact table sets which contain all the measures
        // queried

        List<CandidateFact> cfacts = new ArrayList<>(cubeql.getCandidateFacts());
        cfactset = findCoveringSets(cubeql, cfacts, queriedMsrs);
        log.info("Measure covering fact sets :{}", cfactset);
        String msrString = getColumns(queriedMsrs).toString();
        if (cfactset.isEmpty()) {
          throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(), msrString);
        }
        cubeql.getCandidateFactSets().addAll(cfactset);
        cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCause.columnNotFound(getColumns(queriedMsrs)));

        if (cubeql.getCandidateFacts().size() == 0) {
          throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(), msrString);
        }
      }
    }
  }

  private static Set<String> getColumns(Collection<QueriedPhraseContext> queriedPhraseContexts) {
    Set<String> cols = new HashSet<>();
    for (QueriedPhraseContext qur : queriedPhraseContexts) {
      cols.addAll(qur.getColumns());
    }
    return cols;
  }
  static Set<Set<CandidateFact>> findCoveringSets(CubeQueryContext cubeql, List<CandidateFact> cfactsPassed,
    Set<QueriedPhraseContext> msrs) throws LensException {
    Set<Set<CandidateFact>> cfactset = new HashSet<>();
    List<CandidateFact> cfacts = new ArrayList<>(cfactsPassed);
    for (Iterator<CandidateFact> i = cfacts.iterator(); i.hasNext();) {
      CandidateFact cfact = i.next();
      i.remove();
      if (!checkForFactColumnExistsAndValidForRange(cfact, msrs, cubeql)) {
        // cfact does not contain any of msrs and none of exprsWithMeasures are evaluable.
        // ignore the fact
        continue;
      } else if (allEvaluable(cfact, msrs, cubeql)) {
        // return single set
        Set<CandidateFact> one = new LinkedHashSet<>();
        one.add(cfact);
        cfactset.add(one);
      } else {
        // find the remaining measures in other facts
        if (i.hasNext()) {
          Set<QueriedPhraseContext> remainingMsrs = new HashSet<>(msrs);
          Set<QueriedPhraseContext> coveredMsrs  = coveredMeasures(cfact, msrs, cubeql);
          remainingMsrs.removeAll(coveredMsrs);

          Set<Set<CandidateFact>> coveringSets = findCoveringSets(cubeql, cfacts, remainingMsrs);
          if (!coveringSets.isEmpty()) {
            for (Set<CandidateFact> set : coveringSets) {
              set.add(cfact);
              cfactset.add(set);
            }
          } else {
            log.info("Couldnt find any set containing remaining measures:{} {} in {}", remainingMsrs,
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
    Set<Aliased<Dimension>> allDims = new HashSet<>();
    for (Dimension dim : cubeql.getDimensions()) {
      allDims.add(Aliased.create(dim));
    }
    allDims.addAll(cubeql.getOptionalDimensions());
    for (Aliased<Dimension> aliasedDim : allDims) {
      Dimension dim = aliasedDim.getObject();
      if (cubeql.getCandidateDimTables().get(dim) != null && !cubeql.getCandidateDimTables().get(dim).isEmpty()) {
        for (Iterator<CandidateDim> i = cubeql.getCandidateDimTables().get(dim).iterator(); i.hasNext();) {
          CandidateDim cdim = i.next();
          CubeDimensionTable dimtable = cdim.dimtable;
          // go over the join columns accessed in the query and find out which tables
          // can participate in join
          // for each join path check for columns involved in path
          boolean removed = false;
          for (Map.Entry<Aliased<Dimension>, Map<AbstractCubeTable, List<String>>> joincolumnsEntry : cubeql
            .getAutoJoinCtx().getJoinPathFromColumns().entrySet()) {
            Aliased<Dimension> reachableDim = joincolumnsEntry.getKey();
            OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(reachableDim);
            Collection<String> colSet = joincolumnsEntry.getValue().get(dim);

            if (!checkForFactColumnExistsAndValidForRange(cdim, colSet, cubeql)) {
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
            for (Map.Entry<Aliased<Dimension>, Map<AbstractCubeTable, List<String>>> joincolumnsEntry : cubeql
              .getAutoJoinCtx().getJoinPathToColumns().entrySet()) {
              Aliased<Dimension> reachableDim = joincolumnsEntry.getKey();
              OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(reachableDim);
              Collection<String> colSet = joincolumnsEntry.getValue().get(dim);

              if (!checkForFactColumnExistsAndValidForRange(cdim, colSet, cubeql)) {
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
            if (cubeql.getOptionalDimensionMap().get(aliasedDim) != null
              && !checkForFactColumnExistsAndValidForRange(cdim,
                cubeql.getOptionalDimensionMap().get(aliasedDim).colQueried, cubeql)) {
              i.remove();
              log.info("Not considering optional dimtable:{} as its denorm fields do not exist. Denorm fields:{}",
                dimtable, cubeql.getOptionalDimensionMap().get(aliasedDim).colQueried);
              cubeql.addDimPruningMsgs(dim, dimtable, CandidateTablePruneCause
                .noColumnPartOfAJoinPath(cubeql.getOptionalDimensionMap().get(aliasedDim).colQueried));
            }
          }
        }
        if (cubeql.getCandidateDimTables().get(dim).size() == 0) {
          OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(aliasedDim);
          if ((cubeql.getDimensions() != null && cubeql.getDimensions().contains(dim))
            || (optdim != null && optdim.isRequiredInJoinChain)) {
            throw new LensException(LensCubeErrorCode.NO_DIM_HAS_COLUMN.getLensErrorInfo(), dim.getName(),
                cubeql.getAutoJoinCtx().getAllJoinPathColumnsOfTable(dim).toString());
          } else {
            // remove it from optional tables
            log.info("Not considering optional dimension {} as, No dimension table has the queried columns:{}"
              + " Clearing the required for candidates:{}", dim, optdim.colQueried, optdim.requiredForCandidates);
            removeOptionalDim(cubeql, aliasedDim);
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
        for (Map.Entry<Aliased<Dimension>, Map<AbstractCubeTable, List<String>>> joincolumnsEntry : cubeql
          .getAutoJoinCtx()
          .getJoinPathFromColumns().entrySet()) {
          Aliased<Dimension> reachableDim = joincolumnsEntry.getKey();
          OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(reachableDim);
          colSet = joincolumnsEntry.getValue().get(cubeql.getCube());

          if (!checkForFactColumnExistsAndValidForRange(cfact, colSet, cubeql)) {
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
    if (cubeql.getOptionalDimensions().isEmpty()) {
      return;
    }
    if (cubeql.getAutoJoinCtx() == null) {
      Set<Aliased<Dimension>> optionaldims = new HashSet<>(cubeql.getOptionalDimensions());
      for (Aliased<Dimension> dim : optionaldims) {
        log.info("Not considering optional dimension {} as, automatic join resolver is disbled ", dim);
        removeOptionalDim(cubeql, dim);
      }
      return;
    }
    // check for source columns for denorm columns
    Map<Aliased<Dimension>, Set<CandidateTable>> removedCandidates = new HashMap<>();
    for (Map.Entry<Aliased<Dimension>, OptionalDimCtx> optdimEntry : cubeql.getOptionalDimensionMap().entrySet()) {
      Aliased<Dimension> dim = optdimEntry.getKey();
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
          if (!checkForFactColumnExistsAndValidForRange(candidate, colSet, cubeql)) {
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
    for (Map.Entry<String, Set<Aliased<Dimension>>> dimColEntry : cubeql.getRefColToDim().entrySet()) {
      Set<CandidateTable> candidatesReachableThroughRefs = new HashSet<>();
      String col = dimColEntry.getKey();
      Set<Aliased<Dimension>> dimSet = dimColEntry.getValue();
      for (Aliased<Dimension> dim : dimSet) {
        OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(dim);
        if (optdim != null) {
          candidatesReachableThroughRefs.addAll(optdim.requiredForCandidates);
        }
      }
      for (Aliased<Dimension> dim : dimSet) {
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
    for (Map.Entry<QueriedExprColumn, Set<Aliased<Dimension>>> exprColEntry : cubeql.getExprColToDim().entrySet()) {
      QueriedExprColumn col = exprColEntry.getKey();
      Set<Aliased<Dimension>> dimSet = exprColEntry.getValue();
      ExpressionContext ec = cubeql.getExprCtx().getExpressionContext(col.getExprCol(), col.getAlias());
      for (Aliased<Dimension> dim : dimSet) {
        if (removedCandidates.get(dim) != null) {
          for (CandidateTable candidate : removedCandidates.get(dim)) {
            // check if evaluable expressions of this candidate are no more evaluable because dimension is not reachable
            // if no evaluable expressions exist, then remove the candidate
            if (ec.getEvaluableExpressions().get(candidate) != null) {
              Iterator<ExprSpecContext> escIter = ec.getEvaluableExpressions().get(candidate).iterator();
              while (escIter.hasNext()) {
                ExprSpecContext esc = escIter.next();
                if (esc.getExprDims().contains(dim.getObject())) {
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
    Set<Aliased<Dimension>> tobeRemoved = new HashSet<>();
    for (Map.Entry<Aliased<Dimension>, OptionalDimCtx> optdimEntry : cubeql.getOptionalDimensionMap().entrySet()) {
      Aliased<Dimension> dim = optdimEntry.getKey();
      OptionalDimCtx optdim = optdimEntry.getValue();
      if ((!optdim.colQueried.isEmpty() && optdim.requiredForCandidates.isEmpty()) && !optdim.isRequiredInJoinChain) {
        log.info("Not considering optional dimension {} as all requiredForCandidates are removed", dim);
        tobeRemoved.add(dim);
      }
    }
    for (Aliased<Dimension> dim : tobeRemoved) {
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
          if (cubeql.getColumnsQueriedForTable(dim.getName()) != null) {
            for (String col : cubeql.getColumnsQueriedForTable(dim.getName())) {
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
            .getColumnsQueriedForTable(dim.getName()).toString());
        }
      }
    }
  }

  // The candidate table contains atleast one column in the colSet and
  // column can the queried in the range specified
  static boolean checkForFactColumnExistsAndValidForRange(CandidateTable table, Collection<String> colSet,
                                                          CubeQueryContext cubeql) {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    for (String column : colSet) {
      if (table.getColumns().contains(column) &&  isFactColumnValidForRange(cubeql, table, column)) {
        return true;
      }
    }
    return false;
  }

  static boolean checkForFactColumnExistsAndValidForRange(CandidateFact table, Collection<QueriedPhraseContext> colSet,
                                                          CubeQueryContext cubeql) throws LensException {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    for (QueriedPhraseContext qur : colSet) {
      if (qur.isEvaluable(cubeql, table)) {
        return true;
      }
    }
    return false;
  }

  static boolean allEvaluable(CandidateFact table, Collection<QueriedPhraseContext> colSet,
                                                          CubeQueryContext cubeql) throws LensException {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    for (QueriedPhraseContext qur : colSet) {
      if (!qur.isEvaluable(cubeql, table)) {
        return false;
      }
    }
    return true;
  }

  static Set<QueriedPhraseContext> coveredMeasures(CandidateFact table, Collection<QueriedPhraseContext> msrs,
                              CubeQueryContext cubeql) throws LensException {
    Set<QueriedPhraseContext> coveringSet = new HashSet<>();
    for (QueriedPhraseContext msr : msrs) {
      if (msr.isEvaluable(cubeql, table)) {
        coveringSet.add(msr);
      }
    }
    return coveringSet;
  }
}
