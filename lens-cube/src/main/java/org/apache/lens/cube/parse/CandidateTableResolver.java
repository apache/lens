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
import org.apache.lens.cube.parse.ExpressionResolver.ExpressionContext;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
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

  private final List<String> supportedStorages;
  private final boolean allStoragesSupported;
  private boolean checkForQueriedColumns = true;

  public CandidateTableResolver(Configuration conf) {
    this.supportedStorages = getSupportedStorages(conf);
    this.allStoragesSupported = (supportedStorages == null);
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
        cubeql.getAutoJoinCtx().pruneAllPaths(cubeql.getCube(),
            CandidateUtil.getColumnsFromCandidates(cubeql.getCandidates()), null);
        cubeql.getAutoJoinCtx().pruneAllPathsForCandidateDims(cubeql.getCandidateDimTables());
        cubeql.getAutoJoinCtx().refreshJoinPathColumns();
      }
      checkForSourceReachabilityForDenormCandidates(cubeql);
      // check for joined columns and denorm columns on refered tables
      resolveCandidateFactTablesForJoins(cubeql);
      resolveCandidateDimTablesForJoinsAndDenorms(cubeql);
      checkForQueriedColumns = true;
    }
  }
  private List<String> getSupportedStorages(Configuration conf) {
    String[] storages = conf.getStrings(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES);
    if (storages != null) {
      return Arrays.asList(storages);
    }
    return null;
  }

  private boolean isStorageSupportedOnDriver(String storage) {
    return allStoragesSupported || supportedStorages.contains(storage);
  }


  private void populateCandidateTables(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() != null) {
      String str = cubeql.getConf().get(CubeQueryConfUtil.getValidFactTablesKey(cubeql.getCube().getName()));
      List<String> validFactTables =
        StringUtils.isBlank(str) ? null : Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
      List<FactTable> factTables = cubeql.getMetastoreClient().getAllFacts(cubeql.getCube());
      if (factTables.isEmpty()) {
        throw new LensException(LensCubeErrorCode.NO_CANDIDATE_FACT_AVAILABLE.getLensErrorInfo(),
            cubeql.getCube().getName() + " does not have any facts");
      }
      for (FactTable fact : factTables) {
        if (fact.getUpdatePeriods().isEmpty()) {
          log.info("Not considering fact: {} as it has no update periods", fact.getName());
        } else if (validFactTables != null && !validFactTables.contains(fact.getName())) {
          log.info("Not considering fact: {} as it's not valid as per user configuration.", fact.getName());
        } else {
          for (String s : fact.getStorages()) {
            StorageCandidate sc = new StorageCandidate(cubeql.getCube(), fact, s, cubeql);
            if (isStorageSupportedOnDriver(sc.getStorageName())) {
              cubeql.getCandidates().add(sc);
            } else {
              log.info("Not considering {} since storage is not supported on driver.", sc.getName());
            }
          }
        }
      }

      log.info("Populated storage candidates: {}", cubeql.getCandidates());
      List<SegmentationCandidate> segmentationCandidates = Lists.newArrayList();
      for (Segmentation segmentation : cubeql.getMetastoreClient().getAllSegmentations(cubeql.getCube())) {
        if (validFactTables != null && !validFactTables.contains(segmentation.getName())) {
          log.info("Not considering segmentation: {} as it's not valid as per user configuration.",
            segmentation.getName());
        } else {
          segmentationCandidates.add(new SegmentationCandidate(cubeql, segmentation));
        }
      }
      cubeql.getCandidates().addAll(segmentationCandidates);
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
      if (candidate instanceof StorageCandidate) {
        log.info("Not considering storage candidate:{} as refered table does not have any valid dimtables", candidate);
        cubeql.getCandidates().remove(candidate);
        cubeql.addStoragePruningMsg(((StorageCandidate) candidate), new CandidateTablePruneCause(
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

  private static boolean isColumnAvailableInRange(final TimeRange range, Date startTime, Date endTime) {
    return (isColumnAvailableFrom(range.getFromDate(), startTime)
        && isColumnAvailableTill(range.getToDate(), endTime));
  }

  private static boolean isColumnAvailableFrom(@NonNull final Date date, Date startTime) {
    return (startTime == null) || (date.equals(startTime) || date.after(startTime));
  }

  private static boolean isColumnAvailableTill(@NonNull final Date date, Date endTime) {
    return (endTime == null) || (date.equals(endTime) || date.before(endTime));
  }

  private static boolean isFactColumnValidForRange(CubeQueryContext cubeql, CandidateTable cfact, String col) {
    for(TimeRange range : cubeql.getTimeRanges()) {
      if (!isColumnAvailableInRange(range, getFactColumnStartTime(cfact, col), getFactColumnEndTime(cfact, col))) {
        return false;
      }
    }
    return true;
  }

  private static Date getFactColumnStartTime(CandidateTable table, String factCol) {
    Date startTime = null;
    if (table instanceof StorageCandidate) {
      for (String key : ((StorageCandidate) table).getFact().getProperties().keySet()) {
        if (key.contains(MetastoreConstants.FACT_COL_START_TIME_PFX)) {
          String propCol = StringUtils.substringAfter(key, MetastoreConstants.FACT_COL_START_TIME_PFX);
          if (factCol.equals(propCol)) {
            startTime = MetastoreUtil.getDateFromProperty(((StorageCandidate) table).getFact().getProperties().get(key),
              false, true);
          }
        }
      }
    }
    return startTime;
  }

  private static Date getFactColumnEndTime(CandidateTable table, String factCol) {
    Date endTime = null;
    if (table instanceof StorageCandidate) {
      for (String key : ((StorageCandidate) table).getFact().getProperties().keySet()) {
        if (key.contains(MetastoreConstants.FACT_COL_END_TIME_PFX)) {
          String propCol = StringUtils.substringAfter(key, MetastoreConstants.FACT_COL_END_TIME_PFX);
          if (factCol.equals(propCol)) {
            endTime = MetastoreUtil.getDateFromProperty(((StorageCandidate) table).getFact().getProperties().get(key),
              false, true);
          }
        }
      }
    }
    return endTime;
  }

  private void resolveCandidateFactTables(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() != null) {

      Set<QueriedPhraseContext> queriedMsrs = new HashSet<>();
      Set<QueriedPhraseContext> dimExprs = new HashSet<>();
      for (QueriedPhraseContext qur : cubeql.getQueriedPhrases()) {
        if (qur.hasMeasures(cubeql)) {
          queriedMsrs.add(qur);
        } else {
          dimExprs.add(qur);
        }
      }
      // Remove storage candidates based on whether they are valid or not.
      for (Iterator<Candidate> i = cubeql.getCandidates().iterator(); i.hasNext();) {
        Candidate cand = i.next();
        if (cand instanceof StorageCandidate) {
          StorageCandidate sc = (StorageCandidate) cand;
          // update expression evaluability for this fact
          for (String expr : cubeql.getQueriedExprs()) {
            cubeql.getExprCtx().updateEvaluables(expr, sc);
          }
        }
        // go over the columns accessed in the query and find out which tables
        // can answer the query
        // the candidate facts should have all the dimensions queried and
        // atleast
        // one measure
        boolean toRemove = false;
        for (QueriedPhraseContext qur : dimExprs) {
          if (!cand.isPhraseAnswerable(qur)) {
            log.info("Not considering storage candidate:{} as columns {} are not available", cand, qur.getColumns());
            cubeql.addStoragePruningMsg(cand, CandidateTablePruneCause.columnNotFound(
              qur.getColumns()));
            toRemove = true;
            break;
          }
        }

        // check if the candidate fact has atleast one measure queried
        // if expression has measures, they should be considered along with other measures and see if the fact can be
        // part of measure covering set
        if (!checkForFactColumnExistsAndValidForRange(cand, queriedMsrs)) {
          Set<String> columns = getColumns(queriedMsrs);
          log.info("Not considering storage candidate:{} as columns {} is not available", cand, columns);
          cubeql.addStoragePruningMsg(cand, CandidateTablePruneCause.columnNotFound(columns));
          toRemove = true;
        }

        // go over join chains and prune facts that dont have any of the columns in each chain
        if (cand instanceof StorageCandidate) {
          StorageCandidate sc = (StorageCandidate) cand;
          for (JoinChain chain : cubeql.getJoinchains().values()) {
            OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(Aliased.create((Dimension) cubeql.getCubeTbls()
              .get(chain.getName()), chain.getName()));
            if (!checkForFactColumnExistsAndValidForRange(sc, chain.getSourceColumns(), cubeql)) {
              // check if chain is optional or not
              if (optdim == null) {
                log.info("Not considering storage candidate:{} as columns {} are not available", sc,
                  chain.getSourceColumns());
                cubeql.addStoragePruningMsg(sc, CandidateTablePruneCause.columnNotFound(
                  chain.getSourceColumns()));
                toRemove = true;
                break;
              }
            }
          }
        }
        if (toRemove) {
          i.remove();
        }
      }
      if (cubeql.getCandidates().size() == 0) {
        throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(),
          getColumns(cubeql.getQueriedPhrases()).toString());
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
              if (optdim == null || optdim.isRequiredInJoinChain || optdim.requiredForCandidates.contains(cdim)) {
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
                if (optdim == null || optdim.isRequiredInJoinChain || optdim.requiredForCandidates.contains(cdim)) {
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
    if (cubeql.getCube() != null && !cubeql.getCandidates().isEmpty()) {
      for (Iterator<StorageCandidate> i =
           CandidateUtil.getStorageCandidates(cubeql.getCandidates()).iterator(); i.hasNext();) {
        StorageCandidate sc = i.next();
        // for each join path check for columns involved in path
        for (Map.Entry<Aliased<Dimension>, Map<AbstractCubeTable, List<String>>> joincolumnsEntry : cubeql
          .getAutoJoinCtx()
          .getJoinPathFromColumns().entrySet()) {
          Aliased<Dimension> reachableDim = joincolumnsEntry.getKey();
          OptionalDimCtx optdim = cubeql.getOptionalDimensionMap().get(reachableDim);
          colSet = joincolumnsEntry.getValue().get(cubeql.getCube());

          if (!checkForFactColumnExistsAndValidForRange(sc, colSet, cubeql)) {
            if (optdim == null || optdim.isRequiredInJoinChain || optdim.requiredForCandidates.contains(sc)) {
              i.remove();
              log.info("Not considering storage candidate :{} as it does not have columns in any of the join paths."
                + " Join columns:{}", sc, colSet);
              cubeql.addStoragePruningMsg(sc, CandidateTablePruneCause.noColumnPartOfAJoinPath(colSet));
              break;
            }
          }
        }
      }
      if (cubeql.getCandidates().size() == 0) {
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
              if (candidate instanceof StorageCandidate) {
                if (cubeql.getCandidates().contains(candidate)) {
                  log.info("Not considering Storage:{} as its required optional dims are not reachable", candidate);
                  cubeql.getCandidates().remove(candidate);
                  cubeql.addStoragePruningMsg((StorageCandidate) candidate,
                      CandidateTablePruneCause.columnNotFound(col));
                  Collection<Candidate> prunedCandidates = CandidateUtil.
                      filterCandidates(cubeql.getCandidates(), (StorageCandidate) candidate);
                  cubeql.addCandidatePruningMsg(prunedCandidates,
                      new CandidateTablePruneCause(CandidateTablePruneCode.ELEMENT_IN_SET_PRUNED));
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
              ec.getEvaluableExpressions().get(candidate).removeIf(esc -> esc.getExprDims().contains(dim.getObject()));
            }
            if (cubeql.getExprCtx().isEvaluable(col.getExprCol(), candidate)) {
              // candidate has other evaluable expressions
              continue;
            }
            if (candidate instanceof StorageCandidate) {
              if (cubeql.getCandidates().contains(candidate)) {
                log.info("Not considering fact:{} as is not reachable through any optional dim", candidate);
                cubeql.getCandidates().remove(candidate);
                cubeql.addStoragePruningMsg(((StorageCandidate) candidate),
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
                } else if (!cubeql.getDeNormCtx().addRefUsage(cubeql, cdim, col, dim.getName())) {
                  // check if it available as reference, if not remove the
                  // candidate
                  log.info("Not considering dimtable: {} as column {} is not available", cdim, col);
                  cubeql.addDimPruningMsgs(dim, cdim.getTable(), CandidateTablePruneCause.columnNotFound(
                    col));
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
  private static boolean checkForFactColumnExistsAndValidForRange(CandidateTable table, Collection<String> colSet,
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


  private static boolean checkForFactColumnExistsAndValidForRange(Candidate sc,
    Collection<QueriedPhraseContext> colSet) throws LensException {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    boolean isEvaluable = false;
    for (QueriedPhraseContext qur : colSet) {
      if (sc.isPhraseAnswerable(qur)) {
        isEvaluable = true;
        continue;
      }
    }
    return isEvaluable;
  }
}
