/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.parse;

import static org.apache.lens.cube.metadata.MetastoreUtil.getFactOrDimtableStorageTableName;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.TIMEDIM_NOT_SUPPORTED;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.TIME_RANGE_NOT_ANSWERABLE;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.noCandidateStorages;
import static org.apache.lens.cube.parse.StorageUtil.getFallbackRange;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.cube.parse.CandidateTablePruneCause.SkipStorageCause;
import org.apache.lens.cube.parse.CandidateTablePruneCause.SkipStorageCode;
import org.apache.lens.cube.parse.CandidateTablePruneCause.SkipUpdatePeriodCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.util.ReflectionUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Resolve storages and partitions of all candidate tables and prunes candidate tables with missing storages or
 * partitions.
 */
@Slf4j
class StorageTableResolver implements ContextRewriter {

  private final Configuration conf;
  private final List<String> supportedStorages;
  private final boolean allStoragesSupported;
  private final boolean failOnPartialData;
  private final List<String> validDimTables;
  private final Map<CubeFactTable, Map<UpdatePeriod, Set<String>>> validStorageMap = new HashMap<>();
  private final UpdatePeriod maxInterval;
  // TODO union : Remove this. All partitions are stored in the StorageCandidate.
  private final Map<String, Set<String>> nonExistingPartitions = new HashMap<>();
  CubeMetastoreClient client;
  Map<String, List<String>> storagePartMap = new HashMap<String, List<String>>();
  private String processTimePartCol = null;
  private TimeRangeWriter rangeWriter;
  private DateFormat partWhereClauseFormat = null;
  private PHASE phase;
  // TODO union : we do not need this. Remove the storage candidate
  private HashMap<CubeFactTable, Map<String, SkipStorageCause>> skipStorageCausesPerFact;
  private float completenessThreshold;
  private String completenessPartCol;

  public StorageTableResolver(Configuration conf) {
    this.conf = conf;
    this.supportedStorages = getSupportedStorages(conf);
    this.allStoragesSupported = (supportedStorages == null);
    this.failOnPartialData = conf.getBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    String str = conf.get(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES);
    validDimTables = StringUtils.isBlank(str) ? null : Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
    this.processTimePartCol = conf.get(CubeQueryConfUtil.PROCESS_TIME_PART_COL);
    String maxIntervalStr = conf.get(CubeQueryConfUtil.QUERY_MAX_INTERVAL);
    if (maxIntervalStr != null) {
      this.maxInterval = UpdatePeriod.valueOf(maxIntervalStr);
    } else {
      this.maxInterval = null;
    }
    rangeWriter = ReflectionUtils.newInstance(conf
      .getClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, CubeQueryConfUtil.DEFAULT_TIME_RANGE_WRITER,
        TimeRangeWriter.class), this.conf);
    String formatStr = conf.get(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT);
    if (formatStr != null) {
      partWhereClauseFormat = new SimpleDateFormat(formatStr);
    }
    this.phase = PHASE.first();
    completenessThreshold = conf
      .getFloat(CubeQueryConfUtil.COMPLETENESS_THRESHOLD, CubeQueryConfUtil.DEFAULT_COMPLETENESS_THRESHOLD);
    completenessPartCol = conf.get(CubeQueryConfUtil.COMPLETENESS_CHECK_PART_COL);
  }

  private List<String> getSupportedStorages(Configuration conf) {
    String[] storages = conf.getStrings(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES);
    if (storages != null) {
      return Arrays.asList(storages);
    }
    return null;
  }

  public boolean isStorageSupportedOnDriver(String storage) {
    return allStoragesSupported || supportedStorages.contains(storage);
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    client = cubeql.getMetastoreClient();

    switch (phase) {
    case STORAGE_TABLES:
      if (!cubeql.getCandidates().isEmpty()) {
        resolveStorageTable(cubeql);
      }
      break;
    case STORAGE_PARTITIONS:
      if (!cubeql.getCandidates().isEmpty()) {
        resolveStoragePartitions(cubeql);
      }
      break;
    case DIM_TABLE_AND_PARTITIONS:
      resolveDimStorageTablesAndPartitions(cubeql);
      if (cubeql.getAutoJoinCtx() != null) {
        // After all candidates are pruned after storage resolver, prune join paths.
        cubeql.getAutoJoinCtx().pruneAllPaths(cubeql.getCube(), cubeql.getCandidateFacts(), null);
        cubeql.getAutoJoinCtx().pruneAllPathsForCandidateDims(cubeql.getCandidateDimTables());
        cubeql.getAutoJoinCtx().refreshJoinPathColumns();
      }
      // TODO union : What is this? We may not need this as it non existing partitions are stored in StorageCandidate
      cubeql.setNonexistingParts(nonExistingPartitions);
      break;
    }
    phase = phase.next();
  }

  /**
   * Each candidate in the set is a complex candidate. We will evaluate each one to get
   * all the partitions needed to answer the query.
   *
   * @param cubeql
   */
  private void resolveStoragePartitions(CubeQueryContext cubeql) throws LensException {
    Set<Candidate> candidateList = cubeql.getCandidates();
    for (Candidate candidate : candidateList) {
      boolean isComplete = true;
      for (TimeRange range : cubeql.getTimeRanges()) {
        isComplete &= candidate.evaluateCompleteness(range, failOnPartialData);
      }
      if (!isComplete) {
        // TODO union : Prune this candidate?
      }
    }
  }

  private void resolveDimStorageTablesAndPartitions(CubeQueryContext cubeql) throws LensException {
    Set<Dimension> allDims = new HashSet<Dimension>(cubeql.getDimensions());
    for (Aliased<Dimension> dim : cubeql.getOptionalDimensions()) {
      allDims.add(dim.getObject());
    }
    for (Dimension dim : allDims) {
      Set<CandidateDim> dimTables = cubeql.getCandidateDimTables().get(dim);
      if (dimTables == null || dimTables.isEmpty()) {
        continue;
      }
      Iterator<CandidateDim> i = dimTables.iterator();
      while (i.hasNext()) {
        CandidateDim candidate = i.next();
        CubeDimensionTable dimtable = candidate.dimtable;
        if (dimtable.getStorages().isEmpty()) {
          cubeql
            .addDimPruningMsgs(dim, dimtable, new CandidateTablePruneCause(CandidateTablePruneCode.MISSING_STORAGES));
          i.remove();
          continue;
        }
        Set<String> storageTables = new HashSet<String>();
        Map<String, String> whereClauses = new HashMap<String, String>();
        boolean foundPart = false;
        Map<String, SkipStorageCause> skipStorageCauses = new HashMap<>();
        for (String storage : dimtable.getStorages()) {
          if (isStorageSupportedOnDriver(storage)) {
            String tableName = getFactOrDimtableStorageTableName(dimtable.getName(), storage).toLowerCase();
            if (validDimTables != null && !validDimTables.contains(tableName)) {
              log.info("Not considering dim storage table:{} as it is not a valid dim storage", tableName);
              skipStorageCauses.put(tableName, new SkipStorageCause(SkipStorageCode.INVALID));
              continue;
            }

            if (dimtable.hasStorageSnapshots(storage)) {
              // check if partition exists
              foundPart = client.dimTableLatestPartitionExists(tableName);
              if (foundPart) {
                log.debug("Adding existing partition {}", StorageConstants.LATEST_PARTITION_VALUE);
              } else {
                log.info("Partition {} does not exist on {}", StorageConstants.LATEST_PARTITION_VALUE, tableName);
              }
              if (!failOnPartialData || foundPart) {
                storageTables.add(tableName);
                String whereClause = StorageUtil
                  .getWherePartClause(dim.getTimedDimension(), null, StorageConstants.getPartitionsForLatest());
                whereClauses.put(tableName, whereClause);
              } else {
                log.info("Not considering dim storage table:{} as no dim partitions exist", tableName);
                skipStorageCauses.put(tableName, new SkipStorageCause(SkipStorageCode.NO_PARTITIONS));
              }
            } else {
              storageTables.add(tableName);
              foundPart = true;
            }
          } else {
            log.info("Storage:{} is not supported", storage);
            skipStorageCauses.put(storage, new SkipStorageCause(SkipStorageCode.UNSUPPORTED));
          }
        }
        if (!foundPart) {
          addNonExistingParts(dim.getName(), StorageConstants.getPartitionsForLatest());
        }
        if (storageTables.isEmpty()) {
          log.info("Not considering dim table:{} as no candidate storage tables eixst", dimtable);
          cubeql.addDimPruningMsgs(dim, dimtable, noCandidateStorages(skipStorageCauses));
          i.remove();
          continue;
        }
        // pick the first storage table
        candidate.setStorageName(storageTables.iterator().next());
        candidate.setWhereClause(whereClauses.get(candidate.getStorageName()));
      }
    }
  }

  /**
   * Following storages are removed:
   * 1. The storage is not supported by driver.
   * 2. The storage is not in the valid storage list.
   * 3. The storage is not in any time range in the query.
   * 4. The storage having no valid update period.
   *
   * This method also creates a list of valid update periods and stores them into {@link StorageCandidate}.
   *
   * TODO union : Do fourth point before 3.
   */
  private void resolveStorageTable(CubeQueryContext cubeql) throws LensException {
    Iterator<Candidate> it = cubeql.getCandidates().iterator();
    while (it.hasNext()) {
      Candidate c = it.next();
      assert (c instanceof StorageCandidate);
      StorageCandidate sc = (StorageCandidate) c;
      String storageTable = sc.getStorageName();
      if (!isStorageSupportedOnDriver(storageTable)) {
        log.info("Skipping storage: {} as it is not supported", storageTable);
        cubeql.addStoragePruningMsg(sc, new CandidateTablePruneCause(CandidateTablePruneCode.UNSUPPORTED_STORAGE));
        it.remove();
        continue;
      }
      String str = conf.get(CubeQueryConfUtil.getValidStorageTablesKey(sc.getFact().getName()));
      List<String> validFactStorageTables = StringUtils.isBlank(str)
                                            ? null
                                            : Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
      // Check if storagetable is in the list of valid storages.
      if (validFactStorageTables != null && !validFactStorageTables.contains(storageTable)) {
        log.info("Skipping storage table {} as it is not valid", storageTable);
        cubeql.addStoragePruningMsg(sc, new CandidateTablePruneCause(CandidateTablePruneCode.INVALID_STORAGE));
        it.remove();
        continue;
      }

      boolean valid = false;
      Set<CandidateTablePruneCause.CandidateTablePruneCode> codes = new HashSet<>();
      for (TimeRange range : cubeql.getTimeRanges()) {
        boolean columnInRange = client
          .isStorageTableCandidateForRange(storageTable, range.getFromDate(), range.getToDate());
        boolean partitionColumnExists = client.partColExists(storageTable, range.getPartitionColumn());
        valid = columnInRange && partitionColumnExists;
        if (valid) {
          break;
        }
        if (!columnInRange) {
          codes.add(TIME_RANGE_NOT_ANSWERABLE);
          continue;
        }
        // This means fallback is required.
        if (!partitionColumnExists) {
          String timeDim = cubeql.getBaseCube().getTimeDimOfPartitionColumn(range.getPartitionColumn());
          if (!sc.getFact().getColumns().contains(timeDim)) {
            // Not a time dimension so no fallback required.
            codes.add(TIMEDIM_NOT_SUPPORTED);
            continue;
          }
          TimeRange fallBackRange = getFallbackRange(range, sc.getFact().getCubeName(), cubeql);
          if (fallBackRange == null) {
            log.info("No partitions for range:{}. fallback range: {}", range, fallBackRange);
            continue;
          }
          valid = client
            .isStorageTableCandidateForRange(storageTable, fallBackRange.getFromDate(), fallBackRange.getToDate());
          if (valid) {
            break;
          } else {
            codes.add(TIME_RANGE_NOT_ANSWERABLE);
          }
        }
      }
      if (!valid) {
        it.remove();
        for (CandidateTablePruneCode code : codes) {
          cubeql.addStoragePruningMsg(sc, new CandidateTablePruneCause(code));
        }
        continue;
      }

      List<String> validUpdatePeriods = CubeQueryConfUtil
        .getStringList(conf, CubeQueryConfUtil.getValidUpdatePeriodsKey(sc.getFact().getName(), storageTable));
      boolean isStorageAdded = false;
      Map<String, SkipUpdatePeriodCode> skipUpdatePeriodCauses = new HashMap<>();

      // Check for update period.
      for (UpdatePeriod updatePeriod : sc.getFact().getUpdatePeriods().get(storageTable)) {
        if (maxInterval != null && updatePeriod.compareTo(maxInterval) > 0) {
          log.info("Skipping update period {} for fact {}", updatePeriod, sc.getFact());
          skipUpdatePeriodCauses.put(updatePeriod.toString(), SkipUpdatePeriodCode.QUERY_INTERVAL_BIGGER);
          continue;
        }
        if (validUpdatePeriods != null && !validUpdatePeriods.contains(updatePeriod.name().toLowerCase())) {
          log.info("Skipping update period {} for fact {} for storage {}", updatePeriod, sc.getFact(), storageTable);
          skipUpdatePeriodCauses.put(updatePeriod.toString(), SkipUpdatePeriodCode.INVALID);
          continue;
        }
        isStorageAdded = true;
        sc.addValidUpdatePeriod(updatePeriod);
      }
      if (!isStorageAdded) {
        cubeql.addStoragePruningMsg(sc, CandidateTablePruneCause.updatePeriodsRejected(skipUpdatePeriodCauses));
        it.remove();
      }
    }
  }

  private TreeSet<UpdatePeriod> getValidUpdatePeriods(CubeFactTable fact) {
    TreeSet<UpdatePeriod> set = new TreeSet<UpdatePeriod>();
    set.addAll(validStorageMap.get(fact).keySet());
    return set;
  }

  private String getStorageTableName(CubeFactTable fact, String storage, List<String> validFactStorageTables) {
    String tableName = getFactOrDimtableStorageTableName(fact.getName(), storage).toLowerCase();
    if (validFactStorageTables != null && !validFactStorageTables.contains(tableName)) {
      log.info("Skipping storage table {} as it is not valid", tableName);
      return null;
    }
    return tableName;
  }

  void addNonExistingParts(String name, Set<String> nonExistingParts) {
    nonExistingPartitions.put(name, nonExistingParts);
  }

  private Set<String> getStorageTablesWithoutPartCheck(FactPartition part, Set<String> storageTableNames)
    throws LensException, HiveException {
    Set<String> validStorageTbls = new HashSet<>();
    for (String storageTableName : storageTableNames) {
      // skip all storage tables for which are not eligible for this partition
      if (client.isStorageTablePartitionACandidate(storageTableName, part.getPartSpec())) {
        validStorageTbls.add(storageTableName);
      } else {
        log.info("Skipping {} as it is not valid for part {}", storageTableName, part.getPartSpec());
      }
    }
    return validStorageTbls;
  }

  enum PHASE {
    STORAGE_TABLES, STORAGE_PARTITIONS, DIM_TABLE_AND_PARTITIONS;

    static PHASE first() {
      return values()[0];
    }

    static PHASE last() {
      return values()[values().length - 1];
    }

    PHASE next() {
      return values()[(this.ordinal() + 1) % values().length];
    }
  }
}
