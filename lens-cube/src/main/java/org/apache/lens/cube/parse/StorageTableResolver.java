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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.metadata.timeline.RangesPartitionTimeline;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.cube.parse.CandidateTablePruneCause.SkipStorageCause;
import org.apache.lens.cube.parse.CandidateTablePruneCause.SkipStorageCode;
import org.apache.lens.cube.parse.CandidateTablePruneCause.SkipUpdatePeriodCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Resolve storages and partitions of all candidate tables and prunes candidate tables with missing storages or
 * partitions.
 */
class StorageTableResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(StorageTableResolver.class.getName());

  private final Configuration conf;
  private final List<String> supportedStorages;
  private final boolean allStoragesSupported;
  CubeMetastoreClient client;
  private final boolean failOnPartialData;
  private final List<String> validDimTables;
  private final Map<CubeFactTable, Map<UpdatePeriod, Set<String>>> validStorageMap =
    new HashMap<CubeFactTable, Map<UpdatePeriod, Set<String>>>();
  private String processTimePartCol = null;
  private final UpdatePeriod maxInterval;
  private final Map<String, Set<String>> nonExistingPartitions = new HashMap<String, Set<String>>();
  private TimeRangeWriter rangeWriter;
  private DateFormat partWhereClauseFormat = null;
  private PHASE phase;

  static enum PHASE {
    FACT_TABLES, FACT_PARTITIONS, DIM_TABLE_AND_PARTITIONS;

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
    rangeWriter =
      ReflectionUtils.newInstance(conf.getClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS,
        CubeQueryConfUtil.DEFAULT_TIME_RANGE_WRITER, TimeRangeWriter.class), this.conf);
    String formatStr = conf.get(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT);
    if (formatStr != null) {
      partWhereClauseFormat = new SimpleDateFormat(formatStr);
    }
    this.phase = PHASE.first();
  }

  private List<String> getSupportedStorages(Configuration conf) {
    String[] storages = conf.getStrings(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES);
    if (storages != null) {
      return Arrays.asList(storages);
    }
    return null;
  }

  public boolean isStorageSupported(String storage) {
    return allStoragesSupported || supportedStorages.contains(storage);
  }

  Map<String, List<String>> storagePartMap = new HashMap<String, List<String>>();

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    client = cubeql.getMetastoreClient();

    switch (phase) {
    case FACT_TABLES:
      if (!cubeql.getCandidateFactTables().isEmpty()) {
        // resolve storage table names
        resolveFactStorageTableNames(cubeql);
      }
      cubeql.pruneCandidateFactSet(CandidateTablePruneCode.NO_CANDIDATE_STORAGES);
      break;
    case FACT_PARTITIONS:
      if (!cubeql.getCandidateFactTables().isEmpty()) {
        // resolve storage partitions
        resolveFactStoragePartitions(cubeql);
      }
      cubeql.pruneCandidateFactSet(CandidateTablePruneCode.NO_CANDIDATE_STORAGES);
      break;
    case DIM_TABLE_AND_PARTITIONS:
      resolveDimStorageTablesAndPartitions(cubeql);
      if (cubeql.getAutoJoinCtx() != null) {
        // After all candidates are pruned after storage resolver, prune join paths.
        cubeql.getAutoJoinCtx().pruneAllPaths(cubeql.getCube(), cubeql.getCandidateFactTables(), null);
        cubeql.getAutoJoinCtx().pruneAllPathsForCandidateDims(cubeql.getCandidateDimTables());
        cubeql.getAutoJoinCtx().refreshJoinPathColumns();
      }
      break;
    }
    //Doing this on all three phases. Keep updating cubeql with the current identified missing partitions.
    cubeql.setNonexistingParts(nonExistingPartitions);
    phase = phase.next();
  }

  private void resolveDimStorageTablesAndPartitions(CubeQueryContext cubeql) throws SemanticException {
    Set<Dimension> allDims = new HashSet<Dimension>(cubeql.getDimensions());
    allDims.addAll(cubeql.getOptionalDimensions());
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
          cubeql.addDimPruningMsgs(dim, dimtable, new CandidateTablePruneCause(
            CandidateTablePruneCode.MISSING_STORAGES));
          i.remove();
          continue;
        }
        Set<String> storageTables = new HashSet<String>();
        Map<String, String> whereClauses = new HashMap<String, String>();
        boolean foundPart = false;
        Map<String, SkipStorageCause> skipStorageCauses = new HashMap<String, SkipStorageCause>();
        for (String storage : dimtable.getStorages()) {
          if (isStorageSupported(storage)) {
            String tableName = MetastoreUtil.getDimStorageTableName(dimtable.getName(), storage).toLowerCase();
            if (validDimTables != null && !validDimTables.contains(tableName)) {
              LOG.info("Not considering dim storage table:" + tableName + " as it is not a valid dim storage");
              skipStorageCauses.put(tableName, new SkipStorageCause(SkipStorageCode.INVALID));
              continue;
            }

            if (dimtable.hasStorageSnapshots(storage)) {
              // check if partition exists
              foundPart = client.dimTableLatestPartitionExists(tableName);
              if (foundPart) {
                LOG.info("Adding existing partition" + StorageConstants.LATEST_PARTITION_VALUE);
              } else {
                LOG.info("Partition " + StorageConstants.LATEST_PARTITION_VALUE + " does not exist on " + tableName);
              }
              if (!failOnPartialData || foundPart) {
                storageTables.add(tableName);
                String whereClause =
                  StorageUtil.getWherePartClause(dim.getTimedDimension(), null,
                    StorageConstants.getPartitionsForLatest());
                whereClauses.put(tableName, whereClause);
              } else {
                LOG.info("Not considering dim storage table:" + tableName + " as no dim partitions exist");
                skipStorageCauses.put(tableName, new SkipStorageCause(SkipStorageCode.NO_PARTITIONS));
              }
            } else {
              storageTables.add(tableName);
              foundPart = true;
            }
          } else {
            LOG.info("Storage:" + storage + " is not supported");
            skipStorageCauses.put(storage, new SkipStorageCause(SkipStorageCode.UNSUPPORTED));
          }
        }
        if (!foundPart) {
          addNonExistingParts(dim.getName(), StorageConstants.getPartitionsForLatest());
        }
        if (storageTables.isEmpty()) {
          LOG.info("Not considering dim table:" + dimtable + " as no candidate storage tables eixst");
          cubeql.addDimPruningMsgs(dim, dimtable, CandidateTablePruneCause.noCandidateStorages(skipStorageCauses));
          i.remove();
          continue;
        }
        // pick the first storage table
        candidate.setStorageTable(storageTables.iterator().next());
        candidate.setWhereClause(whereClauses.get(candidate.getStorageTable()));
      }
    }
  }

  // Resolves all the storage table names, which are valid for each updatePeriod
  private void resolveFactStorageTableNames(CubeQueryContext cubeql) throws SemanticException {
    Iterator<CandidateFact> i = cubeql.getCandidateFactTables().iterator();
    while (i.hasNext()) {
      CubeFactTable fact = i.next().fact;
      if (fact.getUpdatePeriods().isEmpty()) {
        cubeql.addFactPruningMsgs(fact, new CandidateTablePruneCause(CandidateTablePruneCode.MISSING_STORAGES));
        i.remove();
        continue;
      }
      Map<UpdatePeriod, Set<String>> storageTableMap = new TreeMap<UpdatePeriod, Set<String>>();
      validStorageMap.put(fact, storageTableMap);
      String str = conf.get(CubeQueryConfUtil.getValidStorageTablesKey(fact.getName()));
      List<String> validFactStorageTables =
        StringUtils.isBlank(str) ? null : Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
      Map<String, SkipStorageCause> skipStorageCauses = new HashMap<String, SkipStorageCause>();

      for (Map.Entry<String, Set<UpdatePeriod>> entry : fact.getUpdatePeriods().entrySet()) {
        String storage = entry.getKey();
        // skip storages that are not supported
        if (!isStorageSupported(storage)) {
          LOG.info("Skipping storage: " + storage + " as it is not supported");
          skipStorageCauses.put(storage, new SkipStorageCause(SkipStorageCode.UNSUPPORTED));
          continue;
        }
        String table = getStorageTableName(fact, storage, validFactStorageTables);
        // skip the update period if the storage is not valid
        if (table == null) {
          skipStorageCauses.put(storage, new SkipStorageCause(SkipStorageCode.INVALID));
          continue;
        }
        List<String> validUpdatePeriods =
          CubeQueryConfUtil.getStringList(conf, CubeQueryConfUtil.getValidUpdatePeriodsKey(fact.getName(), storage));

        boolean isStorageAdded = false;
        Map<String, SkipUpdatePeriodCode> skipUpdatePeriodCauses = new HashMap<String, SkipUpdatePeriodCode>();
        for (UpdatePeriod updatePeriod : entry.getValue()) {
          if (maxInterval != null && updatePeriod.compareTo(maxInterval) > 0) {
            LOG.info("Skipping update period " + updatePeriod + " for fact" + fact);
            skipUpdatePeriodCauses.put(updatePeriod.toString(), SkipUpdatePeriodCode.QUERY_INTERVAL_BIGGER);
            continue;
          }
          if (validUpdatePeriods != null && !validUpdatePeriods.contains(updatePeriod.name().toLowerCase())) {
            LOG.info("Skipping update period " + updatePeriod + " for fact" + fact + " for storage" + storage);
            skipUpdatePeriodCauses.put(updatePeriod.toString(), SkipUpdatePeriodCode.INVALID);
            continue;
          }
          Set<String> storageTables = storageTableMap.get(updatePeriod);
          if (storageTables == null) {
            storageTables = new LinkedHashSet<String>();
            storageTableMap.put(updatePeriod, storageTables);
          }
          isStorageAdded = true;
          LOG.info("Adding storage table:" + table + " for fact:" + fact + " for update period" + updatePeriod);
          storageTables.add(table);
        }
        if (!isStorageAdded) {
          skipStorageCauses.put(storage, SkipStorageCause.noCandidateUpdatePeriod(skipUpdatePeriodCauses));
        }
      }
      if (storageTableMap.isEmpty()) {
        LOG.info("Not considering fact table:" + fact + " as it does not" + " have any storage tables");
        cubeql.addFactPruningMsgs(fact, CandidateTablePruneCause.noCandidateStorages(skipStorageCauses));
        i.remove();
      }
    }
  }

  private TreeSet<UpdatePeriod> getValidUpdatePeriods(CubeFactTable fact) {
    TreeSet<UpdatePeriod> set = new TreeSet<UpdatePeriod>();
    set.addAll(validStorageMap.get(fact).keySet());
    return set;
  }

  String getStorageTableName(CubeFactTable fact, String storage, List<String> validFactStorageTables) {
    String tableName = MetastoreUtil.getFactStorageTableName(fact.getName(), storage).toLowerCase();
    if (validFactStorageTables != null && !validFactStorageTables.contains(tableName)) {
      LOG.info("Skipping storage table " + tableName + " as it is not valid");
      return null;
    }
    return tableName;
  }

  private void resolveFactStoragePartitions(CubeQueryContext cubeql) throws SemanticException {
    // Find candidate tables wrt supported storages
    Iterator<CandidateFact> i = cubeql.getCandidateFactTables().iterator();
    while (i.hasNext()) {
      CandidateFact cfact = i.next();
      List<FactPartition> answeringParts = new ArrayList<FactPartition>();
      HashMap<String, SkipStorageCause> skipStorageCauses = new HashMap<String, SkipStorageCause>();
      Map<UpdatePeriod, RangesPartitionTimeline> missingPartitionRanges = Maps.newHashMap();
      boolean noPartsForRange = false;
      for (TimeRange range : cubeql.getTimeRanges()) {
        Set<FactPartition> rangeParts = getPartitions(cfact.fact, range, skipStorageCauses, missingPartitionRanges);
        if (rangeParts == null || rangeParts.isEmpty()) {
          LOG.info("No partitions for range:" + range);
          noPartsForRange = true;
          continue;
        }
        cfact.incrementPartsQueried(rangeParts.size());
        answeringParts.addAll(rangeParts);
        cfact.getPartsQueried().addAll(rangeParts);
        cfact.getRangeToWhereClause().put(range, rangeWriter.getTimeRangeWhereClause(cubeql,
          cubeql.getAliasForTabName(cubeql.getCube().getName()), rangeParts));
      }
      Set<String> nonExistingParts = Sets.newHashSet();
      if (!missingPartitionRanges.isEmpty()) {
        for (UpdatePeriod period : missingPartitionRanges.keySet()) {
          for (TimePartitionRange range : missingPartitionRanges.get(period).getRanges()) {
            nonExistingParts.add(range.toString());
          }
        }
      }
      if (!nonExistingParts.isEmpty()) {
        addNonExistingParts(cfact.fact.getName(), nonExistingParts);
      }
      if (cfact.getNumQueriedParts() == 0 || (failOnPartialData && (noPartsForRange || !nonExistingParts.isEmpty()))) {
        LOG.info("Not considering fact table:" + cfact.fact + " as it could" + " not find partition for given ranges: "
          + cubeql.getTimeRanges());
        /*
         * This fact is getting discarded because of any of following reasons:
         * 1. Has missing partitions
         * 2. All Storage tables were skipped for some reasons.
         * 3. Storage tables do not have the update period for the timerange queried.
         */
        if (failOnPartialData && !nonExistingParts.isEmpty()) {
          cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.missingPartitions(nonExistingParts));
        } else if (!skipStorageCauses.isEmpty()) {
          CandidateTablePruneCause cause = CandidateTablePruneCause.noCandidateStorages(skipStorageCauses);
          cubeql.addFactPruningMsgs(cfact.fact, cause);
        } else {
          CandidateTablePruneCause cause =
            new CandidateTablePruneCause(CandidateTablePruneCode.NO_FACT_UPDATE_PERIODS_FOR_GIVEN_RANGE);
          cubeql.addFactPruningMsgs(cfact.fact, cause);
        }
        i.remove();
        continue;
      }
      // Map from storage to covering parts
      Map<String, Set<FactPartition>> minimalStorageTables = new LinkedHashMap<String, Set<FactPartition>>();
      boolean enabledMultiTableSelect = StorageUtil.getMinimalAnsweringTables(answeringParts, minimalStorageTables);
      if (minimalStorageTables.isEmpty()) {
        LOG.info("Not considering fact table:" + cfact + " as it does not" + " have any storage tables");
        cubeql.addFactPruningMsgs(cfact.fact, CandidateTablePruneCause.noCandidateStorages(skipStorageCauses));
        i.remove();
        continue;
      }
      Set<String> storageTables = new LinkedHashSet<String>();
      storageTables.addAll(minimalStorageTables.keySet());
      cfact.setStorageTables(storageTables);
      // multi table select is already false, do not alter it
      if (cfact.isEnabledMultiTableSelect()) {
        cfact.setEnabledMultiTableSelect(enabledMultiTableSelect);
      }
      LOG.info("Resolved partitions for fact " + cfact + ": " + answeringParts + " storageTables:" + storageTables);
    }
  }


  void addNonExistingParts(String name, Set<String> nonExistingParts) {
    nonExistingPartitions.put(name, nonExistingParts);
  }

  private Set<FactPartition> getPartitions(CubeFactTable fact, TimeRange range,
    HashMap<String, SkipStorageCause> skipStorageCauses,
    Map<UpdatePeriod, RangesPartitionTimeline> nonExistingParts) throws SemanticException {
    try {
      return getPartitions(fact, range, getValidUpdatePeriods(fact), true, skipStorageCauses,
        nonExistingParts);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private Set<FactPartition> getPartitions(CubeFactTable fact, TimeRange range, TreeSet<UpdatePeriod> updatePeriods,
    boolean addNonExistingParts, Map<String, SkipStorageCause> skipStorageCauses,
    Map<UpdatePeriod, RangesPartitionTimeline> nonExistingParts)
    throws Exception {
    Set<FactPartition> partitions = new TreeSet<FactPartition>();
    if (range.isCoverableBy(updatePeriods)
      && getPartitions(fact, range.getFromDate(), range.getToDate(), range.getPartitionColumn(), partitions,
        updatePeriods, addNonExistingParts, skipStorageCauses, nonExistingParts)) {
      return partitions;
    } else {
      return new TreeSet<FactPartition>();
    }
  }

  private boolean getPartitions(CubeFactTable fact, Date fromDate, Date toDate, String partCol,
    Set<FactPartition> partitions, TreeSet<UpdatePeriod> updatePeriods,
    boolean addNonExistingParts, Map<String, SkipStorageCause> skipStorageCauses,
    Map<UpdatePeriod, RangesPartitionTimeline> nonExistingParts)
    throws Exception {
    LOG.info("getPartitions for " + fact + " from fromDate:" + fromDate + " toDate:" + toDate);
    if (fromDate.equals(toDate) || fromDate.after(toDate)) {
      return true;
    }
    UpdatePeriod interval = CubeFactTable.maxIntervalInRange(fromDate, toDate, updatePeriods);
    if (interval == null) {
      LOG.info("No max interval for range:" + fromDate + " to " + toDate);
      return false;
    }
    LOG.info("Max interval for " + fact + " is:" + interval);
    Set<String> storageTbls = new LinkedHashSet<String>();
    storageTbls.addAll(validStorageMap.get(fact).get(interval));

    Iterator<String> it = storageTbls.iterator();
    while (it.hasNext()) {
      String storageTableName = it.next();
      if (!client.partColExists(storageTableName, partCol)) {
        LOG.info(partCol + " does not exist in" + storageTableName);
        skipStorageCauses.put(storageTableName, SkipStorageCause.partColDoesNotExist(partCol));
        it.remove();
        continue;
      }
    }

    if (storageTbls.isEmpty()) {
      return false;
    }
    Date ceilFromDate = DateUtil.getCeilDate(fromDate, interval);
    Date floorToDate = DateUtil.getFloorDate(toDate, interval);

    int lookAheadNumParts =
      conf.getInt(CubeQueryConfUtil.getLookAheadPTPartsKey(interval), CubeQueryConfUtil.DEFAULT_LOOK_AHEAD_PT_PARTS);

    TimeRange.Iterable.Iterator iter = TimeRange.iterable(ceilFromDate, floorToDate, interval, 1)
      .iterator();
    // add partitions from ceilFrom to floorTo
    while (iter.hasNext()) {
      Date dt = iter.next();
      Date nextDt = iter.peekNext();
      FactPartition part = new FactPartition(partCol, dt, interval, null, partWhereClauseFormat);
      LOG.info("candidate storage tables for searching partitions: " + storageTbls);
      updateFactPartitionStorageTablesFrom(fact, part, storageTbls);
      LOG.info("Storage tables containing Partition " + part + " are: " + part.getStorageTables());
      if (part.isFound()) {
        LOG.info("Adding existing partition" + part);
        partitions.add(part);
        LOG.info("Looking for look ahead process time partitions for " + part);
        if (processTimePartCol == null) {
          LOG.info("processTimePartCol is null");
        } else if (partCol.equals(processTimePartCol)) {
          LOG.info("part column is process time col");
        } else if (updatePeriods.first().equals(interval)) {
          LOG.info("Update period is the least update period");
        } else if ((iter.getNumIters() - iter.getCounter()) > lookAheadNumParts) {
          // see if this is the part of the last-n look ahead partitions
          LOG.info("Not a look ahead partition");
        } else {
          LOG.info("Looking for look ahead process time partitions for " + part);
          // check if finer partitions are required
          // final partitions are required if no partitions from
          // look-ahead
          // process time are present
          TimeRange.Iterable.Iterator processTimeIter = TimeRange.iterable(nextDt, lookAheadNumParts,
            interval, 1).iterator();
          while (processTimeIter.hasNext()) {
            Date pdt = processTimeIter.next();
            Date nextPdt = processTimeIter.peekNext();
            FactPartition processTimePartition = new FactPartition(processTimePartCol, pdt, interval, null,
              partWhereClauseFormat);
            updateFactPartitionStorageTablesFrom(fact, processTimePartition,
              part.getStorageTables());
            if (processTimePartition.isFound()) {
              LOG.info("Finer parts not required for look-ahead partition :" + part);
            } else {
              LOG.info("Looked ahead process time partition " + processTimePartition + " is not found");
              TreeSet<UpdatePeriod> newset = new TreeSet<UpdatePeriod>();
              newset.addAll(updatePeriods);
              newset.remove(interval);
              LOG.info("newset of update periods:" + newset);
              if (!newset.isEmpty()) {
                // Get partitions for look ahead process time
                LOG.info("Looking for process time partitions between " + pdt + " and " + nextPdt);
                Set<FactPartition> processTimeParts =
                  getPartitions(fact, TimeRange.getBuilder().fromDate(pdt).toDate(nextPdt).partitionColumn(
                    processTimePartCol).build(), newset, false, skipStorageCauses, nonExistingParts);
                LOG.info("Look ahead partitions: " + processTimeParts);
                TimeRange timeRange = TimeRange.getBuilder().fromDate(dt).toDate(nextDt).build();
                for (FactPartition pPart : processTimeParts) {
                  LOG.info("Looking for finer partitions in pPart: " + pPart);
                  for (Date date : timeRange.iterable(pPart.getPeriod(), 1)) {
                    partitions.add(new FactPartition(partCol, date, pPart.getPeriod(), pPart,
                      partWhereClauseFormat));
                  }
                  LOG.info("added all sub partitions blindly in pPart: " + pPart);
//                          if (!getPartitions(fact, dt, cal.getTime(), partCol, pPart, partitions, newset, false,
//                            skipStorageCauses, nonExistingParts)) {
//                            LOG.info("No partitions found in look ahead range");
//                          }
                }
              }
            }
          }
        }
      } else {
        LOG.info("Partition:" + part + " does not exist in any storage table");
        TreeSet<UpdatePeriod> newset = new TreeSet<UpdatePeriod>();
        newset.addAll(updatePeriods);
        newset.remove(interval);
        if (!getPartitions(fact, dt, nextDt, partCol, partitions, newset, false, skipStorageCauses,
          nonExistingParts)) {

          LOG.info("Adding non existing partition" + part);
          if (addNonExistingParts) {
            // Add non existing partitions for all cases of whether we populate all non existing or not.
            if (!nonExistingParts.containsKey(part.getPeriod())) {
              nonExistingParts.put(part.getPeriod(), new RangesPartitionTimeline(null, null, null));
            }
            nonExistingParts.get(part.getPeriod()).add(TimePartition.of(part.getPeriod(), dt));
            if (!failOnPartialData) {
              partitions.add(part);
              // add all storage tables as the answering tables
              part.getStorageTables().addAll(storageTbls);
            }
          } else {
            LOG.info("No finer granual partitions exist for" + part);
            return false;
          }
        } else {
          LOG.info("Finer granual partitions added for " + part);
        }
      }
    }
    return getPartitions(fact, fromDate, ceilFromDate, partCol, partitions,
      updatePeriods, addNonExistingParts, skipStorageCauses, nonExistingParts)
      && getPartitions(fact, floorToDate, toDate, partCol, partitions,
        updatePeriods, addNonExistingParts, skipStorageCauses, nonExistingParts);
  }

  private void updateFactPartitionStorageTablesFrom(CubeFactTable fact, FactPartition part,
    Set<String> storageTableNames) throws LensException, HiveException, ParseException {
    for (String storageTableName : storageTableNames) {
      if (client.factPartitionExists(fact, part, storageTableName)) {
        part.getStorageTables().add(storageTableName);
        part.setFound(true);
      }
    }
  }
}
