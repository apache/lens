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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lens.cube.metadata.CubeDimensionTable;
import org.apache.lens.cube.metadata.CubeFactTable;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.StorageConstants;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CubeTableCause;
import org.apache.lens.cube.parse.CandidateTablePruneCause.SkipStorageCause;
import org.apache.lens.cube.parse.CandidateTablePruneCause.SkipUpdatePeriodCause;

/**
 * Resolve storages and partitions of all candidate tables and prunes candidate
 * tables with missing storages or partitions.
 */
class StorageTableResolver implements ContextRewriter {
  private static Log LOG = LogFactory.getLog(StorageTableResolver.class.getName());

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
  private final boolean populateNonExistingParts;
  private final Map<String, List<String>> nonExistingPartitions = new HashMap<String, List<String>>();
  private TimeRangeWriter rangeWriter;
  private DateFormat partWhereClauseFormat = null;

  public StorageTableResolver(Configuration conf) {
    this.conf = conf;
    this.supportedStorages = getSupportedStorages(conf);
    this.allStoragesSupported = (supportedStorages == null);
    this.failOnPartialData = conf.getBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    if (!failOnPartialData) {
      this.populateNonExistingParts = true;
    } else {
      this.populateNonExistingParts =
          conf.getBoolean(CubeQueryConfUtil.ADD_NON_EXISTING_PARTITIONS,
              CubeQueryConfUtil.DEFAULT_ADD_NON_EXISTING_PARTITIONS);
    }
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
  }

  private List<String> getSupportedStorages(Configuration conf) {
    String[] storages = conf.getStrings(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES);
    if (storages != null) {
      return Arrays.asList(storages);
    }
    return null;
  }

  public boolean isStorageSupported(String storage) {
    if (!allStoragesSupported) {
      return supportedStorages.contains(storage);
    }
    return true;
  }

  Map<String, List<String>> storagePartMap = new HashMap<String, List<String>>();

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    client = cubeql.getMetastoreClient();

    if (!cubeql.getCandidateFactTables().isEmpty()) {
      // resolve storage table names
      resolveFactStorageTableNames(cubeql);
      // resolve storage partitions
      resolveFactStoragePartitions(cubeql);
    }
    // resolve dimension tables
    resolveDimStorageTablesAndPartitions(cubeql);
    cubeql.setNonexistingParts(nonExistingPartitions);
    cubeql.pruneCandidateFactSet(CubeTableCause.NO_CANDIDATE_STORAGES);
  }

  private void resolveDimStorageTablesAndPartitions(CubeQueryContext cubeql) throws SemanticException {
    Set<Dimension> allDims = new HashSet<Dimension>(cubeql.getDimensions());
    allDims.addAll(cubeql.getOptionalDimensions());
    for (Dimension dim : allDims) {
      Set<CandidateDim> dimTables = cubeql.getCandidateDimTables().get(dim);
      if (dimTables == null || dimTables.isEmpty()) {
        continue;
      }
      for (Iterator<CandidateDim> i = dimTables.iterator(); i.hasNext();) {
        CandidateDim candidate = i.next();
        CubeDimensionTable dimtable = candidate.dimtable;
        if (dimtable.getStorages().isEmpty()) {
          cubeql.addDimPruningMsgs(dim, dimtable, new CandidateTablePruneCause(dimtable.getName(),
              CubeTableCause.MISSING_STORAGES));
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
            try {
              if (!client.tableExists(tableName)) {
                LOG.info("Not considering dim storage table:" + tableName + ", as it does not exist");
                skipStorageCauses.put(tableName, SkipStorageCause.TABLE_NOT_EXIST);
                continue;
              }
            } catch (HiveException e) {
              throw new SemanticException(e);
            }
            if (validDimTables != null && !validDimTables.contains(tableName)) {
              LOG.info("Not considering dim storage table:" + tableName + " as it is not a valid dim storage");
              skipStorageCauses.put(tableName, SkipStorageCause.INVALID);
              continue;
            }

            if (dimtable.hasStorageSnapshots(storage)) {
              // check if partition exists
              int numParts;
              try {
                numParts =
                    client.getNumPartitionsByFilter(tableName,
                        getDimFilter(dim.getTimedDimension(), StorageConstants.LATEST_PARTITION_VALUE));
              } catch (Exception e) {
                e.printStackTrace();
                throw new SemanticException("Could not check if partition exists on " + dim, e);
              }
              if (numParts > 0) {
                LOG.info("Adding existing partition" + StorageConstants.LATEST_PARTITION_VALUE);
                foundPart = true;
              } else {
                LOG.info("Partition " + StorageConstants.LATEST_PARTITION_VALUE + " does not exist on " + tableName);
              }
              if (!failOnPartialData || (failOnPartialData && numParts > 0)) {
                storageTables.add(tableName);
                String whereClause =
                    StorageUtil.getWherePartClause(dim.getTimedDimension(), cubeql.getAliasForTabName(dim.getName()),
                        StorageConstants.getPartitionsForLatest());
                whereClauses.put(tableName, whereClause);
              } else {
                LOG.info("Not considering dim storage table:" + tableName + " as no dim partitions exist");
                skipStorageCauses.put(tableName, SkipStorageCause.NO_PARTITIONS);
              }
            } else {
              storageTables.add(tableName);
              foundPart = true;
            }
          } else {
            LOG.info("Storage:" + storage + " is not supported");
            skipStorageCauses.put(storage, SkipStorageCause.UNSUPPORTED);
          }
        }
        if (!foundPart) {
          addNonExistingParts(dim.getName(), StorageConstants.getPartitionsForLatest());
        }
        if (storageTables.isEmpty()) {
          LOG.info("Not considering dim table:" + dimtable + " as no candidate storage tables eixst");
          CandidateTablePruneCause cause =
              new CandidateTablePruneCause(dimtable.getName(), CubeTableCause.NO_CANDIDATE_STORAGES);
          cause.setStorageCauses(skipStorageCauses);
          cubeql.addDimPruningMsgs(dim, dimtable, cause);
          i.remove();
          continue;
        }
        // pick the first storage table
        candidate.storageTable = storageTables.iterator().next();
        candidate.whereClause = whereClauses.get(candidate.storageTable);
      }
    }
  }

  private String getDimFilter(String partCol, String partSpec) {
    StringBuilder builder = new StringBuilder();
    builder.append(partCol);
    builder.append("='").append(partSpec).append("'");
    return builder.toString();
  }

  // Resolves all the storage table names, which are valid for each updatePeriod
  private void resolveFactStorageTableNames(CubeQueryContext cubeql) throws SemanticException {
    for (Iterator<CandidateFact> i = cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
      CubeFactTable fact = i.next().fact;
      if (fact.getUpdatePeriods().isEmpty()) {
        cubeql.addFactPruningMsgs(fact, new CandidateTablePruneCause(fact.getName(), CubeTableCause.MISSING_STORAGES));
        i.remove();
        continue;
      }
      Map<UpdatePeriod, Set<String>> storageTableMap = new TreeMap<UpdatePeriod, Set<String>>();
      validStorageMap.put(fact, storageTableMap);
      String str = conf.get(CubeQueryConfUtil.getValidStorageTablesKey(fact.getName()));
      List<String> validFactStorageTables =
          StringUtils.isBlank(str) ? null : Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
      Map<String, SkipStorageCause> skipStorageCauses = new HashMap<String, SkipStorageCause>();
      Map<String, Map<String, SkipUpdatePeriodCause>> updatePeriodCauses =
          new HashMap<String, Map<String, SkipUpdatePeriodCause>>();

      for (Map.Entry<String, Set<UpdatePeriod>> entry : fact.getUpdatePeriods().entrySet()) {
        String storage = entry.getKey();
        // skip storages that are not supported
        if (!isStorageSupported(storage)) {
          LOG.info("Skipping storage: " + storage + " as it is not supported");
          skipStorageCauses.put(storage, SkipStorageCause.UNSUPPORTED);
          continue;
        }
        String tableName;
        // skip the update period if the storage is not valid
        if ((tableName = getStorageTableName(fact, storage, validFactStorageTables)) == null) {
          skipStorageCauses.put(storage, SkipStorageCause.INVALID);
          continue;
        }
        List<String> validUpdatePeriods =
            CubeQueryConfUtil.getStringList(conf, CubeQueryConfUtil.getValidUpdatePeriodsKey(fact.getName(), storage));

        boolean isStorageAdded = false;
        Map<String, SkipUpdatePeriodCause> skipUpdatePeriodCauses = new HashMap<String, SkipUpdatePeriodCause>();
        for (UpdatePeriod updatePeriod : entry.getValue()) {
          if (maxInterval != null && updatePeriod.compareTo(maxInterval) > 0) {
            LOG.info("Skipping update period " + updatePeriod + " for fact" + fact);
            skipUpdatePeriodCauses.put(updatePeriod.toString(), SkipUpdatePeriodCause.QUERY_INTERVAL_BIGGER);
            continue;
          }
          if (validUpdatePeriods != null && !validUpdatePeriods.contains(updatePeriod.name().toLowerCase())) {
            LOG.info("Skipping update period " + updatePeriod + " for fact" + fact + " for storage" + storage);
            skipUpdatePeriodCauses.put(updatePeriod.toString(), SkipUpdatePeriodCause.INVALID);
            continue;
          }
          Set<String> storageTables = storageTableMap.get(updatePeriod);
          if (storageTables == null) {
            storageTables = new LinkedHashSet<String>();
            storageTableMap.put(updatePeriod, storageTables);
          }
          isStorageAdded = true;
          LOG.info("Adding storage table:" + tableName + " for fact:" + fact + " for update period" + updatePeriod);
          storageTables.add(tableName);
        }
        if (!isStorageAdded) {
          skipStorageCauses.put(storage, SkipStorageCause.NO_CANDIDATE_PERIODS);
          updatePeriodCauses.put(storage, skipUpdatePeriodCauses);
        }
      }
      if (storageTableMap.isEmpty()) {
        LOG.info("Not considering fact table:" + fact + " as it does not" + " have any storage tables");
        CandidateTablePruneCause cause =
            new CandidateTablePruneCause(fact.getName(), CubeTableCause.NO_CANDIDATE_STORAGES);
        cause.setStorageCauses(skipStorageCauses);
        cause.setUpdatePeriodCauses(updatePeriodCauses);
        cubeql.addFactPruningMsgs(fact, cause);
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
    for (Iterator<CandidateFact> i = cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
      CandidateFact cfact = i.next();
      List<FactPartition> answeringParts = new ArrayList<FactPartition>();
      Map<String, SkipStorageCause> skipStorageCauses = new HashMap<String, SkipStorageCause>();
      List<String> nonExistingParts = new ArrayList<String>();
      boolean noPartsForRange = false;
      for (TimeRange range : cubeql.getTimeRanges()) {
        Set<FactPartition> rangeParts = getPartitions(cfact.fact, range, skipStorageCauses, nonExistingParts);
        if (rangeParts == null || rangeParts.isEmpty()) {
          LOG.info("No partitions for range:" + range);
          noPartsForRange = true;
          continue;
        }
        cfact.numQueriedParts += rangeParts.size();
        answeringParts.addAll(rangeParts);
        cfact.rangeToWhereClause.put(range, rangeWriter.getTimeRangeWhereClause(cubeql,
            cubeql.getAliasForTabName(cubeql.getCube().getName()), rangeParts));
      }
      if (!nonExistingParts.isEmpty()) {
        addNonExistingParts(cfact.fact.getName(), nonExistingParts);
      }
      if (cfact.numQueriedParts == 0 || (failOnPartialData && (noPartsForRange || !nonExistingParts.isEmpty()))) {
        LOG.info("Not considering fact table:" + cfact.fact + " as it could" + " not find partition for given ranges: "
            + cubeql.getTimeRanges());
        if (!skipStorageCauses.isEmpty()) {
          CandidateTablePruneCause cause = new CandidateTablePruneCause(cfact.fact.getName(), skipStorageCauses);
          cubeql.addFactPruningMsgs(cfact.fact, cause);
        }
        if (!nonExistingParts.isEmpty()) {
          CandidateTablePruneCause cause =
              new CandidateTablePruneCause(cfact.fact.getName(), CubeTableCause.MISSING_PARTITIONS);
          cause.setMissingPartitions(nonExistingParts);
          cubeql.addFactPruningMsgs(cfact.fact, cause);
        } else {
          CandidateTablePruneCause cause =
              new CandidateTablePruneCause(cfact.fact.getName(), CubeTableCause.NO_FACT_UPDATE_PERIODS_FOR_GIVEN_RANGE);
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
        CandidateTablePruneCause cause =
            new CandidateTablePruneCause(cfact.fact.getName(), CubeTableCause.NO_CANDIDATE_STORAGES);
        cause.setStorageCauses(skipStorageCauses);
        cubeql.addFactPruningMsgs(cfact.fact, cause);
        i.remove();
        continue;
      }
      Set<String> storageTables = new LinkedHashSet<String>();
      storageTables.addAll(minimalStorageTables.keySet());
      cfact.storageTables = storageTables;
      // multi table select is already false, do not alter it
      if (cfact.enabledMultiTableSelect) {
        cfact.enabledMultiTableSelect = enabledMultiTableSelect;
      }
      LOG.info("Resolved partitions for fact " + cfact + ": " + answeringParts + " storageTables:" + storageTables);
    }
  }

  void addNonExistingParts(String name, List<String> nonExistingParts) {
    nonExistingPartitions.put(name, nonExistingParts);
  }

  private Set<FactPartition> getPartitions(CubeFactTable fact, TimeRange range,
      Map<String, SkipStorageCause> skipStorageCauses, List<String> nonExistingParts) throws SemanticException {
    try {
      return getPartitions(fact, range, getValidUpdatePeriods(fact), populateNonExistingParts, skipStorageCauses,
          nonExistingParts);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private Set<FactPartition> getPartitions(CubeFactTable fact, TimeRange range, TreeSet<UpdatePeriod> updatePeriods,
      boolean addNonExistingParts, Map<String, SkipStorageCause> skipStorageCauses, List<String> nonExistingParts)
      throws Exception {
    Set<FactPartition> partitions = new TreeSet<FactPartition>();
    if (getPartitions(fact, range.getFromDate(), range.getToDate(), range.getPartitionColumn(), null, partitions,
        updatePeriods, addNonExistingParts, skipStorageCauses, nonExistingParts)) {
      return partitions;
    } else {
      return null;
    }
  }

  private boolean getPartitions(CubeFactTable fact, Date fromDate, Date toDate, String partCol,
      FactPartition containingPart, Set<FactPartition> partitions, TreeSet<UpdatePeriod> updatePeriods,
      boolean addNonExistingParts, Map<String, SkipStorageCause> skipStorageCauses, List<String> nonExistingParts)
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
        skipStorageCauses.put(storageTableName, SkipStorageCause.PART_COL_DOES_NOT_EXIST);
        it.remove();
        continue;
      }
      if (containingPart != null) {
        if (!client.partColExists(storageTableName, containingPart.getPartCol())) {
          LOG.info(partCol + " does not exist in" + storageTableName);
          skipStorageCauses.put(storageTableName, SkipStorageCause.PART_COL_DOES_NOT_EXIST);
          it.remove();
          continue;
        }
      }
    }

    if (storageTbls.isEmpty()) {
      return false;
    }

    Date ceilFromDate = DateUtil.getCeilDate(fromDate, interval);
    Date floorToDate = DateUtil.getFloorDate(toDate, interval);

    // add partitions from ceilFrom to floorTo
    Calendar cal = Calendar.getInstance();
    cal.setTime(ceilFromDate);
    Date dt = cal.getTime();
    long numIters = DateUtil.getTimeDiff(ceilFromDate, floorToDate, interval);
    int i = 1;
    int lookAheadNumParts =
        conf.getInt(CubeQueryConfUtil.getLookAheadPTPartsKey(interval), CubeQueryConfUtil.DEFAULT_LOOK_AHEAD_PT_PARTS);
    boolean leastInterval = updatePeriods.first().equals(interval);
    while (dt.compareTo(floorToDate) < 0) {
      cal.add(interval.calendarField(), 1);
      boolean foundPart = false;
      FactPartition part = new FactPartition(partCol, dt, interval, containingPart, partWhereClauseFormat);
      Map<String, List<Partition>> metaParts = new HashMap<String, List<Partition>>();
      for (String storageTableName : storageTbls) {
        int numParts;
        if (leastInterval) {
          numParts = client.getNumPartitionsByFilter(storageTableName, part.getFilter());
        } else {
          List<Partition> sParts = client.getPartitionsByFilter(storageTableName, part.getFilter());
          metaParts.put(storageTableName, sParts);
          numParts = sParts.size();
        }
        if (numParts > 0) {
          if (!foundPart) {
            LOG.info("Adding existing partition" + part);
            partitions.add(part);
            foundPart = true;
          }
          part.getStorageTables().add(storageTableName);
        } else {
          LOG.info("Partition " + part + " does not exist on " + storageTableName);
        }
      }
      if (containingPart == null) {
        if (!foundPart) {
          LOG.info("Partition:" + part + " does not exist in any storage table");
          TreeSet<UpdatePeriod> newset = new TreeSet<UpdatePeriod>();
          newset.addAll(updatePeriods);
          newset.remove(interval);
          if (!getPartitions(fact, dt, cal.getTime(), partCol, null, partitions, newset, false, skipStorageCauses,
              nonExistingParts)) {
            if (addNonExistingParts) {
              LOG.info("Adding non existing partition" + part);
              if (!failOnPartialData) {
                partitions.add(part);
                foundPart = true;
                // add all storage tables as the answering tables
                part.getStorageTables().addAll(storageTbls);
              }
              nonExistingParts.add(part.getPartString());
            } else {
              LOG.info("No finer granual partitions exist for" + part);
              return false;
            }
          } else {
            LOG.info("Finer granual partitions added for " + part);
          }
        } else if (processTimePartCol != null) {
          LOG.info("Looking for look ahead process time partitions for " + part);
          if (!partCol.equals(processTimePartCol)) {
            if (!leastInterval) {
              // see if this is the part of the last-n look ahead partitions
              if ((numIters - i) <= lookAheadNumParts) {
                LOG.info("Looking for look ahead process time partitions for " + part);
                // check if finer partitions are required
                // final partitions are required if no partitions from
                // look-ahead
                // process time are present
                Calendar processCal = Calendar.getInstance();
                processCal.setTime(cal.getTime());
                Date start = processCal.getTime();
                processCal.add(interval.calendarField(), lookAheadNumParts);
                Date end = processCal.getTime();
                Calendar temp = Calendar.getInstance();
                temp.setTime(start);
                while (temp.getTime().compareTo(end) < 0) {
                  Date pdt = temp.getTime();
                  String lPart = interval.format().format(pdt);
                  temp.add(interval.calendarField(), 1);
                  Boolean foundLookAheadParts = false;
                  for (Map.Entry<String, List<Partition>> entry : metaParts.entrySet()) {
                    for (Partition mpart : entry.getValue()) {
                      if (mpart.getValues().get(0).contains(lPart)) {
                        LOG.info("Founr lPart in " + mpart + " in table:" + entry.getKey());
                        foundLookAheadParts = true;
                        break;
                      }
                    }
                  }
                  if (!foundLookAheadParts) {
                    LOG.info("Looked ahead process time partition " + lPart + " is not found");
                    TreeSet<UpdatePeriod> newset = new TreeSet<UpdatePeriod>();
                    newset.addAll(updatePeriods);
                    newset.remove(interval);
                    LOG.info("newset of update periods:" + newset);
                    if (!newset.isEmpty()) {
                      // Get partitions for look ahead process time
                      Set<FactPartition> processTimeParts = new TreeSet<FactPartition>();
                      LOG.info("Looking for process time partitions between " + pdt + " and " + temp.getTime());
                      getPartitions(fact, pdt, temp.getTime(), processTimePartCol, null, processTimeParts, newset,
                          false, skipStorageCauses, nonExistingParts);
                      if (!processTimeParts.isEmpty()) {
                        for (FactPartition pPart : processTimeParts) {
                          LOG.info("Looking for finer partitions in pPart" + pPart);
                          if (!getPartitions(fact, dt, cal.getTime(), partCol, pPart, partitions, newset, false,
                              skipStorageCauses, nonExistingParts)) {
                            LOG.info("No partitions found in look ahead range");
                          }
                        }
                      } else {
                        LOG.info("No look ahead partitions found");
                      }
                    }
                  } else {
                    LOG.info("Finer parts not required for look-ahead partition :" + part);
                  }
                }
              } else {
                LOG.info("Not a look ahead partition");
              }
            } else {
              LOG.info("Update period is the least update period");
            }
          } else {
            LOG.info("part column is process time col");
          }
        }
      }
      dt = cal.getTime();
      i++;
    }
    if (containingPart == null) {
      return (getPartitions(fact, fromDate, ceilFromDate, partCol, null, partitions, updatePeriods,
          addNonExistingParts, skipStorageCauses, nonExistingParts) && getPartitions(fact, floorToDate, toDate,
          partCol, null, partitions, updatePeriods, addNonExistingParts, skipStorageCauses, nonExistingParts));
    } else {
      return true;
    }
  }
}
