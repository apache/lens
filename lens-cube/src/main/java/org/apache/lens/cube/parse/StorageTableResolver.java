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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.lens.cube.metadata.DateUtil.WSPACE;
import static org.apache.lens.cube.metadata.MetastoreUtil.*;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.*;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.*;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.SkipStorageCode.*;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.cube.parse.CandidateTablePruneCause.*;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metastore.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
  CubeMetastoreClient client;
  private final boolean failOnPartialData;
  private final List<String> validDimTables;
  private final Map<CubeFactTable, Map<UpdatePeriod, Set<String>>> validStorageMap = new HashMap<>();
  private String processTimePartCol = null;
  private final UpdatePeriod maxInterval;
  private final Map<String, Set<String>> nonExistingPartitions = new HashMap<>();
  private TimeRangeWriter rangeWriter;
  private DateFormat partWhereClauseFormat = null;
  private PHASE phase;
  private HashMap<CubeFactTable, Map<String, SkipStorageCause>> skipStorageCausesPerFact;
  private float completenessThreshold;
  private String completenessPartCol;

  enum PHASE {
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
    completenessThreshold = conf.getFloat(CubeQueryConfUtil.COMPLETENESS_THRESHOLD,
            CubeQueryConfUtil.DEFAULT_COMPLETENESS_THRESHOLD);
    completenessPartCol = conf.get(CubeQueryConfUtil.COMPLETENESS_CHECK_PART_COL);
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
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    client = cubeql.getMetastoreClient();

    switch (phase) {
    case FACT_TABLES:
      if (!cubeql.getCandidateFacts().isEmpty()) {
        // resolve storage table names
        resolveFactStorageTableNames(cubeql);
      }
      cubeql.pruneCandidateFactSet(CandidateTablePruneCode.NO_CANDIDATE_STORAGES);
      break;
    case FACT_PARTITIONS:
      if (!cubeql.getCandidateFacts().isEmpty()) {
        // resolve storage partitions
        resolveFactStoragePartitions(cubeql);
      }
      cubeql.pruneCandidateFactSet(CandidateTablePruneCode.NO_CANDIDATE_STORAGES);
      if (client != null && client.isDataCompletenessCheckEnabled()) {
        if (!cubeql.getCandidateFacts().isEmpty()) {
          // resolve incomplete fact partition
          resolveFactCompleteness(cubeql);
        }
        cubeql.pruneCandidateFactSet(CandidateTablePruneCode.INCOMPLETE_PARTITION);
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
      break;
    }
    //Doing this on all three phases. Keep updating cubeql with the current identified missing partitions.
    cubeql.setNonexistingParts(nonExistingPartitions);
    phase = phase.next();
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
          cubeql.addDimPruningMsgs(dim, dimtable, new CandidateTablePruneCause(
            CandidateTablePruneCode.MISSING_STORAGES));
          i.remove();
          continue;
        }
        Set<String> storageTables = new HashSet<String>();
        Map<String, String> whereClauses = new HashMap<String, String>();
        boolean foundPart = false;
        Map<String, SkipStorageCause> skipStorageCauses = new HashMap<>();
        for (String storage : dimtable.getStorages()) {
          if (isStorageSupported(storage)) {
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
                String whereClause =
                  StorageUtil.getWherePartClause(dim.getTimedDimension(), null,
                    StorageConstants.getPartitionsForLatest());
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
        candidate.setStorageTable(storageTables.iterator().next());
        candidate.setWhereClause(whereClauses.get(candidate.getStorageTable()));
      }
    }
  }

  // Resolves all the storage table names, which are valid for each updatePeriod
  private void resolveFactStorageTableNames(CubeQueryContext cubeql) throws LensException {
    Iterator<CandidateFact> i = cubeql.getCandidateFacts().iterator();
    skipStorageCausesPerFact = new HashMap<>();
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
      Map<String, SkipStorageCause> skipStorageCauses = new HashMap<>();

      for (Map.Entry<String, Set<UpdatePeriod>> entry : fact.getUpdatePeriods().entrySet()) {
        String storage = entry.getKey();
        // skip storages that are not supported
        if (!isStorageSupported(storage)) {
          log.info("Skipping storage: {} as it is not supported", storage);
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
            log.info("Skipping update period {} for fact {}", updatePeriod, fact);
            skipUpdatePeriodCauses.put(updatePeriod.toString(), SkipUpdatePeriodCode.QUERY_INTERVAL_BIGGER);
            continue;
          }
          if (validUpdatePeriods != null && !validUpdatePeriods.contains(updatePeriod.name().toLowerCase())) {
            log.info("Skipping update period {} for fact {} for storage {}", updatePeriod, fact, storage);
            skipUpdatePeriodCauses.put(updatePeriod.toString(), SkipUpdatePeriodCode.INVALID);
            continue;
          }
          Set<String> storageTables = storageTableMap.get(updatePeriod);
          if (storageTables == null) {
            storageTables = new LinkedHashSet<>();
            storageTableMap.put(updatePeriod, storageTables);
          }
          isStorageAdded = true;
          log.debug("Adding storage table:{} for fact:{} for update period {}", table, fact, updatePeriod);
          storageTables.add(table);
        }
        if (!isStorageAdded) {
          skipStorageCauses.put(storage, SkipStorageCause.noCandidateUpdatePeriod(skipUpdatePeriodCauses));
        }
      }
      skipStorageCausesPerFact.put(fact, skipStorageCauses);
      if (storageTableMap.isEmpty()) {
        log.info("Not considering fact table:{} as it does not have any storage tables", fact);
        cubeql.addFactPruningMsgs(fact, noCandidateStorages(skipStorageCauses));
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
    String tableName = getFactOrDimtableStorageTableName(fact.getName(), storage).toLowerCase();
    if (validFactStorageTables != null && !validFactStorageTables.contains(tableName)) {
      log.info("Skipping storage table {} as it is not valid", tableName);
      return null;
    }
    return tableName;
  }

  private TimeRange getFallbackRange(TimeRange range, CandidateFact cfact, CubeQueryContext cubeql)
    throws LensException {
    Cube baseCube = cubeql.getBaseCube();
    ArrayList<String> tableNames = Lists.newArrayList(cfact.fact.getName(), cubeql.getCube().getName());
    if (!cubeql.getCube().getName().equals(baseCube.getName())) {
      tableNames.add(baseCube.getName());
    }
    String fallBackString = null;
    String timedim = baseCube.getTimeDimOfPartitionColumn(range.getPartitionColumn());
    for (String tableName : tableNames) {
      fallBackString = cubeql.getMetastoreClient().getTable(tableName).getParameters()
        .get(MetastoreConstants.TIMEDIM_RELATION + timedim);
      if (StringUtils.isNotBlank(fallBackString)) {
        break;
      }
    }
    if (StringUtils.isBlank(fallBackString)) {
      return null;
    }
    Matcher matcher = Pattern.compile("(.*?)\\+\\[(.*?),(.*?)\\]").matcher(fallBackString.replaceAll(WSPACE, ""));
    if (!matcher.matches()) {
      return null;
    }
    DateUtil.TimeDiff diff1 = DateUtil.TimeDiff.parseFrom(matcher.group(2).trim());
    DateUtil.TimeDiff diff2 = DateUtil.TimeDiff.parseFrom(matcher.group(3).trim());
    String relatedTimeDim = matcher.group(1).trim();
    String fallbackPartCol = baseCube.getPartitionColumnOfTimeDim(relatedTimeDim);
    return TimeRange.getBuilder()
      .fromDate(diff2.negativeOffsetFrom(range.getFromDate()))
      .toDate(diff1.negativeOffsetFrom(range.getToDate()))
      .partitionColumn(fallbackPartCol).build();
  }

  private void resolveFactStoragePartitions(CubeQueryContext cubeql) throws LensException {
    // Find candidate tables wrt supported storages
    Iterator<CandidateFact> i = cubeql.getCandidateFacts().iterator();
    while (i.hasNext()) {
      CandidateFact cfact = i.next();
      Map<TimeRange, String> whereClauseForFallback = new LinkedHashMap<TimeRange, String>();
      List<FactPartition> answeringParts = new ArrayList<>();
      Map<String, SkipStorageCause> skipStorageCauses = skipStorageCausesPerFact.get(cfact.fact);
      if (skipStorageCauses == null) {
        skipStorageCauses = new HashMap<>();
      }
      PartitionRangesForPartitionColumns missingParts = new PartitionRangesForPartitionColumns();
      boolean noPartsForRange = false;
      Set<String> unsupportedTimeDims = Sets.newHashSet();
      Set<String> partColsQueried = Sets.newHashSet();
      for (TimeRange range : cubeql.getTimeRanges()) {
        partColsQueried.add(range.getPartitionColumn());
        StringBuilder extraWhereClause = new StringBuilder();
        Set<FactPartition> rangeParts = getPartitions(cfact.fact, range, skipStorageCauses, missingParts);
        // If no partitions were found, then we'll fallback.
        String partCol = range.getPartitionColumn();
        boolean partColNotSupported = rangeParts.isEmpty();
        for (String storage : cfact.fact.getStorages()) {
          String storageTableName = getFactOrDimtableStorageTableName(cfact.fact.getName(), storage).toLowerCase();
          partColNotSupported &= skipStorageCauses.containsKey(storageTableName)
            && skipStorageCauses.get(storageTableName).getCause().equals(PART_COL_DOES_NOT_EXIST)
            && skipStorageCauses.get(storageTableName).getNonExistantPartCols().contains(partCol);
        }
        TimeRange prevRange = range;
        String sep = "";
        while (rangeParts.isEmpty()) {
          // TODO: should we add a condition whether on range's partcol any missing partitions are not there
          String timeDim = cubeql.getBaseCube().getTimeDimOfPartitionColumn(partCol);
          if (partColNotSupported && !cfact.getColumns().contains(timeDim)) {
            unsupportedTimeDims.add(cubeql.getBaseCube().getTimeDimOfPartitionColumn(range.getPartitionColumn()));
            break;
          }
          TimeRange fallBackRange = getFallbackRange(prevRange, cfact, cubeql);
          log.info("No partitions for range:{}. fallback range: {}", range, fallBackRange);
          if (fallBackRange == null) {
            break;
          }
          partColsQueried.add(fallBackRange.getPartitionColumn());
          rangeParts = getPartitions(cfact.fact, fallBackRange, skipStorageCauses, missingParts);
          extraWhereClause.append(sep)
            .append(prevRange.toTimeDimWhereClause(cubeql.getAliasForTableName(cubeql.getCube()), timeDim));
          sep = " AND ";
          prevRange = fallBackRange;
          partCol = prevRange.getPartitionColumn();
          if (!rangeParts.isEmpty()) {
            break;
          }
        }
        whereClauseForFallback.put(range, extraWhereClause.toString());
        if (rangeParts.isEmpty()) {
          log.info("No partitions for fallback range:{}", range);
          noPartsForRange = true;
          continue;
        }
        // If multiple storage tables are part of the same fact,
        // capture range->storage->partitions
        Map<String, LinkedHashSet<FactPartition>> tablePartMap = new HashMap<String, LinkedHashSet<FactPartition>>();
        for (FactPartition factPart : rangeParts) {
          for (String table : factPart.getStorageTables()) {
            if (!tablePartMap.containsKey(table)) {
              tablePartMap.put(table, new LinkedHashSet<>(Collections.singletonList(factPart)));
            } else {
              LinkedHashSet<FactPartition> storagePart = tablePartMap.get(table);
              storagePart.add(factPart);
            }
          }
        }
        cfact.getRangeToStoragePartMap().put(range, tablePartMap);
        cfact.incrementPartsQueried(rangeParts.size());
        answeringParts.addAll(rangeParts);
        cfact.getPartsQueried().addAll(rangeParts);
      }
      if (!unsupportedTimeDims.isEmpty()) {
        log.info("Not considering fact table:{} as it doesn't support time dimensions: {}", cfact.fact,
          unsupportedTimeDims);
        cubeql.addFactPruningMsgs(cfact.fact, timeDimNotSupported(unsupportedTimeDims));
        i.remove();
        continue;
      }
      Set<String> nonExistingParts = missingParts.toSet(partColsQueried);
      if (!nonExistingParts.isEmpty()) {
        addNonExistingParts(cfact.fact.getName(), nonExistingParts);
      }
      if (cfact.getNumQueriedParts() == 0 || (failOnPartialData && (noPartsForRange || !nonExistingParts.isEmpty()))) {
        log.info("Not considering fact table:{} as it could not find partition for given ranges: {}", cfact.fact,
          cubeql.getTimeRanges());
        /*
         * This fact is getting discarded because of any of following reasons:
         * 1. Has missing partitions
         * 2. All Storage tables were skipped for some reasons.
         * 3. Storage tables do not have the update period for the timerange queried.
         */
        if (failOnPartialData && !nonExistingParts.isEmpty()) {
          cubeql.addFactPruningMsgs(cfact.fact, missingPartitions(nonExistingParts));
        } else if (!skipStorageCauses.isEmpty()) {
          CandidateTablePruneCause cause = noCandidateStorages(skipStorageCauses);
          cubeql.addFactPruningMsgs(cfact.fact, cause);
        } else {
          CandidateTablePruneCause cause =
            new CandidateTablePruneCause(NO_FACT_UPDATE_PERIODS_FOR_GIVEN_RANGE);
          cubeql.addFactPruningMsgs(cfact.fact, cause);
        }
        i.remove();
        continue;
      }
      // Map from storage to covering parts
      Map<String, Set<FactPartition>> minimalStorageTables = new LinkedHashMap<String, Set<FactPartition>>();
      StorageUtil.getMinimalAnsweringTables(answeringParts, minimalStorageTables);
      if (minimalStorageTables.isEmpty()) {
        log.info("Not considering fact table:{} as it does not have any storage tables", cfact);
        cubeql.addFactPruningMsgs(cfact.fact, noCandidateStorages(skipStorageCauses));
        i.remove();
        continue;
      }
      Set<String> storageTables = new LinkedHashSet<>();
      storageTables.addAll(minimalStorageTables.keySet());
      cfact.setStorageTables(storageTables);
      // Update range->storage->partitions with time range where clause
      for (TimeRange trange : cfact.getRangeToStoragePartMap().keySet()) {
        Map<String, String> rangeToWhere = new HashMap<>();
        for (Map.Entry<String, Set<FactPartition>> entry : minimalStorageTables.entrySet()) {
          String table = entry.getKey();
          Set<FactPartition> minimalParts = entry.getValue();

          LinkedHashSet<FactPartition> rangeParts = cfact.getRangeToStoragePartMap().get(trange).get(table);
          LinkedHashSet<FactPartition> minimalPartsCopy = Sets.newLinkedHashSet();

          if (rangeParts != null) {
            minimalPartsCopy.addAll(minimalParts);
            minimalPartsCopy.retainAll(rangeParts);
          }
          if (!StringUtils.isEmpty(whereClauseForFallback.get(trange))) {
            rangeToWhere.put(table, "(("
              + rangeWriter.getTimeRangeWhereClause(cubeql, cubeql.getAliasForTableName(cubeql.getCube().getName()),
                minimalPartsCopy) + ") and  (" + whereClauseForFallback.get(trange) + "))");
          } else {
            rangeToWhere.put(table, rangeWriter.getTimeRangeWhereClause(cubeql,
              cubeql.getAliasForTableName(cubeql.getCube().getName()), minimalPartsCopy));
          }
        }
        cfact.getRangeToStorageWhereMap().put(trange, rangeToWhere);
      }
      log.info("Resolved partitions for fact {}: {} storageTables:{}", cfact, answeringParts, storageTables);
    }
  }

  private static boolean processCubeColForDataCompleteness(CubeQueryContext cubeql, String cubeCol, String alias,
                                                        Set<String> measureTag,
                                                        Map<String, String> tagToMeasureOrExprMap) {
    CubeMeasure column = cubeql.getCube().getMeasureByName(cubeCol);
    if (column != null && column.getTags() != null) {
      String dataCompletenessTag = column.getTags().get(MetastoreConstants.MEASURE_DATACOMPLETENESS_TAG);
      //Checking if dataCompletenessTag is set for queried measure
      if (dataCompletenessTag != null) {
        measureTag.add(dataCompletenessTag);
        String value = tagToMeasureOrExprMap.get(dataCompletenessTag);
        if (value == null) {
          tagToMeasureOrExprMap.put(dataCompletenessTag, alias);
        } else {
          value = value.concat(",").concat(alias);
          tagToMeasureOrExprMap.put(dataCompletenessTag, value);
        }
        return true;
      }
    }
    return false;
  }

  private static void processMeasuresFromExprMeasures(CubeQueryContext cubeql, Set<String> measureTag,
                                                             Map<String, String> tagToMeasureOrExprMap) {
    boolean isExprProcessed;
    String cubeAlias = cubeql.getAliasForTableName(cubeql.getCube().getName());
    for (String expr : cubeql.getQueriedExprsWithMeasures()) {
      isExprProcessed = false;
      for (ExpressionResolver.ExprSpecContext esc : cubeql.getExprCtx().getExpressionContext(expr, cubeAlias)
              .getAllExprs()) {
        if (esc.getTblAliasToColumns().get(cubeAlias) != null) {
          for (String cubeCol : esc.getTblAliasToColumns().get(cubeAlias)) {
            if (processCubeColForDataCompleteness(cubeql, cubeCol, expr, measureTag, tagToMeasureOrExprMap)) {
              /* This is done to associate the expression with one of the dataCompletenessTag for the measures.
              So, even if the expression is composed of measures with different dataCompletenessTags, we will be
              determining the dataCompleteness from one of the measure and this expression is grouped with the
              other queried measures that have the same dataCompletenessTag. */
              isExprProcessed = true;
              break;
            }
          }
        }
        if (isExprProcessed) {
          break;
        }
      }
    }
  }

  private void resolveFactCompleteness(CubeQueryContext cubeql) throws LensException {
    if (client == null || client.getCompletenessChecker() == null || completenessPartCol == null) {
      return;
    }
    DataCompletenessChecker completenessChecker = client.getCompletenessChecker();
    Set<String> measureTag = new HashSet<>();
    Map<String, String> tagToMeasureOrExprMap = new HashMap<>();

    processMeasuresFromExprMeasures(cubeql, measureTag, tagToMeasureOrExprMap);

    Set<String> measures = cubeql.getQueriedMsrs();
    if (measures == null) {
      measures = new HashSet<>();
    }
    for (String measure : measures) {
      processCubeColForDataCompleteness(cubeql, measure, measure, measureTag, tagToMeasureOrExprMap);
    }
    //Checking if dataCompletenessTag is set for the fact
    if (measureTag.isEmpty()) {
      log.info("No Queried measures with the dataCompletenessTag, hence skipping the availability check");
      return;
    }
    Iterator<CandidateFact> i = cubeql.getCandidateFacts().iterator();
    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    while (i.hasNext()) {
      CandidateFact cFact = i.next();
      // Map from measure to the map from partition to %completeness
      Map<String, Map<String, Float>> incompleteMeasureData = new HashMap<>();

      String factDataCompletenessTag = cFact.fact.getDataCompletenessTag();
      if (factDataCompletenessTag == null) {
        log.info("Not checking completeness for the fact table:{} as the dataCompletenessTag is not set", cFact.fact);
        continue;
      }
      boolean isFactDataIncomplete = false;
      for (TimeRange range : cubeql.getTimeRanges()) {
        if (!range.getPartitionColumn().equals(completenessPartCol)) {
          log.info("Completeness check not available for partCol:{}", range.getPartitionColumn());
          continue;
        }
        Date from = range.getFromDate();
        Date to = range.getToDate();
        Map<String, Map<Date, Float>> completenessMap =  completenessChecker.getCompleteness(factDataCompletenessTag,
                from, to, measureTag);
        if (completenessMap != null && !completenessMap.isEmpty()) {
          for (Map.Entry<String, Map<Date, Float>> measureCompleteness : completenessMap.entrySet()) {
            String tag = measureCompleteness.getKey();
            for (Map.Entry<Date, Float> completenessResult : measureCompleteness.getValue().entrySet()) {
              if (completenessResult.getValue() < completenessThreshold) {
                log.info("Completeness for the measure_tag {} is {}, threshold: {}, for the hour {}", tag,
                        completenessResult.getValue(), completenessThreshold,
                        formatter.format(completenessResult.getKey()));
                String measureorExprFromTag = tagToMeasureOrExprMap.get(tag);
                Map<String, Float> incompletePartition = incompleteMeasureData.get(measureorExprFromTag);
                if (incompletePartition == null) {
                  incompletePartition = new HashMap<>();
                  incompleteMeasureData.put(measureorExprFromTag, incompletePartition);
                }
                incompletePartition.put(formatter.format(completenessResult.getKey()), completenessResult.getValue());
                isFactDataIncomplete = true;
              }
            }
          }
        }
      }
      if (isFactDataIncomplete) {
        log.info("Fact table:{} has partitions with incomplete data: {} for given ranges: {}", cFact.fact,
                incompleteMeasureData, cubeql.getTimeRanges());
        if (failOnPartialData) {
          i.remove();
          cubeql.addFactPruningMsgs(cFact.fact, incompletePartitions(incompleteMeasureData));
        } else {
          cFact.setDataCompletenessMap(incompleteMeasureData);
        }
      }
    }
  }

  void addNonExistingParts(String name, Set<String> nonExistingParts) {
    nonExistingPartitions.put(name, nonExistingParts);
  }

  private Set<FactPartition> getPartitions(CubeFactTable fact, TimeRange range,
    Map<String, SkipStorageCause> skipStorageCauses,
    PartitionRangesForPartitionColumns missingPartitions) throws LensException {
    try {
      return getPartitions(fact, range, getValidUpdatePeriods(fact), true, failOnPartialData, skipStorageCauses,
        missingPartitions);
    } catch (Exception e) {
      throw new LensException(e);
    }
  }

  private Set<FactPartition> getPartitions(CubeFactTable fact, TimeRange range, TreeSet<UpdatePeriod> updatePeriods,
    boolean addNonExistingParts, boolean failOnPartialData, Map<String, SkipStorageCause> skipStorageCauses,
    PartitionRangesForPartitionColumns missingPartitions)
    throws Exception {
    Set<FactPartition> partitions = new TreeSet<>();
    if (range != null && range.isCoverableBy(updatePeriods)
      && getPartitions(fact, range.getFromDate(), range.getToDate(), range.getPartitionColumn(), partitions,
        updatePeriods, addNonExistingParts, failOnPartialData, skipStorageCauses, missingPartitions)) {
      return partitions;
    } else {
      return new TreeSet<>();
    }
  }

  private boolean getPartitions(CubeFactTable fact, Date fromDate, Date toDate, String partCol,
    Set<FactPartition> partitions, TreeSet<UpdatePeriod> updatePeriods,
    boolean addNonExistingParts, boolean failOnPartialData, Map<String, SkipStorageCause> skipStorageCauses,
    PartitionRangesForPartitionColumns missingPartitions)
    throws Exception {
    log.info("getPartitions for {} from fromDate:{} toDate:{}", fact, fromDate, toDate);
    if (fromDate.equals(toDate) || fromDate.after(toDate)) {
      return true;
    }
    UpdatePeriod interval = CubeFactTable.maxIntervalInRange(fromDate, toDate, updatePeriods);
    if (interval == null) {
      log.info("No max interval for range: {} to {}", fromDate, toDate);
      return false;
    }
    log.debug("Max interval for {} is: {}", fact, interval);
    Set<String> storageTbls = new LinkedHashSet<String>();
    storageTbls.addAll(validStorageMap.get(fact).get(interval));

    if (interval == UpdatePeriod.CONTINUOUS && rangeWriter.getClass().equals(BetweenTimeRangeWriter.class)) {
      for (String storageTbl : storageTbls) {
        FactPartition part = new FactPartition(partCol, fromDate, interval, null, partWhereClauseFormat);
        partitions.add(part);
        part.getStorageTables().add(storageTbl);
        part = new FactPartition(partCol, toDate, interval, null, partWhereClauseFormat);
        partitions.add(part);
        part.getStorageTables().add(storageTbl);
        log.info("Added continuous fact partition for storage table {}", storageTbl);
      }
      return true;
    }

    Iterator<String> it = storageTbls.iterator();
    while (it.hasNext()) {
      String storageTableName = it.next();
      if (!client.isStorageTableCandidateForRange(storageTableName, fromDate, toDate)) {
        skipStorageCauses.put(storageTableName, new SkipStorageCause(RANGE_NOT_ANSWERABLE));
        it.remove();
      } else if (!client.partColExists(storageTableName, partCol)) {
        log.info("{} does not exist in {}", partCol, storageTableName);
        skipStorageCauses.put(storageTableName, SkipStorageCause.partColDoesNotExist(partCol));
        it.remove();
      }
    }

    if (storageTbls.isEmpty()) {
      return false;
    }
    Date ceilFromDate = DateUtil.getCeilDate(fromDate, interval);
    Date floorToDate = DateUtil.getFloorDate(toDate, interval);

    int lookAheadNumParts =
      conf.getInt(CubeQueryConfUtil.getLookAheadPTPartsKey(interval), CubeQueryConfUtil.DEFAULT_LOOK_AHEAD_PT_PARTS);

    TimeRange.Iterable.Iterator iter = TimeRange.iterable(ceilFromDate, floorToDate, interval, 1).iterator();
    // add partitions from ceilFrom to floorTo
    while (iter.hasNext()) {
      Date dt = iter.next();
      Date nextDt = iter.peekNext();
      FactPartition part = new FactPartition(partCol, dt, interval, null, partWhereClauseFormat);
      log.debug("candidate storage tables for searching partitions: {}", storageTbls);
      updateFactPartitionStorageTablesFrom(fact, part, storageTbls);
      log.debug("Storage tables containing Partition {} are: {}", part, part.getStorageTables());
      if (part.isFound()) {
        log.debug("Adding existing partition {}", part);
        partitions.add(part);
        log.debug("Looking for look ahead process time partitions for {}", part);
        if (processTimePartCol == null) {
          log.debug("processTimePartCol is null");
        } else if (partCol.equals(processTimePartCol)) {
          log.debug("part column is process time col");
        } else if (updatePeriods.first().equals(interval)) {
          log.debug("Update period is the least update period");
        } else if ((iter.getNumIters() - iter.getCounter()) > lookAheadNumParts) {
          // see if this is the part of the last-n look ahead partitions
          log.debug("Not a look ahead partition");
        } else {
          log.debug("Looking for look ahead process time partitions for {}", part);
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
              log.debug("Finer parts not required for look-ahead partition :{}", part);
            } else {
              log.debug("Looked ahead process time partition {} is not found", processTimePartition);
              TreeSet<UpdatePeriod> newset = new TreeSet<UpdatePeriod>();
              newset.addAll(updatePeriods);
              newset.remove(interval);
              log.debug("newset of update periods:{}", newset);
              if (!newset.isEmpty()) {
                // Get partitions for look ahead process time
                log.debug("Looking for process time partitions between {} and {}", pdt, nextPdt);
                Set<FactPartition> processTimeParts =
                  getPartitions(fact, TimeRange.getBuilder().fromDate(pdt).toDate(nextPdt).partitionColumn(
                    processTimePartCol).build(), newset, true, false, skipStorageCauses, missingPartitions);
                log.debug("Look ahead partitions: {}", processTimeParts);
                TimeRange timeRange = TimeRange.getBuilder().fromDate(dt).toDate(nextDt).build();
                for (FactPartition pPart : processTimeParts) {
                  log.debug("Looking for finer partitions in pPart: {}", pPart);
                  for (Date date : timeRange.iterable(pPart.getPeriod(), 1)) {
                    FactPartition innerPart = new FactPartition(partCol, date, pPart.getPeriod(), pPart,
                      partWhereClauseFormat);
                    updateFactPartitionStorageTablesFrom(fact, innerPart, pPart);
                    if (innerPart.isFound()) {
                      partitions.add(innerPart);
                    }
                  }
                  log.debug("added all sub partitions blindly in pPart: {}", pPart);
                }
              }
            }
          }
        }
      } else {
        log.info("Partition:{} does not exist in any storage table", part);
        TreeSet<UpdatePeriod> newset = new TreeSet<UpdatePeriod>();
        newset.addAll(updatePeriods);
        newset.remove(interval);
        if (!getPartitions(fact, dt, nextDt, partCol, partitions, newset, false, failOnPartialData, skipStorageCauses,
          missingPartitions)) {

          log.debug("Adding non existing partition {}", part);
          if (addNonExistingParts) {
            // Add non existing partitions for all cases of whether we populate all non existing or not.
            missingPartitions.add(part);
            if (!failOnPartialData) {
              Set<String> st = getStorageTablesWithoutPartCheck(part, storageTbls);
              if (st.isEmpty()) {
                log.info("No eligible storage tables");
                return false;
              }
              partitions.add(part);
              part.getStorageTables().addAll(st);
            }
          } else {
            log.info("No finer granual partitions exist for {}", part);
            return false;
          }
        } else {
          log.debug("Finer granual partitions added for {}", part);
        }
      }
    }
    return getPartitions(fact, fromDate, ceilFromDate, partCol, partitions,
      updatePeriods, addNonExistingParts, failOnPartialData, skipStorageCauses, missingPartitions)
      && getPartitions(fact, floorToDate, toDate, partCol, partitions,
        updatePeriods, addNonExistingParts, failOnPartialData, skipStorageCauses, missingPartitions);
  }

  private Set<String> getStorageTablesWithoutPartCheck(FactPartition part,
    Set<String> storageTableNames) throws LensException, HiveException {
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

  private void updateFactPartitionStorageTablesFrom(CubeFactTable fact,
    FactPartition part, Set<String> storageTableNames) throws LensException, HiveException, ParseException {
    for (String storageTableName : storageTableNames) {
      // skip all storage tables for which are not eligible for this partition
      if (client.isStorageTablePartitionACandidate(storageTableName, part.getPartSpec())
        && (client.factPartitionExists(fact, part, storageTableName))) {
        part.getStorageTables().add(storageTableName);
        part.setFound(true);
      }
    }
  }

  private void updateFactPartitionStorageTablesFrom(CubeFactTable fact,
    FactPartition part, FactPartition pPart) throws LensException, HiveException, ParseException {
    updateFactPartitionStorageTablesFrom(fact, part, pPart.getStorageTables());
    part.setFound(part.isFound() && pPart.isFound());
  }
}
