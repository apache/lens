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

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.lens.cube.parse.CandidateTablePruneCause.*;
import static org.apache.lens.cube.parse.StorageUtil.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metastore.DataCompletenessChecker;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.ReflectionUtils;

import org.antlr.runtime.CommonToken;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a fact on a storage table and the dimensions it needs to be joined with to answer the query
 */
@Slf4j
public class StorageCandidate implements Candidate, CandidateTable {

  // TODO union : Put comments on member variables.
  @Getter
  private final CubeQueryContext cubeql;
  private final TimeRangeWriter rangeWriter;
  private final String processTimePartCol;
  private final CubeMetastoreClient client;
  private final String completenessPartCol;
  private final float completenessThreshold;
  @Getter
  private final String name;
  /**
   * Valid udpate periods populated by Phase 1.
   */
  @Getter
  private TreeSet<UpdatePeriod> validUpdatePeriods = new TreeSet<>();
  private Configuration conf = null;
  @Getter
  private Map<String, Map<String, Float>> dataCompletenessMap = new HashMap<>();
  private SimpleDateFormat partWhereClauseFormat = null;
  /**
   * Participating fact, storage and dimensions for this StorageCandidate
   */
  @Getter
  private CubeFactTable fact;
  @Getter
  private String storageName;
  @Getter
  @Setter
  private QueryAST queryAst;
  private Map<Dimension, CandidateDim> dimensions;
  @Getter
  private Map<TimeRange, String> rangeToWhere = new LinkedHashMap<>();
  @Getter
  @Setter
  private String whereString;
  @Getter
  private final Set<Integer> answerableMeasurePhraseIndices = Sets.newHashSet();
  @Getter
  @Setter
  private String fromString;
  @Getter
  private CubeInterface cube;
  @Getter
  Map<Dimension, CandidateDim> dimsToQuery;
  /**
   * Cached fact columns
   */
  private Collection<String> factColumns;
  /**
   * This map holds Tags (A tag refers to one or more measures) that have incomplete (below configured threshold) data.
   * Value is a map of date string and %completeness.
   */
  @Getter
  @Setter
  private Map<String, Map<String, Float>> incompleteDataDetails;
  /**
   * Partition calculated by getPartition() method.
   */
  @Getter
  private Set<FactPartition> participatingPartitions = new HashSet<>();
  /**
   * Non existing partitions
   */
  private Set<String> nonExistingPartitions = new HashSet<>();
  @Getter
  private int numQueriedParts = 0;

  public StorageCandidate(CubeInterface cube, CubeFactTable fact, String storageName, CubeQueryContext cubeql) {
    if ((cube == null) || (fact == null) || (storageName == null)) {
      throw new IllegalArgumentException("Cube,fact and storageName should be non null");
    }
    this.cube = cube;
    this.fact = fact;
    this.cubeql = cubeql;
    this.storageName = storageName;
    this.conf = cubeql.getConf();
    this.name = MetastoreUtil.getFactOrDimtableStorageTableName(fact.getName(), storageName);
    rangeWriter = ReflectionUtils.newInstance(conf
      .getClass(CubeQueryConfUtil.TIME_RANGE_WRITER_CLASS, CubeQueryConfUtil.DEFAULT_TIME_RANGE_WRITER,
        TimeRangeWriter.class), conf);
    this.processTimePartCol = conf.get(CubeQueryConfUtil.PROCESS_TIME_PART_COL);
    String formatStr = conf.get(CubeQueryConfUtil.PART_WHERE_CLAUSE_DATE_FORMAT);
    if (formatStr != null) {
      this.partWhereClauseFormat = new SimpleDateFormat(formatStr);
    }
    completenessPartCol = conf.get(CubeQueryConfUtil.COMPLETENESS_CHECK_PART_COL);
    client = cubeql.getMetastoreClient();
    completenessThreshold = conf
      .getFloat(CubeQueryConfUtil.COMPLETENESS_THRESHOLD, CubeQueryConfUtil.DEFAULT_COMPLETENESS_THRESHOLD);
  }

  public StorageCandidate(StorageCandidate sc) {
    this(sc.getCube(), sc.getFact(), sc.getStorageName(), sc.getCubeql());
    // Copy update periods.
    for (UpdatePeriod updatePeriod : sc.getValidUpdatePeriods()) {
      this.validUpdatePeriods.add(updatePeriod);
    }
  }

  static boolean containsAny(Collection<String> srcSet, Collection<String> colSet) {
    if (colSet == null || colSet.isEmpty()) {
      return true;
    }
    for (String column : colSet) {
      if (srcSet.contains(column)) {
        return true;
      }
    }
    return false;
  }

  private void setMissingExpressions() throws LensException {
    setFromString(String.format("%s", getFromTable()));
    setWhereString(joinWithAnd(whereString, null));
    if (cubeql.getHavingAST() != null) {
      queryAst.setHavingAST(MetastoreUtil.copyAST(cubeql.getHavingAST()));
    }
  }

  public void setAnswerableMeasurePhraseIndices(int index) {
    answerableMeasurePhraseIndices.add(index);
  }

  public String toHQL() throws LensException {
    setMissingExpressions();
    // Check if the picked candidate is a StorageCandidate and in that case
    // update the selectAST with final alias.
    if (this == cubeql.getPickedCandidate()) {
      CandidateUtil.updateFinalAlias(queryAst.getSelectAST(), cubeql);
    }
    return CandidateUtil
      .buildHQLString(queryAst.getSelectString(), fromString, whereString, queryAst.getGroupByString(),
        queryAst.getOrderByString(), queryAst.getHavingString(), queryAst.getLimitValue());
  }

  @Override
  public String getStorageString(String alias) {
    return storageName + " " + alias;
  }

  @Override
  public AbstractCubeTable getTable() {
    return fact;
  }

  @Override
  public AbstractCubeTable getBaseTable() {
    return (AbstractCubeTable) cube;
  }

  @Override
  public Collection<String> getColumns() {
    if (factColumns == null) {
      factColumns = fact.getValidColumns();
      if (factColumns == null) {
        factColumns = fact.getAllFieldNames();
      }
    }
    return factColumns;
  }

  @Override
  public Date getStartTime() {
    // TODO union : get storage stat time and take max out of it
    return fact.getStartTime();
  }

  @Override
  public Date getEndTime() {
    return fact.getEndTime();
  }

  @Override
  public double getCost() {
    return fact.weight();
  }

  @Override
  public boolean contains(Candidate candidate) {
    return this.equals(candidate);
  }

  @Override
  public Collection<Candidate> getChildren() {
    return null;
  }

  private void updatePartitionStorage(FactPartition part) throws LensException {
    try {
      if (client.isStorageTablePartitionACandidate(name, part.getPartSpec()) && (client
        .factPartitionExists(fact, part, name))) {
        part.getStorageTables().add(name);
        part.setFound(true);
      }
    } catch (HiveException e) {
      log.warn("Hive exception while getting storage table partition", e);
    }
  }

  /**
   * Gets FactPartitions for the given fact using the following logic
   *
   * 1. Find the max update interval that will be used for the query. Lets assume time range is 15 Sep to 15 Dec and the
   * fact has two storage with update periods as MONTHLY,DAILY,HOURLY. In this case the data for
   * [15 sep - 1 oct)U[1 Dec - 15 Dec) will be answered by DAILY partitions and [1 oct - 1Dec) will be answered by
   * MONTHLY partitions. The max interavl for this query will be MONTHLY.
   *
   * 2.Prune Storgaes that do not fall in the queries time range.
   * {@link CubeMetastoreClient#isStorageTableCandidateForRange(String, Date, Date)}
   *
   * 3. Iterate over max interavl . In out case it will give two months Oct and Nov. Find partitions for these two months.
   * Check validity of FactPartitions for Oct and Nov via {@link #updatePartitionStorage(FactPartition)}.
   * If the partition is missing, try getting partitions for the time range form other update periods (DAILY,HOURLY).This
   * is achieved by calling getPartitions() recursively but passing only 2 update periods (DAILY,HOURLY)
   *
   * 4.If the monthly partitions are found, check for lookahead partitions and call getPartitions recursively for the
   * remaining time intervals i.e, [15 sep - 1 oct) and [1 Dec - 15 Dec)
   *
   * TODO union : Move this into util.
   */
  private boolean getPartitions(Date fromDate, Date toDate, String partCol, Set<FactPartition> partitions,
    TreeSet<UpdatePeriod> updatePeriods, boolean addNonExistingParts, boolean failOnPartialData,
    PartitionRangesForPartitionColumns missingPartitions) throws LensException {
    if (fromDate.equals(toDate) || fromDate.after(toDate)) {
      return true;
    }
    UpdatePeriod interval = CubeFactTable.maxIntervalInRange(fromDate, toDate, updatePeriods);
    if (interval == null) {
      log.info("No max interval for range: {} to {}", fromDate, toDate);
      return false;
    }

    if (interval == UpdatePeriod.CONTINUOUS && rangeWriter.getClass().equals(BetweenTimeRangeWriter.class)) {
      FactPartition part = new FactPartition(partCol, fromDate, interval, null, partWhereClauseFormat);
      partitions.add(part);
      part.getStorageTables().add(storageName);
      part = new FactPartition(partCol, toDate, interval, null, partWhereClauseFormat);
      partitions.add(part);
      part.getStorageTables().add(storageName);
      log.info("Added continuous fact partition for storage table {}", storageName);
      return true;
    }

    if (!client.isStorageTableCandidateForRange(name, fromDate, toDate)) {
      cubeql.addStoragePruningMsg(this,
        new CandidateTablePruneCause(CandidateTablePruneCause.CandidateTablePruneCode.TIME_RANGE_NOT_ANSWERABLE));
      return false;
    } else if (!client.partColExists(name, partCol)) {
      log.info("{} does not exist in {}", partCol, name);
      List<String> missingCols = new ArrayList<>();
      missingCols.add(partCol);
      //      cubeql.addStoragePruningMsg(this, partitionColumnsMissing(missingCols));
      return false;
    }

    Date ceilFromDate = DateUtil.getCeilDate(fromDate, interval);
    Date floorToDate = DateUtil.getFloorDate(toDate, interval);

    int lookAheadNumParts = conf
      .getInt(CubeQueryConfUtil.getLookAheadPTPartsKey(interval), CubeQueryConfUtil.DEFAULT_LOOK_AHEAD_PT_PARTS);

    TimeRange.Iterable.Iterator iter = TimeRange.iterable(ceilFromDate, floorToDate, interval, 1).iterator();
    // add partitions from ceilFrom to floorTo
    while (iter.hasNext()) {
      Date dt = iter.next();
      Date nextDt = iter.peekNext();
      FactPartition part = new FactPartition(partCol, dt, interval, null, partWhereClauseFormat);
      updatePartitionStorage(part);
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
          TimeRange.Iterable.Iterator processTimeIter = TimeRange.iterable(nextDt, lookAheadNumParts, interval, 1)
            .iterator();
          while (processTimeIter.hasNext()) {
            Date pdt = processTimeIter.next();
            Date nextPdt = processTimeIter.peekNext();
            FactPartition processTimePartition = new FactPartition(processTimePartCol, pdt, interval, null,
              partWhereClauseFormat);
            updatePartitionStorage(processTimePartition);
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
                Set<FactPartition> processTimeParts = getPartitions(
                  TimeRange.getBuilder().fromDate(pdt).toDate(nextPdt).partitionColumn(processTimePartCol).build(),
                  newset, true, failOnPartialData, missingPartitions);
                log.debug("Look ahead partitions: {}", processTimeParts);
                TimeRange timeRange = TimeRange.getBuilder().fromDate(dt).toDate(nextDt).build();
                for (FactPartition pPart : processTimeParts) {
                  log.debug("Looking for finer partitions in pPart: {}", pPart);
                  for (Date date : timeRange.iterable(pPart.getPeriod(), 1)) {
                    FactPartition innerPart = new FactPartition(partCol, date, pPart.getPeriod(), pPart,
                      partWhereClauseFormat);
                    updatePartitionStorage(innerPart);
                    innerPart.setFound(pPart.isFound());
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
        TreeSet<UpdatePeriod> newset = new TreeSet<>();
        newset.addAll(updatePeriods);
        newset.remove(interval);
        if (!getPartitions(dt, nextDt, partCol, partitions, newset, false, failOnPartialData, missingPartitions)) {
          log.debug("Adding non existing partition {}", part);
          if (addNonExistingParts) {
            // Add non existing partitions for all cases of whether we populate all non existing or not.
            missingPartitions.add(part);
            if (!failOnPartialData) {
              if (!client.isStorageTablePartitionACandidate(name, part.getPartSpec())) {
                log.info("Storage tables not eligible");
                return false;
              }
              partitions.add(part);
              part.getStorageTables().add(storageName);
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
    return
      getPartitions(fromDate, ceilFromDate, partCol, partitions, updatePeriods, addNonExistingParts, failOnPartialData,
        missingPartitions) && getPartitions(floorToDate, toDate, partCol, partitions, updatePeriods,
        addNonExistingParts, failOnPartialData, missingPartitions);
  }

  /**
   * Finds all the partitions for a storage table with a particular time range.
   *
   * @param timeRange         : TimeRange to check completeness for. TimeRange consists of start time, end time and the
   *                          partition column
   * @param failOnPartialData : fail fast if the candidate can answer the query only partially
   * @return Steps:
   * 1. Get skip storage causes
   * 2. getPartitions for timeRange and validUpdatePeriods
   */
  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, TimeRange parentTimeRange, boolean failOnPartialData)
    throws LensException {
    // Check the measure tags.
    if (!evaluateMeasuresCompleteness(timeRange)) {
      log
        .info("Fact table:{} has partitions with incomplete data: {} for given ranges: {}", fact, dataCompletenessMap,
          cubeql.getTimeRanges());
      cubeql.addStoragePruningMsg(this, incompletePartitions(dataCompletenessMap));
      if (failOnPartialData) {
        return false;
      }
    }
    PartitionRangesForPartitionColumns missingParts = new PartitionRangesForPartitionColumns();
    PruneCauses<StorageCandidate> storagePruningMsgs = cubeql.getStoragePruningMsgs();
    Set<String> unsupportedTimeDims = Sets.newHashSet();
    Set<String> partColsQueried = Sets.newHashSet();
    partColsQueried.add(timeRange.getPartitionColumn());
    StringBuilder extraWhereClauseFallback = new StringBuilder();
    Set<FactPartition> rangeParts = getPartitions(timeRange, validUpdatePeriods, true, failOnPartialData, missingParts);
    String partCol = timeRange.getPartitionColumn();
    boolean partColNotSupported = rangeParts.isEmpty();
    String storageTableName = getName();

    if (storagePruningMsgs.containsKey(storageTableName)) {
      List<CandidateTablePruneCause> causes = storagePruningMsgs.get(storageTableName);
      // Find the PART_COL_DOES_NOT_EXISTS
      for (CandidateTablePruneCause cause : causes) {
        if (cause.getCause().equals(CandidateTablePruneCode.PART_COL_DOES_NOT_EXIST)) {
          partColNotSupported &= cause.getNonExistantPartCols().contains(partCol);
        }
      }
    } else {
      partColNotSupported = false;
    }
    TimeRange prevRange = timeRange;
    String sep = "";
    while (rangeParts.isEmpty()) {
      String timeDim = cubeql.getBaseCube().getTimeDimOfPartitionColumn(partCol);
      if (partColNotSupported && !getFact().getColumns().contains(timeDim)) {
        unsupportedTimeDims.add(cubeql.getBaseCube().getTimeDimOfPartitionColumn(timeRange.getPartitionColumn()));
        break;
      }
      TimeRange fallBackRange = getFallbackRange(prevRange, this.getFact().getName(), cubeql);
      log.info("No partitions for range:{}. fallback range: {}", timeRange, fallBackRange);
      if (fallBackRange == null) {
        break;
      }
      partColsQueried.add(fallBackRange.getPartitionColumn());
      rangeParts = getPartitions(fallBackRange, validUpdatePeriods, true, failOnPartialData, missingParts);
      extraWhereClauseFallback.append(sep)
        .append(prevRange.toTimeDimWhereClause(cubeql.getAliasForTableName(cubeql.getCube()), timeDim));
      sep = " AND ";
      prevRange = fallBackRange;
      partCol = prevRange.getPartitionColumn();
      if (!rangeParts.isEmpty()) {
        break;
      }
    }
    numQueriedParts += rangeParts.size();
    if (!unsupportedTimeDims.isEmpty()) {
      log.info("Not considering fact table:{} as it doesn't support time dimensions: {}", this.getFact(),
        unsupportedTimeDims);
      cubeql.addStoragePruningMsg(this, timeDimNotSupported(unsupportedTimeDims));
      return false;
    }
    Set<String> nonExistingParts = missingParts.toSet(partColsQueried);
    // TODO union : Relook at this.
    nonExistingPartitions.addAll(nonExistingParts);
    if (rangeParts.size() == 0 || (failOnPartialData && !nonExistingParts.isEmpty())) {
      log.info("No partitions for fallback range:{}", timeRange);
      return false;
    }
    String extraWhere = extraWhereClauseFallback.toString();
    if (!StringUtils.isEmpty(extraWhere)) {
      rangeToWhere.put(parentTimeRange, "((" + rangeWriter
        .getTimeRangeWhereClause(cubeql, cubeql.getAliasForTableName(cubeql.getCube().getName()), rangeParts)
        + ") and  (" + extraWhere + "))");
    } else {
      rangeToWhere.put(parentTimeRange, rangeWriter
        .getTimeRangeWhereClause(cubeql, cubeql.getAliasForTableName(cubeql.getCube().getName()), rangeParts));
    }
    // Add all the partitions. participatingPartitions contains all the partitions for previous time ranges also.
    this.participatingPartitions.addAll(rangeParts);
    return true;
  }

  private boolean evaluateMeasuresCompleteness(TimeRange timeRange) throws LensException {
    String factDataCompletenessTag = fact.getDataCompletenessTag();
    if (factDataCompletenessTag == null) {
      log.info("Not checking completeness for the fact table:{} as the dataCompletenessTag is not set", fact);
      return true;
    }
    Set<String> measureTag = new HashSet<>();
    Map<String, String> tagToMeasureOrExprMap = new HashMap<>();

    processExpressionsForCompleteness(cubeql, measureTag, tagToMeasureOrExprMap);

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
      return true;
    }
    boolean isDataComplete = false;
    DataCompletenessChecker completenessChecker = client.getCompletenessChecker();
    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    if (!timeRange.getPartitionColumn().equals(completenessPartCol)) {
      log.info("Completeness check not available for partCol:{}", timeRange.getPartitionColumn());
      return true;
    }
    Date from = timeRange.getFromDate();
    Date to = timeRange.getToDate();
    Map<String, Map<Date, Float>> completenessMap = completenessChecker
      .getCompleteness(factDataCompletenessTag, from, to, measureTag);
    if (completenessMap != null && !completenessMap.isEmpty()) {
      for (Map.Entry<String, Map<Date, Float>> measureCompleteness : completenessMap.entrySet()) {
        String tag = measureCompleteness.getKey();
        for (Map.Entry<Date, Float> completenessResult : measureCompleteness.getValue().entrySet()) {
          if (completenessResult.getValue() < completenessThreshold) {
            log.info("Completeness for the measure_tag {} is {}, threshold: {}, for the hour {}", tag,
              completenessResult.getValue(), completenessThreshold, formatter.format(completenessResult.getKey()));
            String measureorExprFromTag = tagToMeasureOrExprMap.get(tag);
            Map<String, Float> incompletePartition = dataCompletenessMap.get(measureorExprFromTag);
            if (incompletePartition == null) {
              incompletePartition = new HashMap<>();
              dataCompletenessMap.put(measureorExprFromTag, incompletePartition);
            }
            incompletePartition.put(formatter.format(completenessResult.getKey()), completenessResult.getValue());
            isDataComplete = true;
          }
        }
      }
    }
    return isDataComplete;
  }

  private Set<FactPartition> getPartitions(TimeRange timeRange, TreeSet<UpdatePeriod> updatePeriods,
    boolean addNonExistingParts, boolean failOnPartialData, PartitionRangesForPartitionColumns missingParts)
    throws LensException {
    Set<FactPartition> partitions = new TreeSet<>();
    if (timeRange != null && timeRange.isCoverableBy(updatePeriods) && getPartitions(timeRange.getFromDate(),
      timeRange.getToDate(), timeRange.getPartitionColumn(), partitions, updatePeriods, addNonExistingParts,
      failOnPartialData, missingParts)) {
      return partitions;
    }
    return new TreeSet<>();
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    return expr.isEvaluable(this);
  }

  /**
   * Update selectAST for StorageCandidate
   * 1. Delete projected select expression if it's not answerable by StorageCandidate.
   * 2. Replace the queried alias with select alias if both are different in a select expr.
   *
   * @param cubeql
   * @throws LensException
   */

  public void updateAnswerableSelectColumns(CubeQueryContext cubeql) throws LensException {
    // update select AST with selected fields
    int currentChild = 0;
    for (int i = 0; i < cubeql.getSelectAST().getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) queryAst.getSelectAST().getChild(currentChild);
      Set<String> exprCols = HQLParser.getColsInExpr(cubeql.getAliasForTableName(cubeql.getCube()), selectExpr);
      if (getColumns().containsAll(exprCols)) {
        ASTNode aliasNode = HQLParser.findNodeByPath(selectExpr, Identifier);
        String alias = cubeql.getSelectPhrases().get(i).getSelectAlias();
        if (aliasNode != null) {
          String queryAlias = aliasNode.getText();
          if (!queryAlias.equals(alias)) {
            // replace the alias node
            ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, alias));
            queryAst.getSelectAST().getChild(currentChild)
              .replaceChildren(selectExpr.getChildCount() - 1, selectExpr.getChildCount() - 1, newAliasNode);
          }
        } else {
          // add column alias
          ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, alias));
          queryAst.getSelectAST().getChild(currentChild).addChild(newAliasNode);
        }
      } else {
        queryAst.getSelectAST().deleteChild(currentChild);
        currentChild--;
      }
      currentChild++;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (super.equals(obj)) {
      return true;
    }

    if (obj == null || !(obj instanceof StorageCandidate)) {
      return false;
    }

    StorageCandidate storageCandidateObj = (StorageCandidate) obj;
    //Assuming that same instance of cube and fact will be used across StorageCandidate s and hence relying directly
    //on == check for these.
    return (this.cube == storageCandidateObj.cube && this.fact == storageCandidateObj.fact && this.storageName
      .equals(storageCandidateObj.storageName));
  }

  @Override
  public int hashCode() {
    return this.name.hashCode();
  }

  @Override
  public String toString() {
    return getName();
  }

  public void addValidUpdatePeriod(UpdatePeriod updatePeriod) {
    this.validUpdatePeriods.add(updatePeriod);
  }

  public void updateFromString(CubeQueryContext query, Set<Dimension> queryDims,
    Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    this.dimsToQuery = dimsToQuery;
    String alias = cubeql.getAliasForTableName(cubeql.getCube().getName());
    fromString = getAliasForTable(alias);
    if (query.isAutoJoinResolved()) {
      fromString = query.getAutoJoinCtx().getFromString(fromString, this, queryDims, dimsToQuery, query, cubeql);
    }
  }

  private String getFromTable() throws LensException {
    if (cubeql.isAutoJoinResolved()) {
        return fromString;
    } else {
        return cubeql.getQBFromString(this, getDimsToQuery());
    }
  }

  public String getAliasForTable(String alias) {
    String database = SessionState.get().getCurrentDatabase();
    String ret;
    if (alias == null || alias.isEmpty()) {
      ret = name;
    } else {
      ret = name + " " + alias;
    }
    if (StringUtils.isNotBlank(database) && !"default".equalsIgnoreCase(database)) {
      ret = database + "." + ret;
    }
    return ret;
  }

}
