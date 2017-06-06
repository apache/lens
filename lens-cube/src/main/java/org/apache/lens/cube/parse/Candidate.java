/*
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

import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This interface represents candidates that are involved in different phases of query rewriting.
 * At the lowest level, Candidate is represented by a StorageCandidate that has a fact on a storage
 * and other joined dimensions (if any) that are required to answer the query or part of the query.
 * At a higher level Candidate can also be a Join or a Union Candidate representing join or union
 * between other candidates
 *
 * Different Re-writers will work on applicable candidates to produce a final candidate which will be used
 * for generating the re-written query.
 */
public interface Candidate {

  /**
   * Returns all the fact columns
   *
   * @return collection of column names
   */
  Collection<String> getColumns();

  /**
   * Returns whether this candidate has the asked column or not
   * @param column column to check
   * @return       whether this candidate contains the column
   */
  default boolean hasColumn(String column) {
    return getColumns().contains(column);
  }

  /**
   * Start Time for this candidate (calculated based on schema)
   *
   * @return start time of this candidate
   */
  Date getStartTime();

  /**
   * End Time for this candidate (calculated based on schema)
   *
   * @return end time of this candidate
   */
  Date getEndTime();

  /**
   * Returns true if the Candidate is valid for all the timeranges based on its start and end times.
   * @param timeRanges time ranges to check
   * @return           whether this candidate is completely valid for all the time ranges
   */
  default boolean isCompletelyValidForTimeRanges(List<TimeRange> timeRanges) {
    return timeRanges.stream().allMatch(range -> range.truncate(getStartTime(), getEndTime()).equals(range));
  }

  /**
   * Utility method to check whether this candidate is partially valid for any of the given time ranges
   * @param timeRanges time ranges to check
   * @return           whether this candidate is partially valid for any of the ranges
   */
  default boolean isPartiallyValidForTimeRanges(List<TimeRange> timeRanges) {
    return timeRanges.stream().map(timeRange ->
      timeRange.truncate(getStartTime(), getEndTime())).anyMatch(TimeRange::isValid);
  }

  /**
   * Utility method for checking whether this candidate is partially valid for the given time range
   * @param timeRange time range to check
   * @return          whether this candidate is partially valid
   */
  default boolean isPartiallyValidForTimeRange(TimeRange timeRange) {
    return isPartiallyValidForTimeRanges(Collections.singletonList(timeRange));
  }

  /**
   * @return the cost of this candidate
   */
  double getCost();

  /**
   * Returns true if this candidate contains the given candidate
   *
   * @param candidate candidate to check
   * @return          whether this contains the candidate in question
   */
  boolean contains(Candidate candidate);

  /**
   * Returns child candidates of this candidate if any.
   * Note: StorageCandidate will return null
   *
   * @return child candidates if this is a complex candidate, else null
   */
  Collection<? extends Candidate> getChildren();

  /**
   * Count of children
   * @return number of children it has. 0 if null.
   */
  default int getChildrenCount() {
    return Optional.ofNullable(getChildren()).map(Collection::size).orElse(0);
  }

  /**
   * Is time range coverable based on start and end times configured in schema for the composing storage candidates
   * and valid update periods.
   *
   * Note: This method is different from {@link #evaluateCompleteness(TimeRange, TimeRange, boolean)} .
   * isTimeRangeCoverable checks the the possibility of covering time range from schema perspective by using valid
   * storages/update periods while evaluateCompleteness checks if a time range can be covered based on
   * registered partitions. So isTimeRangeCoverable = false implies evaluateCompleteness = false but vice versa is
   * not true.
   *
   * @param timeRange       The time range to check
   * @return                whether this time range is coverable by this candidate
   * @throws LensException  propagated exceptions
   */
  boolean isTimeRangeCoverable(TimeRange timeRange) throws LensException;

  /**
   * Calculates if this candidate can answer the query for given time range based on actual data registered with
   * the underlying candidate storages. This method will also update any internal candidate data structures that are
   * required for writing the re-written query and to answer {@link #getParticipatingPartitions()}.
   *
   * @param timeRange         : TimeRange to check completeness for. TimeRange consists of start time, end time and the
   *                          partition column
   * @param queriedTimeRange  : User quried timerange
   * @param failOnPartialData : fail fast if the candidate can answer the query only partially
   * @return true if this Candidate can answer query for the given time range.
   */
  boolean evaluateCompleteness(TimeRange timeRange, TimeRange queriedTimeRange, boolean failOnPartialData)
    throws LensException;

  /**
   * Returns the set of fact partitions that will participate in this candidate.
   * Note: This method can be called only after call to
   * {@link #evaluateCompleteness(TimeRange, TimeRange, boolean)}
   *
   * @return a set of participating partitions
   */
  Set<FactPartition> getParticipatingPartitions();

  /**
   * Checks whether an expression is evaluable by this candidate
   * 1. For a JoinCandidate, atleast one of the child candidates should be able to answer the expression
   * 2. For a UnionCandidate, all child candidates should answer the expression
   *
   * @param expressionContext   Expression to be evaluated for Candidate
   * @return                    Whether the given expression is evaluable or not
   */
  boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expressionContext);

  /**
   * Checks whether an expression is evaluable by this candidate
   * 1. For a JoinCandidate, atleast one of the child candidates should be able to answer the expression
   * 2. For a UnionCandidate, all child candidates should answer the expression
   *
   * @param expr                Expression to be evaluated for Candidate
   * @return                    Whether the given expression is evaluable or not
   */
  boolean isExpressionEvaluable(String expr);

  /**
   * Checks whether a dim attribute is evaluable by this candidate
   * @param dim             dim attribute
   * @return                whether the dim attribute is evaluable by this candidate
   * @throws LensException  propageted exception
   */
  boolean isDimAttributeEvaluable(String dim) throws LensException;

  /**
   * Gets the index positions of answerable measure phrases in CubeQueryContext#selectPhrases
   * @return set of indices of answerable phrases
   */
  Set<Integer> getAnswerableMeasurePhraseIndices();

  /**
   * Clones this candidate
   * @return the clone
   * @throws LensException propagated exception
   */
  default Candidate copy() throws LensException {
    throw new LensException("Candidate " + this + " doesn't support copy");
  }

  /**
   * Checks whether the given queries phrase is evaluable by this candidate
   * @param phrase          Phrase to check
   * @return                whether the phrase is evaluable by this candidate
   * @throws LensException  propagated exception
   */
  boolean isPhraseAnswerable(QueriedPhraseContext phrase) throws LensException;

  /**
   * Add `index` as answerable index in a pre-decided list of queried phrases.
   * @param index index to mark as answerable
   */
  void addAnswerableMeasurePhraseIndices(int index);
  /**
   * Default method to update querieble phrase indices in candidate
   * @param qpcList         List of queries phrases
   * @throws LensException  propagated exception
   */
  default void updateStorageCandidateQueriablePhraseIndices(List<QueriedPhraseContext> qpcList) throws LensException {
    for (int index = 0; index < qpcList.size(); index++) {
      if (isPhraseAnswerable(qpcList.get(index))) {
        addAnswerableMeasurePhraseIndices(index);
      }
    }
  }

  /**
   * Utility method for clubbing column contains check and column range validity check.
   * @param column column name to check
   * @return       true if this candidate can answer this column looking at existence and time validity.
   */
  default boolean isColumnPresentAndValidForRange(String column) {
    return getColumns().contains(column) && isColumnValidForRange(column);
  }

  /**
   * Utility method for checking column time range validity.
   * @param column column to check
   * @return       true if this column is valid for all ranges queried
   */
  default boolean isColumnValidForRange(String column) {
    Optional<Date> start = getColumnStartTime(column);
    Optional<Date> end = getColumnEndTime(column);
    return (!start.isPresent()
      || getCubeQueryContext().getTimeRanges().stream().noneMatch(range -> range.getFromDate().before(start.get())))
      && (!end.isPresent()
      || getCubeQueryContext().getTimeRanges().stream().noneMatch(range -> range.getToDate().after(end.get())));
  }

  /**
   * This method should give start time of a column, if there's any. Else, this should return Optional.absent
   * @param column column name
   * @return       optional start time of this column
   */
  Optional<Date> getColumnStartTime(String column);
  /**
   * This method should give end time of a column, if there's any. Else, this should return Optional.absent
   * @param column column name
   * @return       optional end time of this column
   */
  Optional<Date> getColumnEndTime(String column);

  /**
   * A candidate always works along with its cube query context. So a top level method to retrieve that.
   * @return cube query context for this candidate.
   */
  CubeQueryContext getCubeQueryContext();

  /**
   * Utility method to return the configuration of its cube query context.
   * @return getCubeQueryContext().getConf()
   */
  default Configuration getConf() {
    return getCubeQueryContext().getConf();
  }

  /**
   * Utility method to return the metastore client of its cube query context
   * @return getCubeQueryContext().getMetastoreClient()
   */
  default CubeMetastoreClient getCubeMetastoreClient() {
    return getCubeQueryContext().getMetastoreClient();
  }

  /**
   * Utility method to return cube of its cube query context
   * @param  <T> a subclass of CubeInterface
   * @return getCubeQueryContext().getCube()
   */
  @SuppressWarnings("unchecked")
  default <T extends CubeInterface> T getCube() {
    return (T) getCubeQueryContext().getCube();
  }

  /**
   * Filters phrases that are covered by this candidate
   * @param phrases  queried phrases to check
   * @return         a set of queried phrases belonging to the list `phrases` that are answerable by this candidate
   * @throws LensException propagated exception
   */
  default Set<QueriedPhraseContext> coveredPhrases(Set<QueriedPhraseContext> phrases) throws LensException {
    Set<QueriedPhraseContext> covered = Sets.newHashSet();
    for (QueriedPhraseContext msr : phrases) {
      if (isPhraseAnswerable(msr)) {
        covered.add(msr);
      }
    }
    return covered;
  }

  /**
   * Explode this candidate into another candidate.
   * Generally candidates can return `this` in this method
   * Special case is storage candidate that returns UnionCandidate if there are multiple update periods covered and
   * update periods have different tables.
   * @return converted candidate
   */
  Candidate explode() throws LensException;

  /**
   * Get query writer context from the candidate. Default implementation is for Union, Join and Segmentation candidates.
   * In the default implementation, a MultiCandidateQueryWriterContext is returned, whose children are obtained by
   * getting the query writer contexts from the children of this candidate.
   * @param dimsToQuery               Dimensions and corresponding picked CandidateDim for query
   * @param rootCubeQueryContext      Root query context.
   * @return                          A Query Writer Context
   * @throws LensException            exception to be propagated
   */
  default QueryWriterContext toQueryWriterContext(Map<Dimension, CandidateDim> dimsToQuery,
    CubeQueryContext rootCubeQueryContext) throws LensException {
    if (getChildren() != null) {
      List<QueryWriterContext> writerContexts = Lists.newArrayList();
      for (Candidate candidate : getChildren()) {
        writerContexts.add(candidate.toQueryWriterContext(dimsToQuery, rootCubeQueryContext));
      }
      if (writerContexts.size() == 1) {
        return writerContexts.iterator().next();
      }
      return new MultiCandidateQueryWriterContext(writerContexts, rootCubeQueryContext);
    }
    throw new IllegalArgumentException("Candidate doesn't have children and no suitable implementation found");
  }
}
