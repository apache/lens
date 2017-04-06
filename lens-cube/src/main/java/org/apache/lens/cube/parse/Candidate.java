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

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
   * @return
   */
  Collection<String> getColumns();

  default boolean hasColumn(String column) {
    return getColumns().contains(column);
  }

  /**
   * Start Time for this candidate (calculated based on schema)
   *
   * @return
   */
  Date getStartTime();

  /**
   * End Time for this candidate (calculated based on schema)
   *
   * @return
   */
  Date getEndTime();

  /**
   * Returns the cost of this candidate
   *
   * @return
   */
  double getCost();

  /**
   * Returns true if this candidate contains the given candidate
   *
   * @param candidate
   * @return
   */
  boolean contains(Candidate candidate);

  /**
   * Returns child candidates of this candidate if any.
   * Note: StorageCandidate will return null
   *
   * @return
   */
  Collection<? extends Candidate> getChildren();

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
   * @param timeRange
   * @return
   * @throws LensException
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
   * @return
   */
  Set<FactPartition> getParticipatingPartitions();

  /**
   * Checks whether an expression is evaluable by a candidate
   * 1. For a JoinCandidate, atleast one of the child candidates should be able to answer the expression
   * 2. For a UnionCandidate, all child candidates should answer the expression
   *
   * @param expressionContext     :Expression need to be evaluated for Candidate
   * @return
   */
  boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expressionContext);

  boolean isExpressionEvaluable(String expr);

  boolean isDimAttributeEvaluable(String dim) throws LensException;

  /**
   * Gets the index positions of answerable measure phrases in CubeQueryContext#selectPhrases
   * @return
   */
  Set<Integer> getAnswerableMeasurePhraseIndices();

  default Candidate copy() throws LensException {
    throw new LensException("Candidate " + this + " doesn't support copy");
  }

  boolean isPhraseAnswerable(QueriedPhraseContext phrase) throws LensException;

  default boolean isColumnPresentAndValidForRange(String column) throws LensException {
    return getColumns().contains(column) && isColumnValidForRange(column);
  }

  // todo: split into two methods
  // todo: override in union candidate since column times might not be contiguous in children
  default boolean isColumnValidForRange(String column) {
    Optional<Date> start = getColumnStartTime(column);
    Optional<Date> end = getColumnEndTime(column);
    return (!start.isPresent()
      || getCubeQueryContext().getTimeRanges().stream().noneMatch(range -> range.getFromDate().before(start.get())))
      && (!end.isPresent()
      || getCubeQueryContext().getTimeRanges().stream().noneMatch(range -> range.getToDate().after(end.get())));
  }

  Optional<Date> getColumnStartTime(String column);

  Optional<Date> getColumnEndTime(String column);

  CubeQueryContext getCubeQueryContext();
  default Configuration getConf() {
    return getCubeQueryContext().getConf();
  }
  default CubeMetastoreClient getCubeMetastoreClient() {
    return getCubeQueryContext().getMetastoreClient();
  }
  default void addAnswerableMeasurePhraseIndices(int index) {
    throw new UnsupportedOperationException("Can't add answerable measure index");
  }
  default <T extends CubeInterface> T getCube() {
    return (T) getCubeQueryContext().getCube();
  }
  default Set<QueriedPhraseContext> coveredMeasures(Set<QueriedPhraseContext> msrs) throws LensException {
    Set<QueriedPhraseContext> covered = Sets.newHashSet();
    for (QueriedPhraseContext msr : msrs) {
      if (isPhraseAnswerable(msr)) {
        covered.add(msr);
      }
    }
    return covered;
  }

  default Candidate explode() throws LensException {
    if (getChildren() != null) {
      for (Candidate candidate : getChildren()) {
        candidate.explode();
      }
    }
    return this;
  }

  default QueryWriterContext toQueryWriterContext(Map<Dimension, CandidateDim> dimsToQuery) throws LensException {
    if (getChildren() != null) {
      List<QueryWriterContext> writerContexts = Lists.newArrayList();
      for (Candidate candidate : getChildren()) {
        QueryWriterContext child = candidate.toQueryWriterContext(dimsToQuery);
        if (child instanceof StorageCandidateHQLContext) {
          ((StorageCandidateHQLContext) child).setRootCubeQueryContext(getCubeQueryContext());
          child.getQueryAst().setHavingAST(null);
        }
        writerContexts.add(child); //todo try to remove exception
      }
      return new MultiCandidateQueryWriterContext(writerContexts, getCubeQueryContext());
    }
    throw new LensException("blah");
  }
}
