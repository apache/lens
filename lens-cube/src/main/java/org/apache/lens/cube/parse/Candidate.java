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
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

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
  Collection<Candidate> getChildren();

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
   * @param expr     :Expression need to be evaluated for Candidate
   * @return
   */
  boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr);

  /**
   * Gets the index positions of answerable measure phrases in CubeQueryContext#selectPhrases
   * @return
   */
  Set<Integer> getAnswerableMeasurePhraseIndices();

}
