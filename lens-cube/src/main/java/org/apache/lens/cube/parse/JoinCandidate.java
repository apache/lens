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

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.util.*;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Getter;

/**
 * Represents a join of two candidates
 */
public class JoinCandidate implements Candidate {

  /**
   * Child candidates that will participate in the join
   */
  @Getter
  private List<Candidate> children;
  private String toStr;
  @Getter
  private CubeQueryContext cubeQueryContext;

  public JoinCandidate(Candidate childCandidate1, Candidate childCandidate2, CubeQueryContext cubeql) {
    children = Lists.newArrayList(childCandidate1, childCandidate2);
    this.cubeQueryContext = cubeql;
  }

  @Override
  public Collection<String> getColumns() {
    Set<String> columns = new HashSet<>();
    for (Candidate child : children) {
      columns.addAll(child.getColumns());
    }
    return columns;
  }

  @Override
  public Date getStartTime() {
    return children.stream().map(Candidate::getStartTime).max(Comparator.naturalOrder()).orElse(new Date(MIN_VALUE));
  }

  @Override
  public Date getEndTime() {
    return children.stream().map(Candidate::getEndTime).min(Comparator.naturalOrder()).orElse(new Date(MAX_VALUE));
  }

  @Override
  public OptionalDouble getCost() {
    double cost = 0;
    for (Candidate candidate : getChildren()) {
      if (candidate.getCost().isPresent()) {
        cost += candidate.getCost().getAsDouble();
      } else {
        return OptionalDouble.empty();
      }
    }
    return OptionalDouble.of(cost);
  }

  @Override
  public boolean contains(final Candidate candidate) {
    return this.equals(candidate) || children.stream().anyMatch(c -> c.contains(candidate));
  }
  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, TimeRange parentTimeRange, boolean failOnPartialData)
    throws LensException {
    for (Candidate child : children) {
      if (!child.evaluateCompleteness(timeRange, parentTimeRange, failOnPartialData)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return all the partitions from the children
   */
  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    return children.stream().map(Candidate::getParticipatingPartitions).flatMap(Collection::stream).collect(toSet());
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    // implied that expression always has measure
    return children.stream().anyMatch(x->x.isExpressionEvaluable(expr));
  }

  @Override
  public boolean isExpressionEvaluable(String expr) {
    return children.stream().anyMatch(x->x.isExpressionEvaluable(expr));
  }

  @Override
  public boolean isDimAttributeEvaluable(String dim) throws LensException {
    for (Candidate childCandidate : children) {
      if (childCandidate.isDimAttributeEvaluable(dim)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Set<Integer> getAnswerableMeasurePhraseIndices() {
    return children.stream().map(Candidate::getAnswerableMeasurePhraseIndices)
      .flatMap(Collection::stream).collect(toSet());
  }

  @Override
  public boolean isPhraseAnswerable(QueriedPhraseContext phrase) throws LensException {
    for (Candidate cand : getChildren()) {
      if (!cand.isPhraseAnswerable(phrase)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void addAnswerableMeasurePhraseIndices(int index) {
    throw new IllegalArgumentException("Join candidates can't add answerable phrase indices");
  }

  @Override
  public Optional<Date> getColumnStartTime(String column) {
    return children.stream().map(x->x.getColumnStartTime(column)).filter(Optional::isPresent).map(Optional::get)
      .max(Comparator.naturalOrder());
  }

  @Override
  public Optional<Date> getColumnEndTime(String column) {
    return children.stream().map(x->x.getColumnEndTime(column)).filter(Optional::isPresent).map(Optional::get)
      .min(Comparator.naturalOrder());
  }

  @Override
  public boolean isTimeRangeCoverable(TimeRange timeRange) throws LensException {
    for (Candidate candidate : getChildren()) {
      if (!candidate.isTimeRangeCoverable(timeRange)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    if (this.toStr == null) {
      this.toStr = getToString();
    }
    return this.toStr;
  }
  public JoinCandidate explode() throws LensException {
    ListIterator<Candidate> i = children.listIterator();
    while(i.hasNext()) {
      i.set(i.next().explode());
    }
    return this;
  }

  @Override
  public Set<Integer> decideMeasurePhrasesToAnswer(Set<Integer> measureIndices) throws LensException {
    Set<Integer> remaining = Sets.newHashSet(measureIndices);
    Set<Integer> allCovered = Sets.newHashSet();
    for (Candidate child : children) {
      Set<Integer> covered = child.decideMeasurePhrasesToAnswer(remaining);
      allCovered.addAll(covered);
      remaining = Sets.difference(remaining, covered);
    }
    return allCovered;
  }

  private String getToString() {
    return children.stream().map(Object::toString).collect(joining("; ", "JOIN[", "]"));
  }
}
