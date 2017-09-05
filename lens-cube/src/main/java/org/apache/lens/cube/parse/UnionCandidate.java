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

import static java.util.Comparator.comparing;

import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;

/**
 * Represents a union of two candidates
 */
public class UnionCandidate implements Candidate {

  /**
   * Caching start and end time calculated for this candidate as it may have many child candidates.
   */
  private Date startTime = null;
  private Date endTime = null;
  private String toStr;
  @Getter
  CubeQueryContext cubeQueryContext;
  /**
   * List of child candidates that will be union-ed
   */
  @Getter
  private List<Candidate> children;

  private Map<TimeRange, Map<Candidate, TimeRange>> splitTimeRangeMap = Maps.newHashMap();
  UnionCandidate(Collection<? extends Candidate> childCandidates, CubeQueryContext cubeQueryContext) {
    this.children = Lists.newArrayList(childCandidates);
    this.cubeQueryContext = cubeQueryContext;
  }
  void cloneChildren() throws LensException {
    ListIterator<Candidate> iter = children.listIterator();
    while(iter.hasNext()) {
      iter.set(iter.next().copy());
    }
  }
  @Override
  public Set<Integer> getAnswerableMeasurePhraseIndices() {
    // All children in the UnionCandiate will be having common quriable measure
    return  getChildren().iterator().next().getAnswerableMeasurePhraseIndices();
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

  public boolean isTimeRangeCoverable(TimeRange timeRange) throws LensException {
    Map<Candidate, TimeRange> candidateRange = getTimeRangeSplit(timeRange);
    for (Map.Entry<Candidate, TimeRange> entry : candidateRange.entrySet()) {
      if (!entry.getKey().isTimeRangeCoverable(entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void addAnswerableMeasurePhraseIndices(int index) {
    for (Candidate candidate : getChildren()) {
      candidate.addAnswerableMeasurePhraseIndices(index);
    }
  }

  @Override
  public boolean isColumnValidForRange(String column) {
    return getChildren().stream().anyMatch(candidate -> candidate.isColumnValidForRange(column));
  }

  @Override
  public Optional<Date> getColumnStartTime(String column) {
    return getChildren().stream()
      .map(x->x.getColumnStartTime(column))
      .filter(Optional::isPresent)
      .map(Optional::get)
      .min(Comparator.naturalOrder());
  }

  @Override
  public Optional<Date> getColumnEndTime(String column) {
    return getChildren().stream()
      .map(x->x.getColumnEndTime(column))
      .filter(Optional::isPresent)
      .map(Optional::get)
      .max(Comparator.naturalOrder());
  }


  @Override
  public Collection<String> getColumns() {
    // In UnionCandidate all columns are same, return the columns
    // of first child
    return children.iterator().next().getColumns();
  }

  @Override
  public Date getStartTime() {
    //Note: concurrent calls not handled specifically (This should not be a problem even if we do
    //get concurrent calls).
    if (startTime == null) {
      startTime = children.stream()
        .map(Candidate::getStartTime)
        .min(Comparator.naturalOrder())
        .orElseGet(() -> new Date(Long.MIN_VALUE)); // this should be redundant.
    }
    return startTime;
  }

  @Override
  public Date getEndTime() {
    if (endTime == null) {
      endTime = children.stream()
        .map(Candidate::getEndTime)
        .max(Comparator.naturalOrder())
        .orElseGet(() -> new Date(Long.MAX_VALUE)); // this should be redundant
    }
    return endTime;
  }

  @Override
  public OptionalDouble getCost() {
    double cost = 0.0;
    for (TimeRange timeRange : getCubeQueryContext().getTimeRanges()) {
      for (Map.Entry<Candidate, TimeRange> entry : getTimeRangeSplit(timeRange).entrySet()) {
        if (entry.getKey().getCost().isPresent()) {
          cost +=  entry.getKey().getCost().getAsDouble() *entry.getValue().milliseconds() / timeRange.milliseconds();
        } else {
          return OptionalDouble.empty();
        }
      }
    }
    return OptionalDouble.of(cost);
  }

  @Override
  public boolean contains(Candidate candidate) {
    if (this.equals(candidate)) {
      return true;
    }
    for (Candidate child : children) {
      if (child.contains((candidate))) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param timeRange
   * @return
   */
  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, TimeRange parentTimeRange, boolean failOnPartialData)
    throws LensException {
    Map<Candidate, TimeRange> candidateRange = getTimeRangeSplit(timeRange);
    boolean ret = true;
    for (Map.Entry<Candidate, TimeRange> entry : candidateRange.entrySet()) {
      ret &= entry.getKey().evaluateCompleteness(entry.getValue(), parentTimeRange, failOnPartialData);
    }
    return ret;
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    Set<FactPartition> factPartitionSet = new HashSet<>();
    for (Candidate c : children) {
      factPartitionSet.addAll(c.getParticipatingPartitions());
    }
    return factPartitionSet;
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    return children.stream().allMatch(cand-> cand.isExpressionEvaluable(expr));
  }

  @Override
  public boolean isExpressionEvaluable(String expr) {
    return children.stream().allMatch(cand->cand.isExpressionEvaluable(expr));
  }

  @Override
  public boolean isDimAttributeEvaluable(String dim) throws LensException {
    for (Candidate childCandidate : children) {
      if (!childCandidate.isDimAttributeEvaluable(dim)) {
        return false;
      }
    }
    return true;
  }
  public UnionCandidate explode() throws LensException {
    ListIterator<Candidate> i = children.listIterator();
    while(i.hasNext()) {
      i.set(i.next().explode());
    }
    return this;
  }

  @Override
  public String toString() {
    if (this.toStr == null) {
      this.toStr = getToString();
    }
    return this.toStr;
  }

  private String getToString() {
    StringBuilder builder = new StringBuilder(10 * children.size());
    builder.append("UNION[");
    for (Candidate candidate : children) {
      builder.append(candidate.toString());
      builder.append("; ");
    }
    builder.delete(builder.length() - 2, builder.length());
    builder.append("]");
    return builder.toString();
  }

  /**
   * Splits the parent time range for each candidate.
   * The candidates are sorted based on their costs.
   *
   * @param timeRange
   * @return
   */
  private Map<Candidate, TimeRange> splitTimeRangeForChildren(TimeRange timeRange) {
    if (children.stream().map(Candidate::getCost).allMatch(OptionalDouble::isPresent)) {
      children.sort(comparing(x -> x.getCost().getAsDouble()));
    }
    Map<Candidate, TimeRange> childrenTimeRangeMap = new HashMap<>();
    // Sorted list based on the weights.
    Set<TimeRange> ranges = new HashSet<>();
    ranges.add(timeRange);
    for (Candidate c : children) {
      TimeRange.TimeRangeBuilder builder = timeRange.cloneAsBuilder();
      TimeRange tr = resolveTimeRangeForChildren(c, ranges, builder);
      if (tr != null) {
        // If the time range is not null it means this child candidate is valid for this union candidate.
        childrenTimeRangeMap.put(c, tr);
      }
    }
    return childrenTimeRangeMap;
  }
  private Map<Candidate, TimeRange> getTimeRangeSplit(TimeRange range) {
    return splitTimeRangeMap.computeIfAbsent(range, this::splitTimeRangeForChildren);
  }

  /**
   * Resolves the time range for this candidate based on overlap.
   *
   * @param candidate : Candidate for which the time range is to be calculated
   * @param ranges    : Set of time ranges from which one has to be choosen.
   * @param builder   : TimeRange builder created by the common AST.
   * @return Calculated timeRange for the candidate. If it returns null then there is no suitable time range split for
   * this candidate. This is the correct behaviour because an union candidate can have non participating child
   * candidates for the parent time range.
   */
  private TimeRange resolveTimeRangeForChildren(Candidate candidate, Set<TimeRange> ranges,
    TimeRange.TimeRangeBuilder builder) {
    Iterator<TimeRange> it = ranges.iterator();
    Set<TimeRange> newTimeRanges = new HashSet<>();
    TimeRange ret = null;
    while (it.hasNext()) {
      TimeRange range = it.next();
      // Check for out of range
      if (candidate.getStartTime().getTime() >= range.getToDate().getTime() || candidate.getEndTime().getTime() <= range
        .getFromDate().getTime()) {
        continue;
      }
      // This means overlap.
      if (candidate.getStartTime().getTime() <= range.getFromDate().getTime()) {
        // Start time of the new time range will be range.getFromDate()
        builder.fromDate(range.getFromDate());
        if (candidate.getEndTime().getTime() <= range.getToDate().getTime()) {
          // End time is in the middle of the range is equal to c.getEndTime().
          builder.toDate(candidate.getEndTime());
        } else {
          // End time will be range.getToDate()
          builder.toDate(range.getToDate());
        }
      } else {
        builder.fromDate(candidate.getStartTime());
        if (candidate.getEndTime().getTime() <= range.getToDate().getTime()) {
          builder.toDate(candidate.getEndTime());
        } else {
          builder.toDate(range.getToDate());
        }
      }
      // Remove the time range and add more time ranges.
      it.remove();
      ret = builder.build();
      if (ret.getFromDate().getTime() == range.getFromDate().getTime()) {
        checkAndUpdateNewTimeRanges(ret, range, newTimeRanges);
      } else {
        TimeRange.TimeRangeBuilder b1 = ret.cloneAsBuilder()
          .fromDate(range.getFromDate())
          .toDate(ret.getFromDate());
        newTimeRanges.add(b1.build());
        checkAndUpdateNewTimeRanges(ret, range, newTimeRanges);

      }
      break;
    }
    ranges.addAll(newTimeRanges);
    return ret;
  }

  private void checkAndUpdateNewTimeRanges(TimeRange ret, TimeRange range, Set<TimeRange> newTimeRanges) {
    if (ret.getToDate().getTime() < range.getToDate().getTime()) {
      TimeRange.TimeRangeBuilder b2 = ret.cloneAsBuilder();
      b2.fromDate(ret.getToDate());
      b2.toDate(range.getToDate());
      newTimeRanges.add(b2.build());
    }
  }
}
