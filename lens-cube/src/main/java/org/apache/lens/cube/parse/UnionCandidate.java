package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

/**
 * Represents a union of two candidates
 */
public class UnionCandidate implements Candidate {

  /**
   * Caching start and end time calculated for this candidate as it may have many child candidates.
   */
  Date startTime = null;
  Date endTime = null;
  String toStr;
  CubeQueryContext cubeql;
  /**
   * List of child candidates that will be union-ed
   */
  private List<Candidate> childCandidates;
  private QueryAST queryAst;

  public UnionCandidate(List<Candidate> childCandidates, CubeQueryContext cubeql) {
    this.childCandidates = childCandidates;
    //this.alias = alias;
    this.cubeql = cubeql;
  }

  @Override
  public Set<Integer> getAnswerableMeasurePhraseIndices() {
    // All children in the UnionCandiate will be having common quriable measure
    return  getChildren().iterator().next().getAnswerableMeasurePhraseIndices();
  }

  @Override
  public Collection<String> getColumns() {
    // In UnionCandidate all columns are same, return the columns
    // of first child
    return childCandidates.iterator().next().getColumns();
  }

  @Override
  public Date getStartTime() {
    //Note: concurrent calls not handled specifically (This should not be a problem even if we do
    //get concurrent calls).

    if (startTime == null) {
      Date minStartTime = childCandidates.get(0).getStartTime();
      for (Candidate child : childCandidates) {
        if (child.getStartTime().before(minStartTime)) {
          minStartTime = child.getStartTime();
        }
      }
      startTime = minStartTime;
    }
    return startTime;
  }

  @Override
  public Date getEndTime() {
    if (endTime == null) {
      Date maxEndTime = childCandidates.get(0).getEndTime();
      for (Candidate child : childCandidates) {
        if (child.getEndTime().after(maxEndTime)) {
          maxEndTime = child.getEndTime();
        }
      }
      endTime = maxEndTime;
    }
    return endTime;
  }

  @Override
  public double getCost() {
    double cost = 0.0;
    for (Candidate cand : childCandidates) {
      cost += cand.getCost();
    }
    return cost;
  }

  @Override
  public boolean contains(Candidate candidate) {
    if (this.equals(candidate)) {
      return true;
    }

    for (Candidate child : childCandidates) {
      if (child.contains((candidate)))
        return true;
    }
    return false;
  }

  @Override
  public Collection<Candidate> getChildren() {
    return childCandidates;
  }

  /**
   * @param timeRange
   * @return
   */
  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, TimeRange parentTimeRange, boolean failOnPartialData)
    throws LensException {
    Map<Candidate, TimeRange> candidateRange = splitTimeRangeForChildren(timeRange);
    boolean ret = true;
    for (Map.Entry<Candidate, TimeRange> entry : candidateRange.entrySet()) {
      ret &= entry.getKey().evaluateCompleteness(entry.getValue(), parentTimeRange, failOnPartialData);
    }
    return ret;
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    Set<FactPartition> factPartitionSet = new HashSet<>();
    for (Candidate c : childCandidates) {
      factPartitionSet.addAll(c.getParticipatingPartitions());
    }
    return factPartitionSet;
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    for (Candidate cand : childCandidates) {
      if (!cand.isExpressionEvaluable(expr)) {
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

  private String getToString() {
    StringBuilder builder = new StringBuilder(10 * childCandidates.size());
    builder.append("UNION[");
    for (Candidate candidate : childCandidates) {
      builder.append(candidate.toString());
      builder.append(", ");
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
    Collections.sort(childCandidates, new Comparator<Candidate>() {
      @Override
      public int compare(Candidate o1, Candidate o2) {
        return o1.getCost() < o2.getCost() ? -1 : o1.getCost() == o2.getCost() ? 0 : 1;
      }
    });
    Map<Candidate, TimeRange> childrenTimeRangeMap = new HashMap<>();
    // Sorted list based on the weights.
    Set<TimeRange> ranges = new HashSet<>();
    ranges.add(timeRange);
    for (Candidate c : childCandidates) {
      TimeRange.TimeRangeBuilder builder = getClonedBuiler(timeRange);
      TimeRange tr = resolveTimeRangeForChildren(c, ranges, builder);
      if (tr != null) {
        // If the time range is not null it means this child candidate is valid for this union candidate.
        childrenTimeRangeMap.put(c, tr);
      }
    }
    return childrenTimeRangeMap;
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
        TimeRange.TimeRangeBuilder b1 = getClonedBuiler(ret);
        b1.fromDate(range.getFromDate());
        b1.toDate(ret.getFromDate());
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
      TimeRange.TimeRangeBuilder b2 = getClonedBuiler(ret);
      b2.fromDate(ret.getToDate());
      b2.toDate(range.getToDate());
      newTimeRanges.add(b2.build());
    }
  }

  private TimeRange.TimeRangeBuilder getClonedBuiler(TimeRange timeRange) {
    TimeRange.TimeRangeBuilder builder = new TimeRange.TimeRangeBuilder();
    builder.astNode(timeRange.getAstNode());
    builder.childIndex(timeRange.getChildIndex());
    builder.parent(timeRange.getParent());
    builder.partitionColumn(timeRange.getPartitionColumn());
    return builder;
  }
}