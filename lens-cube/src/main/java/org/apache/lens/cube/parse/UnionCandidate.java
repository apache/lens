package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;

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
  @Getter
  String alias;
  /**
   * List of child candidates that will be union-ed
   */
  private List<Candidate> childCandidates;

  public UnionCandidate(List<Candidate> childCandidates, String alias) {
    this.childCandidates = childCandidates;
    this.alias = alias;
  }

  @Override
  public String toHQL() {
    return null;
  }

  @Override
  public QueryAST getQueryAst() {
    return null;
  }

  @Override
  public Collection<String> getColumns() {
    return null;
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
  public boolean evaluateCompleteness(TimeRange timeRange, boolean failOnPartialData) throws LensException {
    Map<Candidate, TimeRange> candidateRange = getTimeRangeForChildren(timeRange);
    boolean ret = true;
    for (Map.Entry<Candidate, TimeRange> entry : candidateRange.entrySet()) {
      ret &= entry.getKey().evaluateCompleteness(entry.getValue(), failOnPartialData);
    }
    return ret;
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    return null;
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

  private Map<Candidate, TimeRange> getTimeRangeForChildren(TimeRange timeRange) {
    Collections.sort(childCandidates, new Comparator<Candidate>() {
      @Override
      public int compare(Candidate o1, Candidate o2) {
        return o1.getCost() < o2.getCost() ? -1 : o1.getCost() == o2.getCost() ? 0 : 1;
      }
    });

    Map<Candidate, TimeRange> candidateTimeRangeMap = new HashMap<>();
    // Sorted list based on the weights.
    Set<TimeRange> ranges = new HashSet<>();

    ranges.add(timeRange);
    for (Candidate c : childCandidates) {
      TimeRange.TimeRangeBuilder builder = getClonedBuiler(timeRange);
      TimeRange tr = resolveTimeRange(c, ranges, builder);
      if (tr != null) {
        // If the time range is not null it means this child candidate is valid for this union candidate.
        candidateTimeRangeMap.put(c, tr);
      }
    }
    return candidateTimeRangeMap;
  }

  private TimeRange resolveTimeRange(Candidate c, Set<TimeRange> ranges, TimeRange.TimeRangeBuilder builder) {
    Iterator<TimeRange> it = ranges.iterator();
    Set<TimeRange> newTimeRanges = new HashSet<>();
    TimeRange ret = null;
    while (it.hasNext()) {
      TimeRange range = it.next();
      // Check for out of range
      if (c.getStartTime().getTime() >= range.getToDate().getTime() || c.getEndTime().getTime() <= range.getFromDate()
        .getTime()) {
        continue;
      }
      // This means overlap.
      if (c.getStartTime().getTime() <= range.getFromDate().getTime()) {
        // Start time of the new time range will be range.getFromDate()
        builder.fromDate(range.getFromDate());
        if (c.getEndTime().getTime() <= range.getToDate().getTime()) {
          // End time is in the middle of the range is equal to c.getEndTime().
          builder.toDate(c.getEndTime());
        } else {
          // End time will be range.getToDate()
          builder.toDate(range.getToDate());
        }
      } else {
        builder.fromDate(c.getStartTime());
        if (c.getEndTime().getTime() <= range.getToDate().getTime()) {
          builder.toDate(c.getEndTime());
        } else {
          builder.toDate(range.getToDate());
        }
      }
      // Remove the time range and add more time ranges.
      it.remove();
      ret = builder.build();
      if (ret.getFromDate().getTime() == range.getFromDate().getTime()) {
        if (ret.getToDate().getTime() < range.getToDate().getTime()) {
          // The end time is the start time of the new range.
          TimeRange.TimeRangeBuilder b1 = getClonedBuiler(ret);
          b1.fromDate(ret.getFromDate());
          b1.toDate(range.getToDate());
          newTimeRanges.add(b1.build());
        }
      } else {
        TimeRange.TimeRangeBuilder b1 = getClonedBuiler(ret);
        b1.fromDate(range.getFromDate());
        b1.toDate(ret.getFromDate());
        newTimeRanges.add(b1.build());
        if (ret.getToDate().getTime() < range.getToDate().getTime()) {
          TimeRange.TimeRangeBuilder b2 = getClonedBuiler(ret);
          b2.fromDate(ret.getToDate());
          b2.toDate(range.getToDate());
          newTimeRanges.add(b2.build());
        }
      }
      break;
    }
    ranges.addAll(newTimeRanges);
    return ret;
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