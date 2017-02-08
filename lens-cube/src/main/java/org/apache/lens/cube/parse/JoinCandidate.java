package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

/**
 * Represents a join of two candidates
 */
public class JoinCandidate implements Candidate {

  /**
   * Child candidates that will participate in the join
   */
  private Candidate childCandidate1;
  private Candidate childCandidate2;
  private String toStr;
  private QueryAST queryAST;
  private CubeQueryContext cubeql;

  public JoinCandidate(Candidate childCandidate1, Candidate childCandidate2, CubeQueryContext cubeql) {
    this.childCandidate1 = childCandidate1;
    this.childCandidate2 = childCandidate2;
    this.cubeql = cubeql;
  }

  @Override
  public Collection<String> getColumns() {
    Set<String> columns = new HashSet<>();
    columns.addAll(childCandidate1.getColumns());
    columns.addAll(childCandidate2.getColumns());
    return columns;
  }

  @Override
  public Date getStartTime() {
    return childCandidate1.getStartTime().after(childCandidate2.getStartTime())
        ? childCandidate1.getStartTime() : childCandidate2.getStartTime();
  }

  @Override
  public Date getEndTime() {
    return childCandidate1.getEndTime().before(childCandidate2.getEndTime())
        ? childCandidate1.getEndTime() : childCandidate2.getEndTime();
  }

  @Override
  public double getCost() {
    return childCandidate1.getCost() + childCandidate2.getCost();
  }

  @Override
  public boolean contains(Candidate candidate) {
    if (this.equals(candidate)) {
      return true;
    } else
      return childCandidate1.contains(candidate) || childCandidate2.contains(candidate);
  }

  @Override
  public Collection<Candidate> getChildren() {
    return new ArrayList() {{
      add(childCandidate1);
      add(childCandidate2);
    }};
  }

  /**
   * @param timeRange
   * @return
   */
  @Override
  public boolean evaluateCompleteness(TimeRange timeRange, TimeRange parentTimeRange, boolean failOnPartialData)
      throws LensException {
    return this.childCandidate1.evaluateCompleteness(timeRange, parentTimeRange, failOnPartialData)
        && this.childCandidate2.evaluateCompleteness(timeRange, parentTimeRange, failOnPartialData);
  }

  /**
   * @return all the partitions from the children
   */
  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    Set<FactPartition> factPartitionsSet = new HashSet<>();
    factPartitionsSet.addAll(childCandidate1.getParticipatingPartitions());
    factPartitionsSet.addAll(childCandidate2.getParticipatingPartitions());
    return factPartitionsSet;
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    return childCandidate1.isExpressionEvaluable(expr) || childCandidate2.isExpressionEvaluable(expr);
  }

  @Override
  public Set<Integer> getAnswerableMeasurePhraseIndices() {
    Set<Integer> mesureIndices = new HashSet<>();
    for (Candidate cand : getChildren()) {
      mesureIndices.addAll(cand.getAnswerableMeasurePhraseIndices());
    }
    return mesureIndices;
  }

  @Override
  public String toString() {
    if (this.toStr == null) {
      this.toStr = getToString();
    }
    return this.toStr;
  }

  private String getToString() {
    return this.toStr = "JOIN[" + childCandidate1.toString() + ", " + childCandidate2.toString() + "]";
  }
}