package org.apache.lens.cube.parse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;

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
  @Getter
  private String alias;

  public JoinCandidate(Candidate childCandidate1, Candidate childCandidate2, String alias) {
    this.childCandidate1 = childCandidate1;
    this.childCandidate2 = childCandidate2;
    this.alias = alias;
  }

  private String getJoinCondition() {
    return null;
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
    return childCandidate1.getStartTime().after(childCandidate2.getStartTime())
           ? childCandidate1.getStartTime()
           : childCandidate2.getStartTime();
  }

  @Override
  public Date getEndTime() {
    return childCandidate1.getEndTime().before(childCandidate2.getEndTime())
           ? childCandidate1.getEndTime()
           : childCandidate2.getEndTime();
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
  public boolean evaluateCompleteness(TimeRange timeRange, boolean failOnPartialData) throws LensException {
    return this.childCandidate1.evaluateCompleteness(timeRange, failOnPartialData) && this.childCandidate2
      .evaluateCompleteness(timeRange, failOnPartialData);
  }

  @Override
  public Set<FactPartition> getParticipatingPartitions() {
    return null;
  }

  @Override
  public boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr) {
    return childCandidate1.isExpressionEvaluable(expr) || childCandidate1.isExpressionEvaluable(expr);
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