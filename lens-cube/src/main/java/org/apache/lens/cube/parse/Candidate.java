package org.apache.lens.cube.parse;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;

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
   * Returns String representation of this Candidate
   * TODO decide if this method should be moved to QueryAST instead
   *
   * @return
   */
  String toHQL();

  /**
   * Returns Query AST
   *
   * @return
   */
  QueryAST getQueryAst();

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
   * Alias used for this candidate.
   *
   * @return
   */
  String getAlias();

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
   * @param failOnPartialData : fail fast if the candidate can answer the query only partially
   * @return true if this Candidate can answer query for the given time range.
   */
  boolean evaluateCompleteness(TimeRange timeRange, boolean failOnPartialData)
    throws LensException;

  /**
   * Returns the set of fact partitions that will participate in this candidate.
   * Note: This method can be called only after call to
   * {@link #evaluateCompleteness(TimeRange, boolean)}
   *
   * @return
   */
  Set<FactPartition> getParticipatingPartitions();

  /**
   * TODO union: in case of join , one of the candidates should be able to answer the mesaure expression
   * TODO union: In case of union, all the candidates should answer the expression
   * TODO union : add isExpresionEvaluable() to Candidate
   *
   * @param expr
   * @return
   */
  boolean isExpressionEvaluable(ExpressionResolver.ExpressionContext expr);

  // Moved to CandidateUtil boolean isValidForTimeRange(TimeRange timeRange);
  // Moved to CandidateUtil boolean isExpressionAnswerable(ASTNode node, CubeQueryContext context) throws LensException;
  // NO caller Set<String> getTimePartCols(CubeQueryContext query) throws LensException;

  //TODO add methods to update AST in this candidate in this class of in CandidateUtil.
  //void updateFromString(CubeQueryContext query) throws LensException;

  //void updateASTs(CubeQueryContext cubeql) throws LensException;

  //void addToHaving(ASTNode ast)  throws LensException;

  //Used Having push down flow
  //String addAndGetAliasFromSelect(ASTNode ast, AliasDecider aliasDecider);

}