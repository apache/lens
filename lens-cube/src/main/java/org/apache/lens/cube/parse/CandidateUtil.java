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
 * KIND, either express or implied.  See the License for theJoinCandidate.java
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.parse;

import static java.util.Comparator.naturalOrder;

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lens.cube.metadata.*;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/**
 * Placeholder for Util methods that will be required for {@link Candidate}
 */
public final class CandidateUtil {

  private CandidateUtil() {
    // Added due to checkstyle error getting below :
    // (design) HideUtilityClassConstructor: Utility classes should not have a public or default constructor.
  }

  /**
   * Returns true if the Candidate is valid for all the timeranges based on its start and end times.
   * @param candidate
   * @param timeRanges
   * @return
   */
  static boolean isValidForTimeRanges(Candidate candidate, List<TimeRange> timeRanges) {
    for (TimeRange timeRange : timeRanges) {
      if (!(timeRange.getFromDate().after(candidate.getStartTime())
          && timeRange.getToDate().before(candidate.getEndTime()))) {
        return false;
      }
    }
    return true;
  }

  static boolean isCandidatePartiallyValidForTimeRange(Date candidateStartTime, Date candidateEndTime,
    Date timeRangeStart, Date timeRangeEnd) {
    Date start  = Stream.of(timeRangeStart, candidateStartTime).max(naturalOrder()).orElse(candidateStartTime);
    Date end = Stream.of(timeRangeEnd, candidateEndTime).min(naturalOrder()).orElse(candidateEndTime);
    return end.after(start);
  }


  static boolean isPartiallyValidForTimeRange(Candidate cand, TimeRange timeRange) {
    return isPartiallyValidForTimeRanges(cand, Arrays.asList(timeRange));
  }

  static boolean isPartiallyValidForTimeRanges(Candidate cand, List<TimeRange> timeRanges) {
    return timeRanges.stream().anyMatch(timeRange ->
      isCandidatePartiallyValidForTimeRange(cand.getStartTime(), cand.getEndTime(),
        timeRange.getFromDate(), timeRange.getToDate()));
  }

  /**
   * Copy Query AST from sourceAst to targetAst
   *
   * @param sourceAst
   * @param targetAst
   * @throws LensException
   */
  static void copyASTs(QueryAST sourceAst, QueryAST targetAst) {

    targetAst.setSelectAST(MetastoreUtil.copyAST(sourceAst.getSelectAST()));
    targetAst.setWhereAST(MetastoreUtil.copyAST(sourceAst.getWhereAST()));
    if (sourceAst.getJoinAST() != null) {
      targetAst.setJoinAST(MetastoreUtil.copyAST(sourceAst.getJoinAST()));
    }
    if (sourceAst.getGroupByAST() != null) {
      targetAst.setGroupByAST(MetastoreUtil.copyAST(sourceAst.getGroupByAST()));
    }
    if (sourceAst.getHavingAST() != null) {
      targetAst.setHavingAST(MetastoreUtil.copyAST(sourceAst.getHavingAST()));
    }
    if (sourceAst.getOrderByAST() != null) {
      targetAst.setOrderByAST(MetastoreUtil.copyAST(sourceAst.getOrderByAST()));
    }

    targetAst.setLimitValue(sourceAst.getLimitValue());
    targetAst.setFromString(sourceAst.getFromString());
    targetAst.setWhereString(sourceAst.getWhereString());
  }

  public static Set<StorageCandidate> getStorageCandidates(final Candidate candidate) {
    return getStorageCandidates(new HashSet<Candidate>(1) {{ add(candidate); }});
  }

  /**
   * Returns true is the Candidates cover the entire time range.
   * @param candidates
   * @param startTime
   * @param endTime
   * @return
   */
  public static boolean isTimeRangeCovered(Collection<Candidate> candidates, Date startTime, Date endTime) {
    RangeSet<Date> set = TreeRangeSet.create();
    for (Candidate candidate : candidates) {
      set.add(Range.range(candidate.getStartTime(), BoundType.CLOSED, candidate.getEndTime(), BoundType.OPEN));
    }
    return set.encloses(Range.range(startTime, BoundType.CLOSED, endTime, BoundType.OPEN));
  }

  public static Set<String> getColumns(Collection<QueriedPhraseContext> queriedPhraseContexts) {
    Set<String> cols = new HashSet<>();
    for (QueriedPhraseContext qur : queriedPhraseContexts) {
      cols.addAll(qur.getColumns());
    }
    return cols;
  }

  /**
   * Filters Candidates that contain the filterCandidate
   *
   * @param candidates
   * @param filterCandidate
   * @return pruned Candidates
   */
  public static Collection<Candidate> filterCandidates(Collection<Candidate> candidates, Candidate filterCandidate) {
    List<Candidate> prunedCandidates = new ArrayList<>();
    Iterator<Candidate> itr = candidates.iterator();
    while (itr.hasNext()) {
      if (itr.next().contains(filterCandidate)) {
        prunedCandidates.add(itr.next());
        itr.remove();
      }
    }
    return prunedCandidates;
  }

  /**
   * Gets all the Storage Candidates that participate in the collection of passed candidates
   *
   * @param candidates
   * @return
   */
  public static Set<StorageCandidate> getStorageCandidates(Collection<? extends Candidate> candidates) {
    Set<StorageCandidate> storageCandidateSet = new HashSet<>();
    getStorageCandidates(candidates, storageCandidateSet);
    return storageCandidateSet;
  }

  private static void getStorageCandidates(Collection<? extends Candidate> candidates,
    Collection<StorageCandidate> storageCandidateSet) {
    for (Candidate candidate : candidates) {
      getStorageCandidates(candidate, storageCandidateSet);
    }
  }
  static void getStorageCandidates(Candidate candidate,
    Collection<StorageCandidate> storageCandidateSet) {
    if (candidate.getChildren() == null) {
      // Expecting this to be a StorageCandidate as it has no children.
      if (candidate instanceof StorageCandidate) {
        storageCandidateSet.add((StorageCandidate) candidate);
      } else if (candidate instanceof SegmentationCandidate) {
        SegmentationCandidate segC = (SegmentationCandidate) candidate;
        for (CubeQueryContext cubeQueryContext : segC.cubeQueryContextMap.values()) {
          if (cubeQueryContext.getPickedCandidate() != null) {
            getStorageCandidates(cubeQueryContext.getPickedCandidate(), storageCandidateSet);
          }
        }
      }
    } else {
      getStorageCandidates(candidate.getChildren(), storageCandidateSet);
    }
  }

  public static boolean factHasColumn(CubeFactTable fact, String column) {
    for (FieldSchema factField : fact.getColumns()) {
      if (factField.getName().equals(column)) {
        return true;
      }
    }
    return false;
  }

  public static String getTimeRangeWhereClasue(TimeRangeWriter rangeWriter, StorageCandidate sc, TimeRange range)
    throws LensException {
    String rangeWhere = rangeWriter.getTimeRangeWhereClause(
      sc.getCubeQueryContext(), sc.getCubeQueryContext().getAliasForTableName(sc.getCube().getName()),
      sc.getRangeToPartitions().get(range));
    String fallback = sc.getRangeToExtraWhereFallBack().get(range);
    if (StringUtils.isNotBlank(fallback)){
      rangeWhere =  "((" + rangeWhere + ") and  (" + fallback + "))";
    }
    return rangeWhere;
  }

  private static final String BASE_QUERY_FORMAT = "SELECT %s FROM %s";

  /**
   *
   * @param selectAST Outer query selectAST
   * @param cubeql Cubequery Context
   *
   *  Update the final alias in the outer select expressions
   *  1. Replace queriedAlias with finalAlias if both are not same
   *  2. If queriedAlias is missing add finalAlias as alias
   */
  static void updateFinalAlias(ASTNode selectAST, CubeQueryContext cubeql) {
    for (int i = 0; i < selectAST.getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) selectAST.getChild(i);
      ASTNode aliasNode = HQLParser.findNodeByPath(selectExpr, Identifier);
      String finalAlias = cubeql.getSelectPhrases().get(i).getFinalAlias().replaceAll("`", "");
      if (aliasNode != null) {
        String queryAlias = aliasNode.getText();
        if (!queryAlias.equals(finalAlias)) {
          // replace the alias node
          ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, finalAlias));
          selectAST.getChild(i).replaceChildren(selectExpr.getChildCount() - 1,
              selectExpr.getChildCount() - 1, newAliasNode);
        }
      } else {
        // add column alias
        ASTNode newAliasNode = new ASTNode(new CommonToken(HiveParser.Identifier, finalAlias));
        selectAST.getChild(i).addChild(newAliasNode);
      }
    }
  }
  static Set<String> getColumnsFromCandidates(Collection<? extends Candidate> scSet) {
    return scSet.stream().map(Candidate::getColumns).flatMap(Collection::stream).collect(Collectors.toSet());
  }
}
