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

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.lib.Node;
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
  static boolean isTimeRangeCovered(Collection<Candidate> candidates, Date startTime, Date endTime) {
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
      Candidate cur = itr.next();
      if (cur.contains(filterCandidate)) {
        prunedCandidates.add(cur);
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

  static void updateOrderByWithFinalAlias(ASTNode orderby, ASTNode select) throws LensException{
    if (orderby == null) {
      return;
    }
    for (Node orderbyNode : orderby.getChildren()) {
      ASTNode orderBychild = (ASTNode) orderbyNode;
      for (Node selectNode : select.getChildren()) {
        ASTNode selectChild = (ASTNode) selectNode;
        if (selectChild.getChildCount() == 2) {
          if (HQLParser.getString((ASTNode) selectChild.getChild(0))
              .equals(HQLParser.getString((ASTNode) orderBychild.getChild(0)))) {
            ASTNode alias = new ASTNode((ASTNode) selectChild.getChild(1));
            if (!alias.toString().matches("\\S+")) {
              throw new LensException(LensCubeErrorCode.ORDERBY_ALIAS_CONTAINING_WHITESPACE.getLensErrorInfo(), alias);
            }
            orderBychild.replaceChildren(0, 0, alias);
            break;
          }
        }
      }
    }
  }
}
