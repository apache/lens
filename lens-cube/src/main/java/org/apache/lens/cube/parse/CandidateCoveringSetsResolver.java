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

import static org.apache.lens.cube.parse.CandidateUtil.getColumns;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CandidateCoveringSetsResolver implements ContextRewriter {

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {

    if (!cubeql.hasCubeInQuery()) {
      return; //Dimension query
    }

    if (cubeql.getCandidates().size() == 0){
      cubeql.throwNoCandidateFactException();
    }

    List<QueriedPhraseContext> qpcList = cubeql.getQueriedPhrases();
    Set<QueriedPhraseContext> queriedMsrs = new HashSet<>();
    for (QueriedPhraseContext qpc : qpcList) {
      if (qpc.hasMeasures(cubeql)) {
        queriedMsrs.add(qpc);
      }
    }

    List<Candidate> timeRangeCoveringSet = resolveTimeRangeCoveringFactSet(cubeql, queriedMsrs, qpcList);
    if (timeRangeCoveringSet.isEmpty()) {
      throw new LensException(LensCubeErrorCode.NO_UNION_CANDIDATE_AVAILABLE.getLensErrorInfo(),
        cubeql.getCube().getName(), cubeql.getTimeRanges().toString(), getColumns(queriedMsrs).toString());
    }
    log.info("Time covering candidates :{}", timeRangeCoveringSet);

    if (queriedMsrs.isEmpty()) {
      cubeql.getCandidates().clear();
      cubeql.getCandidates().addAll(timeRangeCoveringSet);
    } else if (!timeRangeCoveringSet.isEmpty()) {
      List<List<Candidate>> measureCoveringSets = resolveJoinCandidates(timeRangeCoveringSet, queriedMsrs);
      if (measureCoveringSets.isEmpty()) {
        throw new LensException(LensCubeErrorCode.NO_JOIN_CANDIDATE_AVAILABLE.getLensErrorInfo(),
          cubeql.getCube().getName(), getColumns(queriedMsrs).toString());
      }
      updateFinalCandidates(measureCoveringSets, cubeql);
    }

    log.info("Final Time and Measure covering candidates :{}", cubeql.getCandidates());
  }

  private Candidate createJoinCandidate(List<Candidate> childCandidates, CubeQueryContext cubeql) {
    Candidate cand;
    Candidate first = childCandidates.get(0);
    Candidate second = childCandidates.get(1);
    cand = new JoinCandidate(first, second, cubeql);
    for (int i = 2; i < childCandidates.size(); i++) {
      cand = new JoinCandidate(cand, childCandidates.get(i), cubeql);
    }
    return cand;
  }

  private void updateFinalCandidates(List<List<Candidate>> joinCandidates, CubeQueryContext cubeql) {
    List<Candidate> finalCandidates = new ArrayList<>();

    for (List<Candidate> joinCandidate : joinCandidates) {
      if (joinCandidate.size() == 1) {
        finalCandidates.add(joinCandidate.iterator().next());
      } else {
        finalCandidates.add(createJoinCandidate(joinCandidate, cubeql));
      }
    }
    cubeql.getCandidates().clear();
    cubeql.getCandidates().addAll(finalCandidates);
  }

  private boolean isCandidateCoveringTimeRanges(UnionCandidate uc, List<TimeRange> ranges) {
    for (TimeRange range : ranges) {
      if (!CandidateUtil.isTimeRangeCovered(uc.getChildren(), range.getFromDate(), range.getToDate())) {
        return false;
      }
    }
    return true;
  }

  private List<Candidate> resolveTimeRangeCoveringFactSet(CubeQueryContext cubeql,
      Set<QueriedPhraseContext> queriedMsrs, List<QueriedPhraseContext> qpcList) throws LensException {
    List<Candidate> candidateSet = new ArrayList<>();
    // All Candidates
    List<Candidate> allCandidates = new ArrayList<>(cubeql.getCandidates());
    // Partially valid candidates
    List<Candidate> allCandidatesPartiallyValid = new ArrayList<>();
    for (Candidate cand : allCandidates) {
      if (cand.isCompletelyValidForTimeRanges(cubeql.getTimeRanges())) {
        candidateSet.add(cand.copy());
      } else if (cand.isPartiallyValidForTimeRanges(cubeql.getTimeRanges())) {
        allCandidatesPartiallyValid.add(cand.copy());
      } else {
        cubeql.addCandidatePruningMsg(cand, CandidateTablePruneCause.storageNotAvailableInRange(
          cubeql.getTimeRanges()));
      }
    }
    // Get all covering fact sets
    List<UnionCandidate> unionCoveringSet =
        getCombinations(new ArrayList<>(allCandidatesPartiallyValid), cubeql);
    // Sort the Collection based on no of elements
    unionCoveringSet.sort(Comparator.comparing(Candidate::getChildrenCount));
    // prune candidate set which doesn't contain any common measure i
    if (!queriedMsrs.isEmpty()) {
      pruneUnionCoveringSetWithoutAnyCommonMeasure(unionCoveringSet, queriedMsrs);
    }
    // prune redundant covering sets
    pruneRedundantUnionCoveringSets(unionCoveringSet);
    // pruing done in the previous steps, now create union candidates
    candidateSet.addAll(unionCoveringSet);
    updateQueriableMeasures(candidateSet, qpcList);
    return candidateSet;
  }
  private void pruneUnionCoveringSetWithoutAnyCommonMeasure(List<UnionCandidate> ucs,
    Set<QueriedPhraseContext> queriedMsrs) throws LensException {
    for (ListIterator<UnionCandidate> itr = ucs.listIterator(); itr.hasNext();) {
      boolean toRemove = true;
      UnionCandidate uc = itr.next();
      for (QueriedPhraseContext msr : queriedMsrs) {
        if (uc.isPhraseAnswerable(msr)) {
          toRemove = false;
          break;
        }
      }
      if (toRemove) {
        itr.remove();
      }
    }
  }

  private void pruneRedundantUnionCoveringSets(List<UnionCandidate> candidates) {
    for (int i = 0; i < candidates.size(); i++) {
      UnionCandidate current = candidates.get(i);
      int j = i + 1;
      for (ListIterator<UnionCandidate> itr = candidates.listIterator(j); itr.hasNext();) {
        UnionCandidate next = itr.next();
        if (next.getChildren().containsAll(current.getChildren())) {
          itr.remove();
        }
      }
    }
  }

  private List<UnionCandidate> getCombinations(final List<Candidate> candidates, CubeQueryContext cubeql) {
    List<UnionCandidate> combinations = new LinkedList<>();
    int size = candidates.size();
    int threshold = Double.valueOf(Math.pow(2, size)).intValue() - 1;

    for (int i = 1; i <= threshold; ++i) {
      LinkedList<Candidate> individualCombinationList = new LinkedList<>();
      int count = size - 1;
      int clonedI = i;
      while (count >= 0) {
        if ((clonedI & 1) != 0) {
          individualCombinationList.addFirst(candidates.get(count));
        }
        clonedI = clonedI >>> 1;
        --count;
      }
      UnionCandidate uc = new UnionCandidate(individualCombinationList, cubeql);
      if (isCandidateCoveringTimeRanges(uc, cubeql.getTimeRanges())) {
        combinations.add(uc);
      }
    }
    return combinations;
  }

  private List<List<Candidate>> resolveJoinCandidates(List<Candidate> candidates,
    Set<QueriedPhraseContext> msrs) throws LensException {
    List<List<Candidate>> msrCoveringSets = new ArrayList<>();
    List<Candidate> ucSet = new ArrayList<>(candidates);
    // Check if a single set can answer all the measures and exprsWithMeasures
    for (Iterator<Candidate> i = ucSet.iterator(); i.hasNext();) {
      boolean allEvaluable = true;
      boolean anyEvaluable = false;
      Candidate uc = i.next();
      for (QueriedPhraseContext msr : msrs) {
        boolean evaluable = uc.isPhraseAnswerable(msr);
        allEvaluable &= evaluable;
        anyEvaluable |= evaluable;
      }
      if (allEvaluable) {
        // single set can answer all the measures as an UnionCandidate
        List<Candidate> one = new ArrayList<>();
        one.add(uc);
        msrCoveringSets.add(one);
        i.remove();
      }
      if (!anyEvaluable) { // none evaluable
        i.remove();
      }
    }
    // Sets that contain all measures or no measures are removed from iteration.
    // find other facts
    for (Iterator<Candidate> i = ucSet.iterator(); i.hasNext();) {
      Candidate candidate = i.next();
      i.remove();
      // find the remaining measures in other facts
      if (i.hasNext()) {
        Set<QueriedPhraseContext> remainingMsrs = new HashSet<>(msrs);
        Set<QueriedPhraseContext> coveredMsrs = candidate.coveredPhrases(msrs);
        remainingMsrs.removeAll(coveredMsrs);

        List<List<Candidate>> coveringSets = resolveJoinCandidates(ucSet, remainingMsrs);
        if (!coveringSets.isEmpty()) {
          for (List<Candidate> candSet : coveringSets) {
            candSet.add(candidate);
            msrCoveringSets.add(candSet);
          }
        } else {
          log.info("Couldnt find any set containing remaining measures:{} {} in {}", remainingMsrs,
              ucSet);
        }
      }
    }
    log.info("Covering set {} for measures {} with factsPassed {}", msrCoveringSets, msrs, ucSet);
    return msrCoveringSets;
  }

  private void updateQueriableMeasures(List<Candidate> cands,
    List<QueriedPhraseContext> qpcList) throws LensException {
    for (Candidate cand : cands) {
      cand.updateStorageCandidateQueriablePhraseIndices(qpcList);
    }
  }
}
