package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CandidateCoveringSetsResolver implements ContextRewriter {

  private List<Candidate> finalCandidates = new ArrayList<>();
  public CandidateCoveringSetsResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    List<QueriedPhraseContext> qpcList = cubeql.getQueriedPhrases();
    Set<QueriedPhraseContext> queriedMsrs = new HashSet<>();
    for (QueriedPhraseContext qpc : qpcList) {
      if (qpc.hasMeasures(cubeql)) {
        queriedMsrs.add(qpc);
      }
    }
    // if no measures are queried, add all StorageCandidates individually as single covering sets
    if (queriedMsrs.isEmpty()) {
      finalCandidates.addAll(cubeql.getCandidates());
    }
    List<Candidate> timeRangeCoveringSet = resolveTimeRangeCoveringFactSet(cubeql, queriedMsrs, qpcList);
    List<List<Candidate>> measureCoveringSets = resolveJoinCandidates(timeRangeCoveringSet, queriedMsrs, cubeql);
    updateFinalCandidates(measureCoveringSets, cubeql);
    log.info("Covering candidate sets :{}", finalCandidates);
    cubeql.getCandidates().clear();
    cubeql.getCandidates().addAll(finalCandidates);
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
    for (Iterator<List<Candidate>> itr = joinCandidates.iterator(); itr.hasNext(); ) {
      List<Candidate> joinCandidate = itr.next();
      if (joinCandidate.size() == 1) {
        finalCandidates.add(joinCandidate.iterator().next());
      } else {
        finalCandidates.add(createJoinCandidate(joinCandidate, cubeql));
      }
    }
  }

  private boolean isCandidateCoveringTimeRanges(UnionCandidate uc, List<TimeRange> ranges) {
    for (Iterator<TimeRange> itr = ranges.iterator(); itr.hasNext(); ) {
      TimeRange range = itr.next();
      if (!CandidateUtil.isTimeRangeCovered(uc.getChildren(), range.getFromDate(), range.getToDate())) {
        return false;
      }
    }
    return true;
  }

  private void pruneUnionCandidatesNotCoveringAllRanges(List<UnionCandidate> ucs, List<TimeRange> ranges) {
    for (Iterator<UnionCandidate> itr = ucs.iterator(); itr.hasNext(); ) {
      UnionCandidate uc = itr.next();
      if (!isCandidateCoveringTimeRanges(uc, ranges)) {
        itr.remove();
      }
    }
  }

  private List<Candidate> resolveTimeRangeCoveringFactSet(CubeQueryContext cubeql,
      Set<QueriedPhraseContext> queriedMsrs, List<QueriedPhraseContext> qpcList) throws LensException {
    // All Candidates
    List<Candidate> allCandidates = new ArrayList<Candidate>(cubeql.getCandidates());
    // Partially valid candidates
    List<Candidate> allCandidatesPartiallyValid = new ArrayList<>();
    List<Candidate> candidateSet = new ArrayList<>();
    for (Candidate cand : allCandidates) {
      // Assuming initial list of candidates populated are StorageCandidate
      if (cand instanceof StorageCandidate) {
        StorageCandidate sc = (StorageCandidate) cand;
        if (CandidateUtil.isValidForTimeRanges(sc, cubeql.getTimeRanges())) {
          candidateSet.add(CandidateUtil.cloneStorageCandidate(sc));
          continue;
        } else if (CandidateUtil.isPartiallyValidForTimeRanges(sc, cubeql.getTimeRanges())) {
          allCandidatesPartiallyValid.add(CandidateUtil.cloneStorageCandidate(sc));
        } else {
          //TODO union : Add cause
        }
      } else {
        throw new LensException("Not a StorageCandidate!!");
      }
    }
    // Get all covering fact sets
    List<UnionCandidate> unionCoveringSet =
        getCombinations(new ArrayList<Candidate>(allCandidatesPartiallyValid), cubeql);
    // Sort the Collection based on no of elements
    Collections.sort(unionCoveringSet, new CandidateUtil.ChildrenSizeBasedCandidateComparator<UnionCandidate>());
    // prune non covering sets
    pruneUnionCandidatesNotCoveringAllRanges(unionCoveringSet, cubeql.getTimeRanges());
    // prune candidate set which doesn't contain any common measure i
    pruneUnionCoveringSetWithoutAnyCommonMeasure(unionCoveringSet, queriedMsrs, cubeql);
    // prune redundant covering sets
    pruneRedundantUnionCoveringSets(unionCoveringSet);
    // pruing done in the previous steps, now create union candidates
    candidateSet.addAll(unionCoveringSet);
    updateQueriableMeasures(candidateSet, qpcList, cubeql);
    return candidateSet ;
  }

  private boolean isMeasureAnswerablebyUnionCandidate(QueriedPhraseContext msr, Candidate uc,
      CubeQueryContext cubeql) throws LensException {
    // Candidate is a single StorageCandidate
    if ((uc instanceof StorageCandidate) && !msr.isEvaluable(cubeql, (StorageCandidate) uc)) {
      return false;
    } else if ((uc instanceof UnionCandidate)){
      for (Candidate cand : uc.getChildren()) {
        if (!msr.isEvaluable(cubeql, (StorageCandidate) cand)) {
          return false;
        }
      }
    }
    return true;
  }

  private void pruneUnionCoveringSetWithoutAnyCommonMeasure(List<UnionCandidate> ucs,
      Set<QueriedPhraseContext> queriedMsrs,
      CubeQueryContext cubeql) throws LensException {
    for (ListIterator<UnionCandidate> itr = ucs.listIterator(); itr.hasNext(); ) {
      boolean toRemove = true;
      UnionCandidate uc = itr.next();
      for (QueriedPhraseContext msr : queriedMsrs) {
        if (isMeasureAnswerablebyUnionCandidate(msr, uc, cubeql)) {
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
      for (ListIterator<UnionCandidate> itr = candidates.listIterator(j); itr.hasNext(); ) {
        UnionCandidate next = itr.next();
        if (next.getChildren().containsAll(current.getChildren())) {
          itr.remove();
        }
      }
    }
  }

  public List<UnionCandidate> getCombinations(final List<Candidate> candidates, CubeQueryContext cubeql) {
    int aliasCounter = 0;
    List<UnionCandidate> combinations = new LinkedList<UnionCandidate>();
    int size = candidates.size();
    int threshold = Double.valueOf(Math.pow(2, size)).intValue() - 1;

    for (int i = 1; i <= threshold; ++i) {
      LinkedList<Candidate> individualCombinationList = new LinkedList<Candidate>();
      int count = size - 1;
      int clonedI = i;
      while (count >= 0) {
        if ((clonedI & 1) != 0) {
          individualCombinationList.addFirst(candidates.get(count));
        }
        clonedI = clonedI >>> 1;
        --count;
      }
      combinations.add(new UnionCandidate(individualCombinationList, cubeql ));
    }
    return combinations;
  }

  private List<List<Candidate>> resolveJoinCandidates(List<Candidate> unionCandidates,
      Set<QueriedPhraseContext> msrs, CubeQueryContext cubeql) throws LensException {
    List<List<Candidate>> msrCoveringSets = new ArrayList<>();
    List<Candidate> ucSet = new ArrayList<>(unionCandidates);
    // Check if a single set can answer all the measures and exprsWithMeasures
    for (Iterator<Candidate> i = ucSet.iterator(); i.hasNext(); ) {
      boolean evaluable = false;
      Candidate uc = i.next();
      for (QueriedPhraseContext msr : msrs) {
        evaluable = isMeasureAnswerablebyUnionCandidate(msr, uc, cubeql) ? true : false;
        if (!evaluable) {
          break;
        }
      }
      if (evaluable) {
        // single set can answer all the measures as an UnionCandidate
        List<Candidate> one = new ArrayList<>();
        one.add(uc);
        msrCoveringSets.add(one);
        i.remove();
      }
    }
    // Sets that contain all measures or no measures are removed from iteration.
    // find other facts
    for (Iterator<Candidate> i = ucSet.iterator(); i.hasNext(); ) {
      Candidate uc = i.next();
      i.remove();
      // find the remaining measures in other facts
      if (i.hasNext()) {
        Set<QueriedPhraseContext> remainingMsrs = new HashSet<>(msrs);
        Set<QueriedPhraseContext> coveredMsrs = CandidateUtil.coveredMeasures(uc, msrs, cubeql);
        remainingMsrs.removeAll(coveredMsrs);

        List<List<Candidate>> coveringSets = resolveJoinCandidates(ucSet, remainingMsrs, cubeql);
        if (!coveringSets.isEmpty()) {
          for (List<Candidate> candSet : coveringSets) {
            candSet.add(uc);
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
      List<QueriedPhraseContext> qpcList, CubeQueryContext cubeql) throws LensException {
    for (Candidate cand : cands ) {
      updateStorageCandidateQueriableMeasures(cand, qpcList, cubeql);
    }
  }


  private void updateStorageCandidateQueriableMeasures(Candidate unionCandidate,
      List<QueriedPhraseContext> qpcList, CubeQueryContext cubeql) throws LensException {
    QueriedPhraseContext msrPhrase;
    boolean isEvaluable;
    for (int index = 0; index < qpcList.size(); index++) {

      if (!qpcList.get(index).hasMeasures(cubeql)) {
        //Not a measure phrase. Skip it
        continue;
      }

      msrPhrase = qpcList.get(index);
      if (unionCandidate instanceof StorageCandidate && msrPhrase.isEvaluable(cubeql,
          (StorageCandidate) unionCandidate)) {
        ((StorageCandidate) unionCandidate).setAnswerableMeasurePhraseIndices(index);
      } else if (unionCandidate instanceof UnionCandidate) {
        isEvaluable = true;
        for (Candidate childCandidate : unionCandidate.getChildren()) {
          if (!msrPhrase.isEvaluable(cubeql, (StorageCandidate) childCandidate)) {
            isEvaluable = false;
            break;
          }
        }
        if (isEvaluable) {
          //Set the index for all the children in this case
          for (Candidate childCandidate : unionCandidate.getChildren()) {
            ((StorageCandidate) childCandidate).setAnswerableMeasurePhraseIndices(index);
          }
        }
      }
    }
  }
}