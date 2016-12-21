package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/**
 * Placeholder for Util methods that will be required for {@link Candidate}
 */
public class CandidateUtil {

  /**
   * Is calculated measure expression answerable by the Candidate
   * @param exprNode
   * @param candidate
   * @param context
   * @return
   * @throws LensException
   */
  public static boolean isMeasureExpressionAnswerable(ASTNode exprNode, Candidate candidate, CubeQueryContext context)
    throws LensException {
    return candidate.getColumns().containsAll(HQLParser.getColsInExpr(
      context.getAliasForTableName(context.getCube()), exprNode));
  }

  /**
   * Returns true if the Candidate is valid for all the timeranges based on its start and end times.
   * @param candidate
   * @param timeRanges
   * @return
   */
  public static boolean isValidForTimeRanges(Candidate candidate, List<TimeRange> timeRanges) {
    for (TimeRange timeRange : timeRanges) {
      if (!(timeRange.getFromDate().after(candidate.getStartTime())
          && timeRange.getToDate().before(candidate.getEndTime()))) {
        return false;
      }
    }
    return true;
  }

  public static boolean isPartiallyValidForTimeRanges(Candidate cand, List<TimeRange> timeRanges) {
    for (TimeRange timeRange : timeRanges) {
      if ((cand.getStartTime().before(timeRange.getFromDate()) && cand.getEndTime().after(timeRange.getFromDate()))
          || (cand.getStartTime().before(timeRange.getToDate()) && cand.getEndTime().after(timeRange.getToDate()))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the time partition columns for a storage candidate
   * TODO decide is this needs to be supported for all Candidate types.
   *
   * @param candidate : Stoarge Candidate
   * @param metastoreClient : Cube metastore client
   * @return
   * @throws LensException
   */
  public Set<String> getTimePartitionCols(StorageCandidate candidate, CubeMetastoreClient metastoreClient)
    throws LensException {
    Set<String> cubeTimeDimensions = candidate.getCube().getTimedDimensions();
    Set<String> timePartDimensions = new HashSet<String>();
    String singleStorageTable = candidate.getStorageName();
    List<FieldSchema> partitionKeys = null;
    partitionKeys = metastoreClient.getTable(singleStorageTable).getPartitionKeys();
    for (FieldSchema fs : partitionKeys) {
      if (cubeTimeDimensions.contains(CubeQueryContext.getTimeDimOfPartitionColumn(candidate.getCube(),
        fs.getName()))) {
        timePartDimensions.add(fs.getName());
      }
    }
    return timePartDimensions;
  }

  /**
   * Copy Query AST from sourceAst to targetAst
   *
   * @param sourceAst
   * @param targetAst
   * @throws LensException
   */
  public void copyASTs(QueryAST sourceAst, QueryAST targetAst) throws LensException {
    targetAst.setSelectAST(MetastoreUtil.copyAST(sourceAst.getSelectAST()));
    targetAst.setWhereAST(MetastoreUtil.copyAST(sourceAst.getWhereAST()));
    if (sourceAst.getJoinAST() != null) {
      targetAst.setJoinAST(MetastoreUtil.copyAST(sourceAst.getJoinAST()));
    }
    if (sourceAst.getGroupByAST() != null) {
      targetAst.setGroupByAST(MetastoreUtil.copyAST(sourceAst.getGroupByAST()));
    }
  }

  public static Set<StorageCandidate> getStorageCandidates(final Candidate candidate) {
    return getStorageCandidates(new HashSet<Candidate>(1) {{
      add(candidate);
    }});
  }


  public static Set<QueriedPhraseContext> coveredMeasures(Candidate candSet, Collection<QueriedPhraseContext> msrs,
    CubeQueryContext cubeql) throws LensException {
    Set<QueriedPhraseContext> coveringSet = new HashSet<>();
    for (QueriedPhraseContext msr : msrs) {
      if (candSet.getChildren() == null) {
        if (msr.isEvaluable(cubeql, (StorageCandidate) candSet)) {
          coveringSet.add(msr);
        }
      } else {
          for (Candidate cand : candSet.getChildren()) {
            if (msr.isEvaluable(cubeql, (StorageCandidate) cand)) {
              coveringSet.add(msr);
            }
          }
        }
    }
    return coveringSet;
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
   * @param candidates
   * @return
   */
  public static Set<StorageCandidate> getStorageCandidates(Collection<Candidate> candidates) {
    Set<StorageCandidate> storageCandidateSet = new HashSet<>();
    getStorageCandidates(candidates, storageCandidateSet);
    return storageCandidateSet;
  }

  private static void getStorageCandidates(Collection<Candidate> candidates,
    Set<StorageCandidate> storageCandidateSet) {
    for (Candidate candidate : candidates) {
      if (candidate.getChildren() == null) {
        //Expecting this to be a StorageCandidate as it has no children.
        storageCandidateSet.add((StorageCandidate)candidate);
      } else {
        getStorageCandidates(candidate.getChildren(), storageCandidateSet);
      }
    }
  }

  public static StorageCandidate cloneStorageCandidate(StorageCandidate sc) {
    return new StorageCandidate(sc.getCube(), sc.getFact(), sc.getStorageName(), sc.getAlias(), sc.getCubeql());
  }

  public static class UnionCandidateComparator<T> implements Comparator<UnionCandidate> {

    @Override
    public int compare(UnionCandidate o1, UnionCandidate o2) {
      return Integer.valueOf(o1.getChildren().size() - o2.getChildren().size());
    }
  }
}
