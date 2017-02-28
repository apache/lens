/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.parse;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.metadata.timeline.RangesPartitionTimeline;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

/**
 * Prune candidates except the ones that are covering maximum of range are pruned
 */
@Slf4j
class MaxCoveringFactResolver implements ContextRewriter {
  private final boolean failOnPartialData;

  public MaxCoveringFactResolver(Configuration conf) {
    this.failOnPartialData = conf.getBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) {
    if (failOnPartialData) {
      // if fail on partial data is true, by the time this resolver starts,
      // all candidate fact sets are covering full time range. We can avoid
      // redundant computation.
      return;
    }
    if (cubeql.getCube() == null || cubeql.getCandidates().size() <= 1) {
      // nothing to prune.
      return;
    }
    resolveByTimeCovered(cubeql);
    if (cubeql.getMetastoreClient() != null && cubeql.getMetastoreClient().isDataCompletenessCheckEnabled()) {
      resolveByDataCompleteness(cubeql);
    }
  }

  private void resolveByTimeCovered(CubeQueryContext cubeql) {
    // For each part column, which candidate fact sets are covering how much amount.
    // Later, we'll maximize coverage for each queried part column.
    Map<String, Map<Candidate, Long>> partCountsPerPartCol = Maps.newHashMap();
    for (Candidate cand : cubeql.getCandidates()) {
      for (Map.Entry<String, Long> entry : getTimeCoveredForEachPartCol(cand).entrySet()) {
        if (!partCountsPerPartCol.containsKey(entry.getKey())) {
          partCountsPerPartCol.put(entry.getKey(), Maps.<Candidate, Long>newHashMap());
        }
        partCountsPerPartCol.get(entry.getKey()).put(cand, entry.getValue());
      }
    }
    // for each queried partition, prune fact sets that are covering less range than max
    for (String partColQueried : cubeql.getPartitionColumnsQueried()) {
      if (partCountsPerPartCol.get(partColQueried) != null) {
        long maxTimeCovered = Collections.max(partCountsPerPartCol.get(partColQueried).values());
        TimeCovered timeCovered = new TimeCovered(maxTimeCovered);
        Iterator<Candidate> iter = cubeql.getCandidates().iterator();
        while (iter.hasNext()) {
          Candidate candidate = iter.next();
          Long timeCoveredLong = partCountsPerPartCol.get(partColQueried).get(candidate);
          if (timeCoveredLong == null) {
            timeCoveredLong = 0L;
          }
          if (timeCoveredLong < maxTimeCovered) {
            log.info("Not considering Candidate:{} from Candidate set as it covers less time than the max"
              + " for partition column: {} which is: {}", candidate, partColQueried, timeCovered);
            iter.remove();
            cubeql.addCandidatePruningMsg(candidate,
              new CandidateTablePruneCause(CandidateTablePruneCause.CandidateTablePruneCode.LESS_DATA));
          }
        }
      }
    }
    //cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCause.lessData(null));
  }

  private void resolveByDataCompleteness(CubeQueryContext cubeql) {
    // From the list of  candidate fact sets, we calculate the maxDataCompletenessFactor.
    float maxDataCompletenessFactor = 0f;
    for (Candidate cand : cubeql.getCandidates()) {
      float dataCompletenessFactor = computeDataCompletenessFactor(cand);
      if (dataCompletenessFactor > maxDataCompletenessFactor) {
        maxDataCompletenessFactor = dataCompletenessFactor;
      }
    }

    if (maxDataCompletenessFactor == 0f) {
      //there is nothing to prune
      return;
    }

    // We prune those candidate fact set, whose dataCompletenessFactor is less than maxDataCompletenessFactor
    Iterator<Candidate> iter = cubeql.getCandidates().iterator();
    while (iter.hasNext()) {
      Candidate cand = iter.next();
      float dataCompletenessFactor = computeDataCompletenessFactor(cand);
      if (dataCompletenessFactor < maxDataCompletenessFactor) {
        log.info("Not considering Candidate :{} from the list as the dataCompletenessFactor for this:{} is "
          + "less than the max:{}", cand, dataCompletenessFactor, maxDataCompletenessFactor);
        iter.remove();
        cubeql.addCandidatePruningMsg(cand,
          new CandidateTablePruneCause(CandidateTablePruneCause.CandidateTablePruneCode.INCOMPLETE_PARTITION));
      }
    }
  }

  private float computeDataCompletenessFactor(Candidate cand) {
    float completenessFactor = 0f;
    int numPartition = 0;
    for (StorageCandidate sc : CandidateUtil.getStorageCandidates(cand)) {
      if (sc.getDataCompletenessMap() != null) {
        Map<String, Map<String, Float>> completenessMap = sc.getDataCompletenessMap();
        for (Map<String, Float> partitionCompleteness : completenessMap.values()) {
          for (Float value : partitionCompleteness.values()) {
            numPartition++;
            completenessFactor += value;
          }
        }
      }
    }
    return numPartition == 0 ? completenessFactor : completenessFactor / numPartition;
  }

  /**
   * Returns time covered by fact set for each part column.
   *
   * @param cand
   * @return
   */
  private Map<String, Long> getTimeCoveredForEachPartCol(Candidate cand) {
    Map<String, Long> ret = Maps.newHashMap();
    UpdatePeriod smallest = UpdatePeriod.values()[UpdatePeriod.values().length - 1];
    for (FactPartition part : cand.getParticipatingPartitions()) {
      if (part.getPeriod().compareTo(smallest) < 0) {
        smallest = part.getPeriod();
      }
    }
    PartitionRangesForPartitionColumns partitionRangesForPartitionColumns = new PartitionRangesForPartitionColumns();
    for (FactPartition part : cand.getParticipatingPartitions()) {
      if (part.isFound()) {
        try {
          partitionRangesForPartitionColumns.add(part);
        } catch (LensException e) {
          log.error("invalid partition: ", e);
        }
      }
    }
    for (Map.Entry<String, RangesPartitionTimeline> entry : partitionRangesForPartitionColumns.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().getTimeCovered());
    }
    return ret;
  }

  public static class TimeCovered {
    private final long days;
    private final long hours;
    private final long minutes;
    private final long seconds;
    private final long milliseconds;

    TimeCovered(long ms) {
      milliseconds = ms % (24 * 60 * 60 * 1000);
      long seconds = ms / (24 * 60 * 60 * 1000);
      this.seconds = seconds % (24 * 60 * 60);
      long minutes = seconds / (24 * 60 * 60);
      this.minutes = minutes % (24 * 60);
      long hours = minutes / (24 * 60);
      this.hours = hours % 24;
      this.days = hours / 24;
    }

    public String toString() {
      return String.valueOf(days) + " days, " + hours + " hours, " + minutes +
        " minutes, " + seconds + " seconds, " + milliseconds + " milliseconds.";
    }
  }
}
