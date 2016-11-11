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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.parse;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.metadata.timeline.RangesPartitionTimeline;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

/**
 * Prune candidate fact sets so that the facts except the ones that are covering maximum of range are pruned
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
    if (cubeql.getCube() == null || cubeql.getCandidateFactSets().size() <= 1) {
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
    Map<String, Map<Set<CandidateFact>, Long>> partCountsPerPartCol = Maps.newHashMap();
    for (Set<CandidateFact> facts : cubeql.getCandidateFactSets()) {
      for (Map.Entry<String, Long> entry : getTimeCoveredForEachPartCol(facts).entrySet()) {
        if (!partCountsPerPartCol.containsKey(entry.getKey())) {
          partCountsPerPartCol.put(entry.getKey(), Maps.<Set<CandidateFact>, Long>newHashMap());
        }
        partCountsPerPartCol.get(entry.getKey()).put(facts, entry.getValue());
      }
    }
    // for each queried partition, prune fact sets that are covering less range than max
    for (String partColQueried : cubeql.getPartitionColumnsQueried()) {
      if (partCountsPerPartCol.get(partColQueried) != null) {
        long maxTimeCovered = Collections.max(partCountsPerPartCol.get(partColQueried).values());
        TimeCovered timeCovered = new TimeCovered(maxTimeCovered);
        Iterator<Set<CandidateFact>> iter = cubeql.getCandidateFactSets().iterator();
        while (iter.hasNext()) {
          Set<CandidateFact> facts = iter.next();
          Long timeCoveredLong = partCountsPerPartCol.get(partColQueried).get(facts);
          if (timeCoveredLong == null) {
            timeCoveredLong = 0L;
          }
          if (timeCoveredLong < maxTimeCovered) {
            log.info("Not considering facts:{} from candidate fact tables as it covers less time than the max"
                    + " for partition column: {} which is: {}", facts, partColQueried, timeCovered);
            iter.remove();
          }
        }
      }
    }
    cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCause.lessData(null));
  }

  private void resolveByDataCompleteness(CubeQueryContext cubeql) {
    // From the list of  candidate fact sets, we calculate the maxDataCompletenessFactor.
    float maxDataCompletenessFactor = 0f;
    for (Set<CandidateFact> facts : cubeql.getCandidateFactSets()) {
      float dataCompletenessFactor = computeDataCompletenessFactor(facts);
      if (dataCompletenessFactor > maxDataCompletenessFactor) {
        maxDataCompletenessFactor = dataCompletenessFactor;
      }
    }

    if (maxDataCompletenessFactor == 0f) {
      //there is nothing to prune
      return;
    }

    // We prune those candidate fact set, whose dataCompletenessFactor is less than maxDataCompletenessFactor
    Iterator<Set<CandidateFact>> iter = cubeql.getCandidateFactSets().iterator();
    while (iter.hasNext()) {
      Set<CandidateFact> facts = iter.next();
      float dataCompletenessFactor = computeDataCompletenessFactor(facts);
      if (dataCompletenessFactor < maxDataCompletenessFactor) {
        log.info("Not considering facts:{} from candidate fact tables as the dataCompletenessFactor for this:{} is "
                + "less than the max:{}", facts, dataCompletenessFactor, maxDataCompletenessFactor);
        iter.remove();
      }
    }
    cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCause.incompletePartitions(null));
  }

  private float computeDataCompletenessFactor(Set<CandidateFact> facts) {
    float completenessFactor = 0f;
    int numPartition = 0;
    for (CandidateFact fact : facts) {
      if (fact.getDataCompletenessMap() != null) {
        Map<String, Map<String, Float>> completenessMap = fact.getDataCompletenessMap();
        for (Map<String, Float> partitionCompleteness : completenessMap.values()) {
          for (Float value : partitionCompleteness.values()) {
            numPartition++;
            completenessFactor += value;
          }
        }
      }
    }
    return numPartition == 0 ? completenessFactor : completenessFactor/numPartition;
  }

  /**
   * Returns time covered by fact set for each part column.
   * @param facts
   * @return
   */
  private Map<String, Long> getTimeCoveredForEachPartCol(Set<CandidateFact> facts) {
    Map<String, Long> ret = Maps.newHashMap();
    UpdatePeriod smallest = UpdatePeriod.values()[UpdatePeriod.values().length - 1];
    for (CandidateFact fact : facts) {
      for (FactPartition part : fact.getPartsQueried()) {
        if (part.getPeriod().compareTo(smallest) < 0) {
          smallest = part.getPeriod();
        }
      }
    }
    PartitionRangesForPartitionColumns partitionRangesForPartitionColumns = new PartitionRangesForPartitionColumns();
    for (CandidateFact fact : facts) {
      for (FactPartition part : fact.getPartsQueried()) {
        if (part.isFound()) {
          try {
            partitionRangesForPartitionColumns.add(part);
          } catch (LensException e) {
            log.error("invalid partition: ", e);
          }
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

    public TimeCovered(long ms) {
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
      return new StringBuilder()
        .append(days)
        .append(" days, ")
        .append(hours)
        .append(" hours, ")
        .append(minutes)
        .append(" minutes, ")
        .append(seconds)
        .append(" seconds, ")
        .append(milliseconds)
        .append(" milliseconds.").toString();
    }
  }
}
