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
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.TimePartitionRange;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.metadata.timeline.RangesPartitionTimeline;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.google.common.collect.Maps;

/**
 * Prune candidate fact sets so that the facts except the ones that are covering maximum of range are pruned
 */
class MaxCoveringFactResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(MaxCoveringFactResolver.class.getName());
  private final boolean failOnPartialData;

  public MaxCoveringFactResolver(Configuration conf) {
    this.failOnPartialData = conf.getBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    if (failOnPartialData) {
      // if fail on partial data is true, by the time this resolver starts,
      // all candidate fact sets are covering full time range. We can avoid
      // redundant computation.
      return;
    }
    if (cubeql.getCube() != null && !cubeql.getCandidateFactSets().isEmpty()) {
      Map<Set<CandidateFact>, Long> factPartCount = Maps.newHashMap();
      for (Set<CandidateFact> facts : cubeql.getCandidateFactSets()) {
        factPartCount.put(facts, getTimeCovered(facts));
      }
      long maxTimeCovered = Collections.max(factPartCount.values());
      TimeCovered timeCovered = new TimeCovered(maxTimeCovered);
      for (Iterator<Set<CandidateFact>> i = cubeql.getCandidateFactSets().iterator(); i.hasNext();) {
        Set<CandidateFact> facts = i.next();
        if (factPartCount.get(facts) < maxTimeCovered) {
          LOG.info("Not considering facts:" + facts + " from candidate fact tables as it covers less time than the max."
            + "which is: " + timeCovered);
          i.remove();
        }
      }
      cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCause.lessData(timeCovered));
    }
  }

  private long getTimeCovered(Set<CandidateFact> facts) {
    UpdatePeriod smallest = UpdatePeriod.values()[UpdatePeriod.values().length - 1];
    for (CandidateFact fact : facts) {
      for (FactPartition part : fact.getPartsQueried()) {
        if (part.getPeriod().compareTo(smallest) < 0) {
          smallest = part.getPeriod();
        }
      }
    }
    RangesPartitionTimeline range = new RangesPartitionTimeline(null, smallest, null);
    for (CandidateFact fact : facts) {
      for (FactPartition part : fact.getPartsQueried()) {
        if (part.isFound()) {
          try {
            TimePartitionRange subrange =
              TimePartition.of(part.getPeriod(), part.getPartSpec()).singletonRange();
            for (TimePartition partition : TimePartition.of(smallest, subrange.getBegin().getDate()).rangeUpto(
              TimePartition.of(smallest, subrange.getEnd().getDate()))) {
              range.add(partition);
            }
          } catch (LensException e) {
            LOG.error("invalid partition: " + e);
          }
        }
      }
    }
    return range.getTimeCovered();
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
