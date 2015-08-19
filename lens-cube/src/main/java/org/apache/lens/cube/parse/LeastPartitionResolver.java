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

import java.util.*;

import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * Prune candidate fact sets which require more partitions than minimum parts.
 */
@Slf4j
class LeastPartitionResolver implements ContextRewriter {
  public LeastPartitionResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() != null && !cubeql.getCandidateFactSets().isEmpty()) {
      Map<Set<CandidateFact>, Integer> factPartCount = new HashMap<Set<CandidateFact>, Integer>();

      //The number of partitions being calculated is not the actual number of partitions,
      // they are number of time values now instead of partitions.
      // This seems fine, as the less number of time values actually represent the rollups on time. And with
      // MaxCoveringFactResolver facts with less partitions which are not covering the range would be removed.
      for (Set<CandidateFact> facts : cubeql.getCandidateFactSets()) {
        factPartCount.put(facts, getPartCount(facts));
      }

      double minPartitions = Collections.min(factPartCount.values());

      for (Iterator<Set<CandidateFact>> i = cubeql.getCandidateFactSets().iterator(); i.hasNext();) {
        Set<CandidateFact> facts = i.next();
        if (factPartCount.get(facts) > minPartitions) {
          log.info("Not considering facts:{} from candidate fact tables as it requires more partitions to be"
            + " queried:{} minimum:{}", facts, factPartCount.get(facts), minPartitions);
          i.remove();
        }
      }
      cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCode.MORE_PARTITIONS);
    }
  }

  private int getPartCount(Set<CandidateFact> set) {
    int parts = 0;
    for (CandidateFact f : set) {
      parts += f.getNumQueriedParts();
    }
    return parts;
  }

}
