/*
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lens.server.api.error.LensException;

import lombok.extern.slf4j.Slf4j;

/**
 * Prune candidates which require more partitions than minimum parts.
 */
@Slf4j
class LeastPartitionResolver implements ContextRewriter {

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() != null && !cubeql.getCandidates().isEmpty()) {
      Map<Candidate, Integer> factPartCount = new HashMap<>();

      //The number of partitions being calculated is not the actual number of partitions,
      // they are number of time values now instead of partitions.
      // This seems fine, as the less number of time values actually represent the rollups on time. And with
      // MaxCoveringFactResolver candidates with less partitions which are not covering the range would be removed.
      for (Candidate candidate : cubeql.getCandidates()) {
        int parts = candidate.getParticipatingPartitions().size();
        if (parts > 0) {
          factPartCount.put(candidate, parts);
        }
      }
      if (!factPartCount.isEmpty()) {
        double minPartitions = Collections.min(factPartCount.values());

        for (Iterator<Candidate> i = cubeql.getCandidates().iterator(); i.hasNext();) {
          Candidate candidate = i.next();
          if (factPartCount.containsKey(candidate) && factPartCount.get(candidate) > minPartitions) {
            log.info("Not considering Candidate:{} as it requires more partitions to be" + " queried:{} minimum:{}",
              candidate, factPartCount.get(candidate), minPartitions);
            i.remove();
            cubeql.addCandidatePruningMsg(candidate,
              new CandidateTablePruneCause(CandidateTablePruneCause.CandidateTablePruneCode.MORE_PARTITIONS));
          }
        }
      }
    }
  }
}
