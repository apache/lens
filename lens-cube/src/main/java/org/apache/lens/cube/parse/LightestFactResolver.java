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
 * Prune fact tables having more weight than minimum.
 */
@Slf4j
public class LightestFactResolver implements ContextRewriter {
  public LightestFactResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() != null && !cubeql.getCandidateFactSets().isEmpty()) {
      Map<Set<CandidateFact>, Double> factWeightMap = new HashMap<Set<CandidateFact>, Double>();

      for (Set<CandidateFact> facts : cubeql.getCandidateFactSets()) {
        factWeightMap.put(facts, getWeight(facts));
      }

      double minWeight = Collections.min(factWeightMap.values());

      for (Iterator<Set<CandidateFact>> i = cubeql.getCandidateFactSets().iterator(); i.hasNext();) {
        Set<CandidateFact> facts = i.next();
        if (factWeightMap.get(facts) > minWeight) {
          log.info("Not considering facts:{} from candidate fact tables as it has more fact weight:{} minimum:{}",
            facts, factWeightMap.get(facts), minWeight);
          i.remove();
        }
      }
      cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCode.MORE_WEIGHT);
    }
  }

  private Double getWeight(Set<CandidateFact> set) {
    Double weight = 0.0;
    for (CandidateFact f : set) {
      weight += f.fact.weight();
    }
    return weight;
  }
}
