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
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.server.api.error.LensException;

import lombok.extern.slf4j.Slf4j;

/**
 * Prune fact tables having more weight than minimum.
 */
@Slf4j
public class LightestFactResolver implements ContextRewriter {

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    if (cubeql.getCube() != null && !cubeql.getCandidates().isEmpty()) {
      Map<Candidate, Double> factWeightMap = cubeql.getCandidates().stream()
        .filter(candidate -> candidate.getCost().isPresent())
        .collect(Collectors.toMap(Function.identity(), x -> x.getCost().getAsDouble()));
      if (!factWeightMap.isEmpty()) {
        double minWeight = Collections.min(factWeightMap.values());
        for (Iterator<Candidate> i = cubeql.getCandidates().iterator(); i.hasNext();) {
          Candidate cand = i.next();
          if (factWeightMap.containsKey(cand)) {
            if (factWeightMap.get(cand) > minWeight) {
              log.info("Not considering candidate:{} from final candidates as it has more fact weight:{} minimum:{}",
                cand, factWeightMap.get(cand), minWeight);
              cubeql.addCandidatePruningMsg(cand, new CandidateTablePruneCause(CandidateTablePruneCode.MORE_WEIGHT));
              i.remove();
            }
          }
        }
      }
    }
  }
}
