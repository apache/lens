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

import static org.apache.lens.cube.parse.CandidateTablePruneCause.segmentationPruned;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Created on 11/04/17.
 */
@RequiredArgsConstructor
@Slf4j
public class SegmentationInnerRewriter implements ContextRewriter {
  private final Configuration conf;
  private final HiveConf hconf;

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    Exploder exploder = new Exploder(cubeql);
    cubeql.getCandidates().removeIf(exploder::shouldBeRemoved);
  }
  @RequiredArgsConstructor
  private class Exploder {
    private final CubeQueryContext cubeql;
    private boolean shouldBeRemoved(Candidate candidate) {
      if (candidate.getChildren() == null) {
        return false;
      } else if (candidate instanceof SegmentationCandidate) {
        SegmentationCandidate segCand = ((SegmentationCandidate) candidate);
        try {
          boolean areCandidatsPicked = segCand.rewriteInternal(conf, hconf);
          if (!areCandidatsPicked) {
            Map<String, PruneCauses<Candidate>> pruneCauses = segCand.getPruneCausesOfFailedContexts();
            Map<String, String> briefCauses = pruneCauses.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getBriefCause()));
            log.info("Segmentation Candidates {} not picked because: {}", candidate, briefCauses);
            cubeql.addCandidatePruningMsg(candidate, segmentationPruned(briefCauses));
          }
          return !areCandidatsPicked;
        } catch (LensException e) {
          log.info("Segmentation Candidates {} not picked because: {}", candidate, e.getMessage());
          cubeql.addCandidatePruningMsg(candidate, segmentationPruned(e));
          return true;
        }
      } else {
        return candidate.getChildren().stream().anyMatch(this::shouldBeRemoved);
      }
    }
  }
}
