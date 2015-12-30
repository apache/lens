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

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * Prune dimension tables having more weight than minimum
 */
@Slf4j
class LightestDimensionResolver implements ContextRewriter {

  public LightestDimensionResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {
    if (!cubeql.getCandidateDimTables().isEmpty()) {
      for (Map.Entry<Dimension, Set<CandidateDim>> entry : cubeql.getCandidateDimTables().entrySet()) {
        if (entry.getValue().isEmpty()) {
          continue;
        }
        Map<CandidateDim, Double> dimWeightMap = new HashMap<CandidateDim, Double>();

        for (CandidateDim dim : entry.getValue()) {
          dimWeightMap.put(dim, dim.dimtable.weight());
        }

        double minWeight = Collections.min(dimWeightMap.values());

        for (Iterator<CandidateDim> i = entry.getValue().iterator(); i.hasNext();) {
          CandidateDim dim = i.next();
          if (dimWeightMap.get(dim) > minWeight) {
            log.info("Not considering dimtable:{} from candidate dimension tables as it has more weight:{} minimum:{}",
              dim, dimWeightMap.get(dim), minWeight);
            cubeql.addDimPruningMsgs(entry.getKey(), dim.dimtable, new CandidateTablePruneCause(
              CandidateTablePruneCode.MORE_WEIGHT));
            i.remove();
          }
        }
      }
    }
  }
}
