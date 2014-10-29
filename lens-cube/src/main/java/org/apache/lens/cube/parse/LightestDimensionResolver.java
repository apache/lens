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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.cube.parse.CandidateTablePruneCause.CubeTableCause;

/**
 * Prune dimension tables having more weight than minimum
 */
class LightestDimensionResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(LightestDimensionResolver.class.getName());

  public LightestDimensionResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
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
            LOG.info("Not considering dimtable:" + dim + " from candidate dimension tables as it has more weight:"
                + dimWeightMap.get(dim) + " minimum:" + minWeight);
            cubeql.addDimPruningMsgs(entry.getKey(), dim.dimtable, new CandidateTablePruneCause(dim.dimtable.getName(),
                CubeTableCause.MORE_WEIGHT));
            i.remove();
          }
        }
      }
    }
  }
}
