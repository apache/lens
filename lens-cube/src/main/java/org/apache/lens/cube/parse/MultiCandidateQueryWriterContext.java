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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

import com.google.common.collect.Lists;
import lombok.Getter;

/**
 * Created on 31/03/17.
 */
public class MultiCandidateQueryWriterContext implements QueryWriterContext {
  @Getter
  private List<QueryWriterContext> children;
  @Getter
  private CubeQueryContext cubeQueryContext;

  public MultiCandidateQueryWriterContext(List<QueryWriterContext> children, CubeQueryContext cubeQueryContext) {
    this.children = children;
    this.cubeQueryContext = cubeQueryContext;
  }

  public void addAutoJoinDims() throws LensException {
    for (QueryWriterContext candidate : getChildren()) {
      candidate.addAutoJoinDims();
    }
  }

  public void addExpressionDims() throws LensException {
    for (QueryWriterContext candidate : getChildren()) {
      candidate.addExpressionDims();
    }
  }

  public void addDenormDims() throws LensException {
    for (QueryWriterContext candidate : getChildren()) {
      candidate.addDenormDims();
    }
  }

  public void updateDimFilterWithFactFilter() throws LensException {
    for (QueryWriterContext candidate : getChildren()) {
      candidate.updateDimFilterWithFactFilter();
    }
  }

  @Override
  public QueryAST getQueryAst() {
    return getCubeQueryContext();
  }

  @Override
  public void updateFromString() throws LensException {
    for (QueryWriterContext queryWriterContext : getChildren()) {
      queryWriterContext.updateFromString();
    }
  }
  private List<StorageCandidateHQLContext> getLeafQueryWriterContexts() {
    List<StorageCandidateHQLContext> ret = Lists.newArrayList();
    for (QueryWriterContext queryWriterContext : getChildren()) {
      if (queryWriterContext instanceof MultiCandidateQueryWriterContext) {
        ret.addAll(((MultiCandidateQueryWriterContext) queryWriterContext).getLeafQueryWriterContexts());
      } else {
        ret.add((StorageCandidateHQLContext) queryWriterContext);
      }
    }
    return ret;
  }
  @Override
  public UnionQueryWriter toQueryWriter() throws LensException {
    List<StorageCandidateHQLContext> leafWriterContexts = getLeafQueryWriterContexts();
    return new UnionQueryWriter(leafWriterContexts, getCubeQueryContext());
  }

  public Map<Dimension, CandidateDim> getDimsToQuery() {
    Map<Dimension, CandidateDim> allDimsQueried = new HashMap<>();
    for (QueryWriterContext ctx : children) {
      allDimsQueried.putAll(ctx.getDimsToQuery());
    }
    return allDimsQueried;
  }
}
