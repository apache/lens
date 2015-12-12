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

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.NotImplementedException;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

@AllArgsConstructor
@RequiredArgsConstructor
public abstract class UnionHQLContext extends SimpleHQLContext {
  protected final CubeQueryContext query;
  protected final CandidateFact fact;

  List<HQLContextInterface> hqlContexts = new ArrayList<>();

  public void setHqlContexts(List<HQLContextInterface> hqlContexts) throws LensException {
    this.hqlContexts = hqlContexts;
    StringBuilder queryParts = new StringBuilder("(");
    String sep = "";
    for (HQLContextInterface ctx : hqlContexts) {
      queryParts.append(sep).append(ctx.toHQL());
      sep = " UNION ALL ";
    }
    setFrom(queryParts.append(") ").append(query.getCube().getName()).toString());
  }

  @Override
  public String getWhere() {
    throw new NotImplementedException("Not Implemented");
  }
}
