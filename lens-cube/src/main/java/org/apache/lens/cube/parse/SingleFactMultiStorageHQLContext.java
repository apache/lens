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
import java.util.Map;

import org.apache.lens.cube.metadata.Dimension;
import org.apache.lens.server.api.error.LensException;

import lombok.Getter;


public class SingleFactMultiStorageHQLContext extends UnionHQLContext {

  @Getter
  private CubeQueryContext query = null;
  private CandidateFact fact = null;

  SingleFactMultiStorageHQLContext(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query)
    throws LensException {
    this.query = query;
    this.fact = fact;
    setUnionContexts(fact, dimsToQuery, query);
  }

  private void setUnionContexts(CandidateFact fact, Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query)
    throws LensException {
    hqlContexts = new ArrayList<HQLContextInterface>();
    String alias = getQuery().getAliasForTableName(getQuery().getCube().getName());
    for (String storageTable : fact.getStorageTables()) {
      SingleFactHQLContext ctx = new SingleFactHQLContext(fact, storageTable + " " + alias, dimsToQuery, query,
          fact.getWhereClause(storageTable.substring(storageTable.indexOf(".") + 1)));
      hqlContexts.add(ctx);
    }
    super.setHqlContexts(hqlContexts);
  }

}
