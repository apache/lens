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
package org.apache.lens.driver.cube;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.parse.CandidateTable;
import org.apache.lens.cube.parse.CubeQueryContext;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.apache.hadoop.hive.ql.metadata.Table;

import lombok.extern.slf4j.Slf4j;

/**
 * The Rewriter plan
 */
@Slf4j
public final class RewriterPlan extends DriverQueryPlan {

  public RewriterPlan(Collection<CubeQueryContext> cubeQueries) {
    extractPlan(cubeQueries);
  }

  @SuppressWarnings("unchecked") // required for (Set<FactPartition>) casting
  void extractPlan(Collection<CubeQueryContext> cubeQueries) {

    for (CubeQueryContext ctx : cubeQueries) {
      if (ctx.getPickedDimTables() != null && !ctx.getPickedDimTables().isEmpty()) {
        for (CandidateTable dim : ctx.getPickedDimTables()) {
          addTablesQueried(dim.getStorageTables());
          if (partitions.get(dim.getName()) == null || partitions.get(dim.getName()).isEmpty()) {
            // puts storage table to latest part
            partitions.put(dim.getName(), dim.getPartsQueried());
          }
        }
      }
      if (ctx.getPickedFacts() != null && !ctx.getPickedFacts().isEmpty()) {
        for (CandidateTable fact : ctx.getPickedFacts()) {
          addTablesQueried(fact.getStorageTables());
          Set<FactPartition> factParts = (Set<FactPartition>) partitions.get(fact.getName());
          if (factParts == null) {
            factParts = new HashSet<FactPartition>();
            partitions.put(fact.getName(), factParts);
          }
          factParts.addAll((Set<FactPartition>) fact.getPartsQueried());
        }
      }
      for (String table : getTablesQueried()) {
        if (!tableWeights.containsKey(table)) {
          Table tbl;
          try {
            tbl = ctx.getMetastoreClient().getTable(table);
          } catch (LensException e) {
            log.error("Error while getting table:" + table, e);
            continue;
          }
          String costStr = tbl.getParameters().get(LensConfConstants.STORAGE_COST);
          Double weight = 1d;
          if (costStr != null) {
            weight = Double.parseDouble(costStr);
          }
          tableWeights.put(table, weight);
        }
      }
    }
    setHasSubQuery(hasSubQuery || cubeQueries.size() > 1);
  }

  @Override
  public String getPlan() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public QueryCost getCost() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
