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
package org.apache.lens.cube.query.cost;

import java.util.Map;
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.query.cost.QueryCostCalculator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class FactPartitionBasedQueryCostCalculator implements QueryCostCalculator {

  public static final String UPDATE_PERIOD_WEIGHT_PREFIX = "update.period.weight.";

  /**
   * Calculates total cost based on weights of selected tables and their selected partitions
   *
   * @param queryContext
   * @param driver
   * @return Query Cost
   * @throws LensException
   */

  @SuppressWarnings("unchecked") // required for (Set<FactPartition>) casting
  private Double getTotalPartitionCost(final AbstractQueryContext queryContext, LensDriver driver)
    throws LensException {
    if (queryContext.getDriverRewriterPlan(driver) == null) {
      return null;
    }
    double cost = 0;
    for (Map.Entry<String, Set<?>> entry : getAllPartitions(queryContext, driver).entrySet()) {
      // Have to do instanceof check, since it can't be handled by polymorphism.
      // The '?' is either a FactPartition or a String. When we decide to write a
      // DimtablePartition, we can probably think about using polymorphism.
      if (!entry.getValue().isEmpty() && entry.getValue().iterator().next() instanceof FactPartition) {
        Set<FactPartition> factParts = (Set<FactPartition>) entry.getValue();
        for (FactPartition partition : factParts) {
          double allTableWeights =
            partition.getAllTableWeights(ImmutableMap.copyOf(queryContext.getTableWeights(driver)));
          if (allTableWeights == 0) {
            allTableWeights = 1;
          }
          cost += allTableWeights * getNormalizedUpdatePeriodCost(partition.getPeriod(), driver);
        }
      }
    }
    return cost;
  }

  /**
   * Normalized cost of an update period. update period weight multiplied by number of partitions of that update period.
   *
   * @param updatePeriod
   * @param driver
   * @return normalized cost.
   * @throws LensException
   */
  private double getNormalizedUpdatePeriodCost(final UpdatePeriod updatePeriod, LensDriver driver)
    throws LensException {
    double weight = driver.getConf().getDouble(UPDATE_PERIOD_WEIGHT_PREFIX + updatePeriod.name().toLowerCase(),
      updatePeriod.getNormalizationFactor());
    return weight * updatePeriod.weight() / UpdatePeriod.DAILY.weight();
  }

  @Override
  public QueryCost calculateCost(final AbstractQueryContext queryContext, LensDriver driver) throws LensException {
    Double cost = getTotalPartitionCost(queryContext, driver);
    return cost == null ? null : new FactPartitionBasedQueryCost(cost);
  }

  public Map<String, Set<?>> getAllPartitions(AbstractQueryContext queryContext, LensDriver driver) {
    if (queryContext.getDriverRewriterPlan(driver) != null) {
      return queryContext.getDriverRewriterPlan(driver).getPartitions();
    }
    return Maps.newHashMap();
  }
}
