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
package org.apache.lens.driver.hive.priority;

import java.util.Map;
import java.util.Set;

import org.apache.lens.api.Priority;
import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.priority.CostToPriorityRangeConf;
import org.apache.lens.server.api.priority.QueryPriorityDecider;
import org.apache.lens.server.api.query.AbstractQueryContext;

public class DurationBasedQueryPriorityDecider implements QueryPriorityDecider {


  private final CostToPriorityRangeConf costToPriorityRangeMap;

  /** Partition Weights for priority calculation based on selected partitions **/

  /** weight of monthly partition * */
  private final float monthlyPartitionWeight;
  /** weight of daily partition * */
  private final float dailyPartitionWeight;
  /** weight of hourly partition * */
  private final float hourlyPartitionWeight;
  private final LensDriver driver;


  /**
   * Constructor. Takes three weights for partitions.
   * @param driver
   * @param ranges
   * @param monthlyPartitoinWeight
   * @param dailyPartitionWeight
   * @param hourlyPartitionWeight
   */
  public DurationBasedQueryPriorityDecider(LensDriver driver,
    String ranges, float monthlyPartitoinWeight, float dailyPartitionWeight, float hourlyPartitionWeight) {
    this.driver = driver;
    this.costToPriorityRangeMap = new CostToPriorityRangeConf(ranges);
    this.monthlyPartitionWeight = monthlyPartitoinWeight;
    this.dailyPartitionWeight = dailyPartitionWeight;
    this.hourlyPartitionWeight = hourlyPartitionWeight;
  }

  /**
   * The Implementation
   *
   * @param abstractQueryContext
   * @return decided Priority
   * @throws LensException Exception occurs mostly when one of drivers/explained queries/plans is null
   */
  public Priority decidePriority(AbstractQueryContext abstractQueryContext) throws LensException {
    float cost = getDurationCost(abstractQueryContext);
    Priority priority = costToPriorityRangeMap.get(cost);
    LOG.info("Deciding Priority " + priority + " since cost = " + cost);
    return priority;
  }

  /**
   * Calculates total cost based on weights of selected tables and their selected partitions
   *
   * @param queryContext
   * @return Query Cost
   * @throws LensException
   */

  @SuppressWarnings("unchecked") // required for (Set<FactPartition>) casting
  private float getDurationCost(AbstractQueryContext queryContext) throws LensException {
    float cost = 0;
    if (queryContext.getDriverContext().getDriverRewriterPlan(driver) != null) {
      // the calculation is done only for cube queries involving fact tables
      // for all other native table queries and dimension only queries, the cost will be zero and priority will
      // be the highest one associated with zero cost
      for (Map.Entry<String, Set<?>> entry : queryContext.getDriverContext().getDriverRewriterPlan(driver)
        .getPartitions().entrySet()) {
        if (!entry.getValue().isEmpty() && entry.getValue().iterator().next() instanceof FactPartition) {
          Set<FactPartition> factParts = (Set<FactPartition>)entry.getValue();
          for (FactPartition partition : factParts) {
            cost += getTableWeights(partition.getStorageTables(), queryContext) * getNormalizedPartitionCost(
              partition.getPeriod());
          }
        }
      }
    }
    return cost;
  }

  private float getTableWeights(Set<String> tables, AbstractQueryContext queryContext) {
    float weight = 0;
    for (String tblName : tables) {
      Double tblWeight = queryContext.getDriverContext().getDriverRewriterPlan(driver).getTableWeight(tblName);
      if (tblWeight != null) {
        weight += tblWeight;
      }
    }
    return weight == 0 ? 1 : weight;
  }

  /**
   * Normalized cost of a partition. PartitionWeight multiplied by number of days in that partition.
   *
   * @param partition
   * @return normalized cost.
   * @throws LensException
   */
  private float getNormalizedPartitionCost(UpdatePeriod updatePeriod) throws LensException {
    switch (updatePeriod) {
    case MONTHLY: //monthly
      return 30 * monthlyPartitionWeight;
    case DAILY: // daily
      return 1 * dailyPartitionWeight;
    case HOURLY: // hourly
      return (1.0f / 24) * hourlyPartitionWeight;
    default:
      throw new LensException("Weight not defined for " + updatePeriod);
    }
  }
}
