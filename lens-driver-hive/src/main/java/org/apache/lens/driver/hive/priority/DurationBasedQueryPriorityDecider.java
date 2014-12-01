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

import org.apache.lens.api.LensException;
import org.apache.lens.api.Priority;
import org.apache.lens.server.api.priority.CostToPriorityRangeConf;
import org.apache.lens.server.api.priority.QueryPriorityDecider;
import org.apache.lens.server.api.query.AbstractQueryContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DurationBasedQueryPriorityDecider implements QueryPriorityDecider {

  /** Partition Weights for priority calculation based on selected partitions **/

  /** weight of monthly partition **/
  private float monthlyPartitionWeight;
  /** weight of daily partition **/
  private float dailyPartitionWeight;
  /** weight of hourly partition **/
  private float hourlyPartitionWeight;

  /**
   * Constructor. Takes three weights for partitions.
   * @param mmonthlyPartitoinWeight
   * @param dailyPartitionWeight
   * @param hourlyPartitionWeight
   */
  public DurationBasedQueryPriorityDecider(float mmonthlyPartitoinWeight,
    float dailyPartitionWeight, float hourlyPartitionWeight){
    monthlyPartitionWeight = mmonthlyPartitoinWeight;
    this.dailyPartitionWeight = dailyPartitionWeight;
    this.hourlyPartitionWeight = hourlyPartitionWeight;
  }

  /**
   * Hard Coded
   * Arbitrary for now. Will need to tune it.
   * Some perspective wrt default weights(1 for hourly, 0.75 for daily, 0.5 for monthly)
   * For exclusively hourly data this translates to VERY_HIGH,7days,HIGH,30days,NORMAL,90days,LOW
   * FOR exclusively daily data this translates to VERY_HIGH,9days,HIGH,40days,NORMAL,120days,LOW
   * for exclusively monthly data this translates to VERY_HIGH,never,HIGH,1month,NORMAL,6months,LOW
   */
  static final CostToPriorityRangeConf costToPriorityRangeMap =

    new CostToPriorityRangeConf("VERY_HIGH,7.0,HIGH,30.0,NORMAL,90,LOW");

  /**
   * The Implementation
   * @param abstractQueryContext
   * @return decided Priority
   * @throws LensException
   * Exception occurs mostly when one of drivers/explained queries/plans is null
   */
  public Priority decidePriority(AbstractQueryContext abstractQueryContext) throws LensException {
    return costToPriorityRangeMap.get(getDurationCost(abstractQueryContext));
  }

  /**
   * Extract partitions from AbstractQueryContext. Hive currently gives partitions in the format
   * {a:[dt partition1, dt partition2]...}. This method removes the "dt"
   * @param queryContext
   * @return all the tables along with their selected partitions.
   * @throws LensException
   */
  protected Map<String,List<String>> extractPartitions(AbstractQueryContext queryContext) throws LensException{
    Map<String, List<String>> partitions = new HashMap<String, List<String>>();
    for(Map.Entry<String, List<String>> entry: queryContext.getDriverContext().getSelectedDriverQueryPlan()
      .getPartitions().entrySet
      ()) {
      partitions.put(entry.getKey(), new ArrayList<String>());
      for(String s: entry.getValue()) {
        String[] splits = s.split("\\s+");
        partitions.get(entry.getKey()).add(splits[splits.length - 1]); //last split.
      }
    }
    return partitions;
  }

  /**
   * Calculates total cost based on weights of selected tables and their selected partitions
   * @param queryContext
   * @return Query Cost
   * @throws LensException
   */

  float getDurationCost(AbstractQueryContext queryContext) throws LensException {
    final Map<String, List<String>> partitions = extractPartitions(queryContext);
    float cost = 0;
    for(String table: partitions.keySet()) {
      for(String partition: partitions.get(table)) {
        if(!partition.equals("latest")) {
          cost +=
            queryContext.getDriverContext().getSelectedDriverQueryPlan().getTableWeight(table) *
              getNormalizedPartitionCost
              (partition);
        }
      }
    }
    return cost;
  }

  /**
   * Normalized cost of a partition. PartitionWeight multiplied by number of days in that partition.
   * @param partition
   * @return normalized cost.
   * @throws LensException
   */
  float getNormalizedPartitionCost(String partition) throws LensException {
    switch (partition.length()) {
      case 7: //monthly
        return 30 * monthlyPartitionWeight;
      case 10: // daily
        return 1 * dailyPartitionWeight;
      case 13: // hourly
        return (1/24) * hourlyPartitionWeight;
      default:
        throw new LensException("Could not recognize partition: " + partition);
    }
  }
}
