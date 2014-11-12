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
  private float monthlyPartitionWeight;
  private float dailyPartitionWeight;
  private float hourlyPartitionWeight;

  public DurationBasedQueryPriorityDecider(float mw, float dw, float hw){
    monthlyPartitionWeight = mw;
    dailyPartitionWeight = dw;
    hourlyPartitionWeight = hw;
  }
  static final CostToPriorityRangeConf costToPriorityRangeMap =
    // Hard Coded
    // Arbitrary for now. Will need to tune it.
    new CostToPriorityRangeConf("VERY_HIGH,7.0,HIGH,30.0,NORMAL,90,LOW");

  public Priority decidePriority(AbstractQueryContext queryContext) throws LensException {
    return costToPriorityRangeMap.get(getDurationCost(queryContext));
  }

  protected Map<String,List<String>> extractPartitions(AbstractQueryContext queryContext) throws LensException{
    Map<String, List<String>> partitions = new HashMap<String, List<String>>();
    for(Map.Entry<String, List<String>> entry: queryContext.getSelectedDriverQueryPlan().getPartitions().entrySet()) {
      partitions.put(entry.getKey(), new ArrayList<String>());
      for(String s: entry.getValue()) {
        String[] splits = s.split("\\s+");
        partitions.get(entry.getKey()).add(splits[splits.length - 1]); //last split.
      }
    }
    return partitions;
  }


  float getDurationCost(AbstractQueryContext queryContext) throws LensException {
    final Map<String, List<String>> partitions = extractPartitions(queryContext);
    float cost = 0;
    for(String table: partitions.keySet()) {
      for(String partition: partitions.get(table)) {
        if(!partition.equals("latest")) {
          cost += queryContext.getSelectedDriverQueryPlan().getTableWeight(table) * getPartitionCost(partition);
        }
      }
    }
    return cost;
  }

  float getPartitionCost(String partition) throws LensException {
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
