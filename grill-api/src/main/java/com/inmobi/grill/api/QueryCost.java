package com.inmobi.grill.api;

public class QueryCost implements Comparable<QueryCost> {
  private final long estimatedExecTimeMillis;
  private final double estimatedResourceUsage;

  public QueryCost(long exectimeMillis, double resourceUsage) {
    this.estimatedExecTimeMillis = exectimeMillis;
    this.estimatedResourceUsage = resourceUsage;
  }

  @Override
  public int compareTo(QueryCost other) {
    if (estimatedExecTimeMillis == other.estimatedExecTimeMillis) {
      if (estimatedResourceUsage == other.estimatedResourceUsage) {
        return 0;
      } else {
        return (int) (estimatedResourceUsage - other.estimatedResourceUsage);
      }
    } else {
      return (int) (estimatedExecTimeMillis - other.estimatedExecTimeMillis);
    }
  }
  
  public double getEstimatedResourceUsage() {
    return estimatedResourceUsage;
  }

  public long getEstimatedExecTimeMillis() {
    return estimatedExecTimeMillis;
  }
}
