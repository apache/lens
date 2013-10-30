package com.inmobi.grill.api;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class QueryCost implements Comparable<QueryCost> {

  @XmlElement
  private long estimatedExecTimeMillis;
  @XmlElement
  private double estimatedResourceUsage;

  public QueryCost() {
    
  }
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
