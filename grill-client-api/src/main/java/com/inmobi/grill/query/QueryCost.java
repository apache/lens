package com.inmobi.grill.query;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryCost implements Comparable<QueryCost> {

  @XmlElement @Getter
  private long estimatedExecTimeMillis;
  @XmlElement @Getter
  private double estimatedResourceUsage;

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
}
