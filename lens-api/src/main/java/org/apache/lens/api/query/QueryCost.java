/*
 * 
 */
package org.apache.lens.api.query;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * The Class QueryCost.
 */
@XmlRootElement
/**
 * Instantiates a new query cost.
 *
 * @param estimatedExecTimeMillis
 *          the estimated exec time millis
 * @param estimatedResourceUsage
 *          the estimated resource usage
 */
@AllArgsConstructor
/**
 * Instantiates a new query cost.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryCost implements Comparable<QueryCost> {

  /** The estimated exec time millis. */
  @XmlElement
  @Getter
  private long estimatedExecTimeMillis;

  /** The estimated resource usage. */
  @XmlElement
  @Getter
  private double estimatedResourceUsage;

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
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
