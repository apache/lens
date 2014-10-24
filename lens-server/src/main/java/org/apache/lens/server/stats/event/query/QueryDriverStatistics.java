package org.apache.lens.server.stats.event.query;

import lombok.Getter;
import lombok.Setter;

/**
 * Statistics class for capturing query driver information.
 */
public class QueryDriverStatistics {

  /** The name. */
  @Getter
  @Setter
  private String name;

  /** The driver query. */
  @Getter
  @Setter
  private String driverQuery;

  /** The start time. */
  @Getter
  @Setter
  private long startTime;

  /** The end time. */
  @Getter
  @Setter
  private long endTime;
}
