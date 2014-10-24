package org.apache.lens.server.stats.event;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Loggable Lens Statistics which is logged to a log4j file as a JSON Object.
 */
public abstract class LoggableLensStatistics extends LensStatistics {

  /**
   * Instantiates a new loggable lens statistics.
   *
   * @param eventTime
   *          the event time
   */
  public LoggableLensStatistics(long eventTime) {
    super(eventTime);
  }

  /**
   * Instantiates a new loggable lens statistics.
   */
  public LoggableLensStatistics() {
    this(System.currentTimeMillis());
  }

  /**
   * Gets the hive table.
   *
   * @param conf
   *          the conf
   * @return the hive table
   */
  public abstract Table getHiveTable(Configuration conf);
}
