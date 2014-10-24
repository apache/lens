package org.apache.lens.server.api.driver;

import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * The Interface DriverSelector.
 */
public interface DriverSelector {

  /**
   * Select.
   *
   * @param drivers
   *          the drivers
   * @param queries
   *          the queries
   * @param conf
   *          the conf
   * @return the lens driver
   */
  public LensDriver select(Collection<LensDriver> drivers, Map<LensDriver, String> queries, Configuration conf);
}
