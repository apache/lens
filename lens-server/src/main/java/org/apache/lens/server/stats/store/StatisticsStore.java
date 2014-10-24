package org.apache.lens.server.stats.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.stats.event.LensStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top level class used to persist the Statistics event.
 *
 * @param <T>
 *          the generic type
 */
public abstract class StatisticsStore<T extends LensStatistics> extends AsyncEventListener<T> {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsStore.class);

  /**
   * Initialize the store.
   *
   * @param conf
   *          configuration for the store
   */
  public abstract void initialize(Configuration conf);

  /**
   * Start the Store.
   *
   * @param service
   *          the service
   */
  public void start(LensEventService service) {
    if (service == null) {
      LOG.warn("Unable to start store as Event service is null");
    }
  }

  /**
   * Stop the store.
   *
   * @param service
   *          the service
   */
  public void stop(LensEventService service) {
    if (service == null) {
      LOG.warn("Unable to stop store as Event service is null");
    }
  }
}
