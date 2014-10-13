package org.apache.lens.server.stats.store;
 /*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.lens.server.api.events.AsyncEventListener;
import org.apache.lens.server.api.events.GrillEventService;
import org.apache.lens.server.stats.event.GrillStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top level class used to persist the Statistics event.
 */
public abstract class StatisticsStore<T extends GrillStatistics> extends AsyncEventListener<T> {
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsStore.class);
  /**
   * Initialize the store
   *
   * @param conf configuration for the store
   */
  public abstract void initialize(Configuration conf);

  /**
   * Start the Store
   */
  public void start(GrillEventService service) {
    if(service == null) {
      LOG.warn("Unable to start store as Event service is null");
    }
  }

  /**
   * Stop the store.
   */
  public void stop(GrillEventService service) {
    if(service == null) {
      LOG.warn("Unable to stop store as Event service is null");
    }
  }
}
