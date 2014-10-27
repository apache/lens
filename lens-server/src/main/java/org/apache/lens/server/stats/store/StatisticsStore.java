/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
