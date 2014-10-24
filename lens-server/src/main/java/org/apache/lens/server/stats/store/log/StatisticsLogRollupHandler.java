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
package org.apache.lens.server.stats.store.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.events.LensEventService;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.Timer;

/**
 * Class which handles the log4j rolled file.
 */
public class StatisticsLogRollupHandler {

  /** The task. */
  private org.apache.lens.server.stats.store.log.StatisticsLogFileScannerTask task;

  /** The timer. */
  private Timer timer;

  /** The rate. */
  private long rate;

  /** The scan set. */
  private final ConcurrentHashSet<String> scanSet = new ConcurrentHashSet<String>();

  /**
   * Initalize the handler.
   *
   * @param conf
   *          configuration to be used while initialization.
   */
  public void initialize(Configuration conf) {
    task = new StatisticsLogFileScannerTask();
    timer = new Timer();
    rate = conf.getLong(LensConfConstants.STATS_ROLLUP_SCAN_RATE, LensConfConstants.DEFAULT_STATS_ROLLUP_SCAN_RATE);
  }

  /**
   * Start.
   *
   * @param service
   *          the service
   */
  public void start(LensEventService service) {
    task.setService(service);
    timer.scheduleAtFixedRate(task, rate, rate);
  }

  /**
   * Stop.
   */
  public void stop() {
    timer.cancel();
  }

  /**
   * Adds the to scan task.
   *
   * @param event
   *          the event
   */
  public void addToScanTask(String event) {
    if (!scanSet.contains(event)) {
      scanSet.add(event);
      task.addLogFile(event);
    }
  }

}
