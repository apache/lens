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
package org.apache.lens.server.stats;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;
import org.apache.lens.server.EventServiceImpl;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.stats.store.StatisticsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class StatisticsService.
 */
public class StatisticsService extends AbstractService {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsService.class);

  /** The Constant STATS_SVC_NAME. */
  public static final String STATS_SVC_NAME = "stats";

  /** The store. */
  private StatisticsStore store;

  /**
   * Instantiates a new statistics service.
   *
   * @param name
   *          the name
   */
  public StatisticsService(String name) {
    super(STATS_SVC_NAME);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.AbstractService#init(org.apache.hadoop.hive.conf.HiveConf)
   */
  @Override
  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    String storeClassName = hiveConf.get(LensConfConstants.STATS_STORE_CLASS);
    if (storeClassName == null) {
      LOG.warn("Statistics service configured with no Stores defined");
      return;
    }
    Class<StatisticsStore> klass = null;
    try {
      klass = (Class<StatisticsStore>) Class.forName(storeClassName);
      store = klass.newInstance();
      LOG.info("Initializing Statistics Store  " + klass.getName());
      store.initialize(hiveConf);
    } catch (Exception e) {
      LOG.error("Unable to initalize the statistics store", e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.AbstractService#start()
   */
  @Override
  public synchronized void start() {
    if (store != null) {
      store.start((org.apache.lens.server.api.events.LensEventService) LensServices.get().getService(
          EventServiceImpl.NAME));
    } else {
      LOG.warn("Unable to start the LogStore.");
    }

    super.start();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.AbstractService#stop()
   */
  @Override
  public synchronized void stop() {
    if (store != null) {
      store.stop((org.apache.lens.server.api.events.LensEventService) LensServices.get().getService(
          EventServiceImpl.NAME));
    } else {
      LOG.warn("Not starting the LogStore as it was not started.");
    }
    super.stop();
  }
}
