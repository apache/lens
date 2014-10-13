package org.apache.lens.server.stats;
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


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;
import org.apache.lens.server.EventServiceImpl;
import org.apache.lens.server.GrillServices;
import org.apache.lens.server.api.GrillConfConstants;
import org.apache.lens.server.stats.store.StatisticsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(StatisticsService.class);

  public static final String STATS_SVC_NAME = "stats";

  private StatisticsStore store;

  public StatisticsService(String name) {
    super(STATS_SVC_NAME);
  }


  @Override
  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    String storeClassName = hiveConf.get(GrillConfConstants.STATS_STORE_CLASS);
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


  @Override
  public synchronized void start() {
    if (store != null) {
      store.start((org.apache.lens.server.api.events.GrillEventService)
          GrillServices.get().getService(EventServiceImpl.NAME));
    } else {
      LOG.warn("Unable to start the LogStore.");
    }

    super.start();
  }

  @Override
  public synchronized void stop() {
    if (store != null) {
      store.stop((org.apache.lens.server.api.events.GrillEventService)
          GrillServices.get().getService(EventServiceImpl.NAME));
    } else {
      LOG.warn("Not starting the LogStore as it was not started.");
    }
    super.stop();
  }
}
