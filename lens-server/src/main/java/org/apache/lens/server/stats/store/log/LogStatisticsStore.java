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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.stats.event.LoggableLensStatistics;
import org.apache.lens.server.stats.store.StatisticsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class LogStatisticsStore.
 */
public class LogStatisticsStore extends StatisticsStore<LoggableLensStatistics> {

  /** The Constant LOG. */
  private static final Logger LOG = LoggerFactory.getLogger(LogStatisticsStore.class);

  /** The Constant LOG_STORE_ERRORS. */
  public static final String LOG_STORE_ERRORS = "log-store-errors";

  /** The mapper. */
  private final ObjectMapper mapper;

  /** The handler. */
  private StatisticsLogPartitionHandler handler;

  /** The rollup handler. */
  private StatisticsLogRollupHandler rollupHandler;

  /**
   * Instantiates a new log statistics store.
   */
  public LogStatisticsStore() {
    this.mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.stats.store.StatisticsStore#initialize(org.apache.hadoop.conf.Configuration)
   */
  public void initialize(Configuration conf) {
    LOG.info("Creating new Partition handler");
    handler = new StatisticsLogPartitionHandler();
    handler.initialize(conf);
    LOG.info("Creating new rollup handler");
    rollupHandler = new StatisticsLogRollupHandler();
    rollupHandler.initialize(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.events.AsyncEventListener#process(org.apache.lens.server.api.events.LensEvent)
   */
  @Override
  public void process(LoggableLensStatistics event) {
    try {
      Class eventClass = event.getClass();
      String representation = null;
      try {
        representation = mapper.writeValueAsString(event);
      } catch (JsonProcessingException ignored) {
      }
      if (representation != null) {
        rollupHandler.addToScanTask(eventClass.getName());
        LoggerFactory.getLogger(eventClass).info(representation);
      }
    } catch (Exception exc) {
      MetricsService metricsService = (MetricsService) LensServices.get().getService(MetricsService.NAME);
      metricsService.incrCounter(LogStatisticsStore.class, LOG_STORE_ERRORS);
      LOG.error("Unknown error ", exc);
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.stats.store.StatisticsStore#start(org.apache.lens.server.api.events.LensEventService)
   */
  public void start(LensEventService service) {
    super.start(service);
    if (service != null) {
      service.addListenerForType(this, LoggableLensStatistics.class);
      service.addListenerForType(handler, PartitionEvent.class);
      rollupHandler.start(service);
    } else {
      LOG.warn("Not starting Log Statistics store as event service is not configured");
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.stats.store.StatisticsStore#stop(org.apache.lens.server.api.events.LensEventService)
   */
  public void stop(LensEventService service) {
    super.stop(service);
    if (service != null) {
      service.removeListenerForType(this, LoggableLensStatistics.class);
      service.removeListenerForType(handler, PartitionEvent.class);
      rollupHandler.stop();
    } else {
      LOG.warn("Not stopping Log Statistics store as event service is not configured");
    }
  }

}
