package org.apache.lens.server.stats.store.log;
/*
 * #%L
 * Lens Server
 * %%
 * Copyright (C) 2014 Apache Software Foundation
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


public class LogStatisticsStore extends StatisticsStore<LoggableLensStatistics> {
  private static final Logger LOG = LoggerFactory.getLogger(LogStatisticsStore.class);
  public static final String LOG_STORE_ERRORS = "log-store-errors";
  private final ObjectMapper mapper;
  private StatisticsLogPartitionHandler handler;
  private StatisticsLogRollupHandler rollupHandler;

  public LogStatisticsStore() {
    this.mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  public void initialize(Configuration conf) {
    LOG.info("Creating new Partition handler");
    handler = new StatisticsLogPartitionHandler();
    handler.initialize(conf);
    LOG.info("Creating new rollup handler");
    rollupHandler = new StatisticsLogRollupHandler();
    rollupHandler.initialize(conf);
  }


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
        LoggerFactory.getLogger(eventClass)
        .info(representation);
      }
    } catch (Exception exc) {
      MetricsService metricsService = (MetricsService) LensServices.get().getService(MetricsService.NAME);
      metricsService.incrCounter(LogStatisticsStore.class, LOG_STORE_ERRORS);
      LOG.error("Unknown error ", exc);
    }

  }

  public void start(LensEventService service) {
    super.start(service);
    if(service != null) {
      service.addListenerForType(this, LoggableLensStatistics.class);
      service.addListenerForType(handler, PartitionEvent.class);
      rollupHandler.start(service);
    } else {
      LOG.warn("Not starting Log Statistics store as event service is not configured");
    }

  }

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
