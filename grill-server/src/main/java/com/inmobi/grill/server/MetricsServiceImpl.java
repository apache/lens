package com.inmobi.grill.server;

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

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import lombok.Getter;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;
import org.apache.log4j.Logger;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.codahale.metrics.JvmAttributeGaugeSet;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.events.AsyncEventListener;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.api.metrics.MetricsService;
import com.inmobi.grill.server.api.query.QueryExecutionService;
import com.inmobi.grill.server.api.query.StatusChange;

public class MetricsServiceImpl extends AbstractService implements MetricsService {
  public static final String METRICS_SVC_NAME = "metrics";
  public static final Logger LOG = Logger.getLogger(MetricsService.class);
  private AsyncEventListener<StatusChange> queryStatusListener;
  @Getter private MetricRegistry metricRegistry;
  private List<ScheduledReporter> reporters;
  @Getter private HealthCheckRegistry healthCheck;

  private Counter queuedQueries;
  private Counter runningQueries;
  private Counter finishedQueries;
  private Counter totalQueuedQueries;
  private Counter totalSuccessfulQueries;
  private Counter totalFinishedQueries;
  private Counter totalFailedQueries;
  private Counter totalCancelledQueries;


  public class AsyncQueryStatusListener extends AsyncEventListener<StatusChange> {
    @Override
    public void process(StatusChange event) {
      processCurrentStatus(event.getCurrentValue());
      processPrevStatus(event.getPreviousValue());
    }

    protected void processPrevStatus(Status previousValue) {
      switch (previousValue) {
      case QUEUED:
        queuedQueries.dec(); break;
      case RUNNING:
        runningQueries.dec(); break;
      default:
        break;
      }
    }


    protected void processCurrentStatus(Status currentValue) {
      switch(currentValue) {
      case QUEUED:
        queuedQueries.inc();
        totalQueuedQueries.inc();
        break;
      case RUNNING:
        runningQueries.inc(); 
        break;
      case CANCELED:
        finishedQueries.inc();
        totalCancelledQueries.inc();
        totalFinishedQueries.inc();
        break;
      case FAILED:
        finishedQueries.inc();
        totalFailedQueries.inc();
        totalFinishedQueries.inc();
        break;
      case SUCCESSFUL:
        finishedQueries.inc();
        totalSuccessfulQueries.inc();
        totalFinishedQueries.inc();
        break;
      case CLOSED:
        finishedQueries.dec();
      default:
        break;
      }
    }
  }

  public MetricsServiceImpl(String name) {
    super(METRICS_SVC_NAME);
  }

  private static int timeBetweenPolls = 10;

  @Override
  public synchronized void init(HiveConf hiveConf) {
    queryStatusListener = new AsyncQueryStatusListener();
    GrillEventService eventService = 
        (GrillEventService) GrillServices.get().getService(GrillEventService.NAME);
    eventService.addListenerForType(queryStatusListener, StatusChange.class);
    metricRegistry = new MetricRegistry();
    healthCheck = new HealthCheckRegistry(); 
    initCounters();
    timeBetweenPolls = hiveConf.getInt(GrillConfConstants.REPORTING_PERIOD , 10);

    reporters = new ArrayList<ScheduledReporter>();
    if (hiveConf.getBoolean(GrillConfConstants.ENABLE_CONSOLE_METRICS, false)) {
      // Start console reporter
      ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .build();
      reporters.add(reporter);
    }

    if (hiveConf.getBoolean(GrillConfConstants.ENABLE_CONSOLE_METRICS, false)) {
      GMetric ganglia;
      try {
        ganglia = new GMetric(hiveConf.get(GrillConfConstants.GANGLIA_SERVERNAME),
            hiveConf.getInt(GrillConfConstants.GANGLIA_PORT, 8080),
            UDPAddressingMode.MULTICAST, 1);
        GangliaReporter greporter = GangliaReporter.forRegistry(metricRegistry)
            .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(ganglia);

        reporters.add(greporter);
      } catch (IOException e) {
        LOG.error("Could not start ganglia reporter", e);
      }
    }
    LOG.info("Started metrics service");
    super.init(hiveConf);
  }

  protected void initCounters() {
    queuedQueries = 
        metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, QUEUED_QUERIES));
    totalQueuedQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class,
        "total-" + QUEUED_QUERIES));

    runningQueries = 
        metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, RUNNING_QUERIES));

    totalSuccessfulQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, 
        "total-success-queries"));

    finishedQueries = 
        metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, FINISHED_QUERIES));
    totalFinishedQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, 
        "total-" + FINISHED_QUERIES));

    totalFailedQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, 
        "total-" + FAILED_QUERIES));

    totalCancelledQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, 
        "total-" + CANCELLED_QUERIES));

    metricRegistry.register("gc", new GarbageCollectorMetricSet());
    metricRegistry.register("memory", new MemoryUsageGaugeSet());
    metricRegistry.register("threads", new ThreadStatesGaugeSet());
    metricRegistry.register("jvm", new JvmAttributeGaugeSet());
  }

  @Override
  public synchronized void start() {
    for (ScheduledReporter reporter : reporters) {
      reporter.start(timeBetweenPolls, TimeUnit.SECONDS);
    }
    super.start();

  }

  @Override
  public synchronized void stop() {
    // unregister 
    GrillEventService eventService = 
        (GrillEventService) GrillServices.get().getService(GrillEventService.NAME);
    eventService.removeListener(queryStatusListener);
    queryStatusListener.stop();
    for (ScheduledReporter reporter : reporters) {
      reporter.stop();
    }
    LOG.info("Stopped metrics service");
    super.stop();
  }


  @Override
  public void incrCounter(String counter) {
    incrCounter(MetricsService.class, counter);
  }


  @Override
  public void decrCounter(String counter) {
    decrCounter(MetricsService.class, counter);
  }


  @Override
  public void incrCounter(Class<?> cls, String counter) {
    metricRegistry.counter(MetricRegistry.name(cls, counter)).inc();
  }


  @Override
  public void decrCounter(Class<?> cls, String counter) {
    metricRegistry.counter(MetricRegistry.name(cls, counter)).dec();
  }


  @Override
  public long getCounter(String counter) {
    return metricRegistry.counter(MetricRegistry.name(MetricsService.class, counter)).getCount();
  }


  @Override
  public long getCounter(Class<?> cls, String counter) {
    return metricRegistry.counter(MetricRegistry.name(cls, counter)).getCount();
  }


  @Override
  public long getQueuedQueries() {
    return queuedQueries.getCount();
  }

  @Override
  public long getRunningQueries() {
    return runningQueries.getCount();
  }


  @Override
  public long getFinishedQueries() {
    return finishedQueries.getCount();
  }


  @Override
  public long getTotalQueuedQueries() {
    return totalQueuedQueries.getCount();
  }


  @Override
  public long getTotalFinishedQueries() {
    return totalFinishedQueries.getCount();
  }


  @Override
  public long getTotalCancelledQueries() {
    return totalCancelledQueries.getCount();
  }


  @Override
  public long getTotalFailedQueries() {
    return totalFailedQueries.getCount();
  }


  @Override
  public void publishReport() {
    if (reporters != null) {
      for (ScheduledReporter reporter : reporters) {
        reporter.report();
      }
    }
  }

  @Override
  public long getTotalSuccessfulQueries() {
    return totalSuccessfulQueries.getCount();
  }
}
