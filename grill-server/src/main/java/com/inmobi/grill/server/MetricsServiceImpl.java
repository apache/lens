package com.inmobi.grill.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;
import org.apache.log4j.Logger;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.server.api.events.AsyncEventListener;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.api.metrics.MetricsService;
import com.inmobi.grill.server.api.query.QueryExecutionService;
import com.inmobi.grill.server.api.query.StatusChange;

public class MetricsServiceImpl extends AbstractService implements MetricsService {
  public static final String METRICS_SVC_NAME = "metrics";
  public static final Logger LOG = Logger.getLogger(MetricsService.class);
  private AsyncEventListener<StatusChange> queryStatusListener;
  private MetricRegistry metricRegistry;
  private List<ScheduledReporter> reporters;
  
  
  private Counter queuedQueries;
  private Counter runningQueries;
  private Counter finishedQueries;
  private Counter totalQueuedQueries;
  private Counter totalSuccessQueries;
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
      // One of the end states could be previous values if query is closed
      case CANCELED:
        totalCancelledQueries.inc();
        totalFinishedQueries.inc();
        break;
      case FAILED:
        totalFailedQueries.inc();
        totalFinishedQueries.inc();
        break;
      case SUCCESSFUL:
        finishedQueries.inc();
        totalFinishedQueries.inc();
        totalSuccessQueries.inc();
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
  

  @Override
  public synchronized void init(HiveConf hiveConf) {
    queryStatusListener = new AsyncQueryStatusListener();
    GrillEventService eventService = 
        (GrillEventService) GrillServices.get().getService(GrillEventService.NAME);
    eventService.addListenerForType(queryStatusListener, StatusChange.class);
    metricRegistry = new MetricRegistry();
    initCounters();
    
    reporters = new ArrayList<ScheduledReporter>();
    // Start console reporter
    ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    reporter.start(1, TimeUnit.MINUTES);
    
    // TODO Add ganglia reporter
    
    reporters.add(reporter);
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
    
    totalSuccessQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, 
        "total-success-queries"));
    
    finishedQueries = 
        metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, FINISHED_QUERIES));
    totalFinishedQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, 
        "total-" + FINISHED_QUERIES));
    
    totalFailedQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, 
        "total-" + FAILED_QUERIES));
    
    totalCancelledQueries = metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, 
        "total-" + CANCELLED_QUERIES));
  }

  @Override
  public synchronized void start() {
    super.start();
  }
  
  @Override
  public synchronized void stop() {
    // unregister 
    GrillEventService eventService = 
        (GrillEventService) GrillServices.get().getService(GrillEventService.NAME);
    eventService.removeListener(queryStatusListener);
    queryStatusListener.stop();
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
  public long getTotalSuccessQueries() {
    return totalSuccessQueries.getCount();
  }

}
