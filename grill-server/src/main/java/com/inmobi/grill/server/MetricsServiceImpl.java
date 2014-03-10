package com.inmobi.grill.server;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;
import org.apache.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
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
  
  private Counter queuedQueries;
  private Counter runningQueries;
  private Counter finishedQueries;
  private Counter acceptedQueries;
  private Counter cancelledQueries;
  private Counter failedQueries;
  
  
  public class AsyncQueryStatusListener extends AsyncEventListener<StatusChange> {
    @Override
    public void process(StatusChange event) {
      processCurrentStatus(event.getCurrentValue());
      processPrevStatus(event.getPreviousValue());
    }

    protected void processPrevStatus(Status previousValue) {
      switch (previousValue) {
      case LAUNCHED:
        acceptedQueries.dec(); break;
      case QUEUED:
        queuedQueries.dec(); break;
      case RUNNING:
        runningQueries.dec(); break;
      // One of the end states could be previous values if query is closed
      case CANCELED:
        cancelledQueries.dec(); break;
      case FAILED:
        failedQueries.dec(); break;
      case SUCCESSFUL:
        finishedQueries.dec(); break;
      default:
        break;
      }
    }
    

    protected void processCurrentStatus(Status currentValue) {
      switch(currentValue) {
      case LAUNCHED:
        acceptedQueries.inc(); break;
      case QUEUED:
        queuedQueries.inc(); break;
      case RUNNING:
        runningQueries.inc(); break;
      // One of the end states could be previous values if query is closed
      case CANCELED:
        cancelledQueries.inc(); break;
      case FAILED:
        failedQueries.inc(); break;
      case SUCCESSFUL:
        finishedQueries.inc(); break;
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
    LOG.info("Started metrics service");
    super.init(hiveConf);
  }
  
  protected void initCounters() {
    queuedQueries = 
        metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "queued-queries"));
    runningQueries = 
        metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "running-queries"));
    finishedQueries = 
        metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "finished-queries"));
    acceptedQueries = 
        metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "accepted-queries"));
    failedQueries = 
        metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "failed-queries"));
    cancelledQueries = 
        metricRegistry.counter(MetricRegistry.name(QueryExecutionService.class, "cancelled-queries"));
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

}
