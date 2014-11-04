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
package org.apache.lens.server.query;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;

import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;
import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.LensPreparedQuery;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryHandleWithResultSet;
import org.apache.lens.api.query.QueryPlan;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.api.query.QueryResult;
import org.apache.lens.api.query.QueryResultSetMetadata;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.query.SubmitOp;
import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.driver.cube.CubeDriver;
import org.apache.lens.driver.cube.RewriteUtil;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.server.LensService;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.metrics.MetricsService;
import org.apache.lens.server.api.query.*;
import org.apache.lens.server.session.LensSessionImpl;
import org.apache.lens.server.stats.StatisticsService;
import org.apache.lens.server.util.UtilityMethods;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.map.module.SimpleModule;

/**
 * The Class QueryExecutionServiceImpl.
 */
public class QueryExecutionServiceImpl extends LensService implements QueryExecutionService {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(QueryExecutionServiceImpl.class);

  /** The Constant PREPARED_QUERIES_COUNTER. */
  public static final String PREPARED_QUERIES_COUNTER = "prepared-queries";

  /** The Constant QUERY_SUBMITTER_COUNTER. */
  public static final String QUERY_SUBMITTER_COUNTER = "query-submitter-errors";

  /** The Constant STATUS_UPDATE_COUNTER. */
  public static final String STATUS_UPDATE_COUNTER = "status-update-errors";

  /** The Constant QUERY_PURGER_COUNTER. */
  public static final String QUERY_PURGER_COUNTER = "query-purger-errors";

  /** The Constant PREPARED_QUERY_PURGER_COUNTER. */
  public static final String PREPARED_QUERY_PURGER_COUNTER = "prepared-query-purger-errors";

  /** The millis in week. */
  private static long millisInWeek = 7 * 24 * 60 * 60 * 1000;

  /** The Constant NAME. */
  public static final String NAME = "query";

  /** The Constant mapper. */
  private static final ObjectMapper mapper = new ObjectMapper();

  /** The accepted queries. */
  private PriorityBlockingQueue<QueryContext> acceptedQueries = new PriorityBlockingQueue<QueryContext>();

  /** The launched queries. */
  private List<QueryContext> launchedQueries = new ArrayList<QueryContext>();

  /** The finished queries. */
  private DelayQueue<FinishedQuery> finishedQueries = new DelayQueue<FinishedQuery>();

  /** The prepared query queue. */
  private DelayQueue<PreparedQueryContext> preparedQueryQueue = new DelayQueue<PreparedQueryContext>();

  /** The prepared queries. */
  private Map<QueryPrepareHandle, PreparedQueryContext> preparedQueries = new HashMap<QueryPrepareHandle, PreparedQueryContext>();

  /** The all queries. */
  private ConcurrentMap<QueryHandle, QueryContext> allQueries = new ConcurrentHashMap<QueryHandle, QueryContext>();

  /** The conf. */
  private Configuration conf;

  /** The query submitter runnable. */
  private final QuerySubmitter querySubmitterRunnable = new QuerySubmitter();

  /** The query submitter. */
  protected final Thread querySubmitter = new Thread(querySubmitterRunnable, "QuerySubmitter");

  /** The status poller. */
  private final Thread statusPoller = new Thread(new StatusPoller(), "StatusPoller");

  /** The query purger. */
  private final Thread queryPurger = new Thread(new QueryPurger(), "QueryPurger");

  /** The prepare query purger. */
  private final Thread prepareQueryPurger = new Thread(new PreparedQueryPurger(), "PrepareQueryPurger");

  /** The query acceptors. */
  private List<QueryAcceptor> queryAcceptors = new ArrayList<QueryAcceptor>();

  /** The drivers. */
  private final Map<String, LensDriver> drivers = new HashMap<String, LensDriver>();

  /** The driver selector. */
  private DriverSelector driverSelector;

  /** The result sets. */
  private Map<QueryHandle, LensResultSet> resultSets = new HashMap<QueryHandle, LensResultSet>();

  /** The event service. */
  private LensEventService eventService;

  /** The metrics service. */
  private MetricsService metricsService;

  /** The statistics service. */
  private StatisticsService statisticsService;

  /** The max finished queries. */
  private int maxFinishedQueries;

  /** The lens server dao. */
  LensServerDAO lensServerDao;

  /** The driver event listener. */
  final LensEventListener<DriverEvent> driverEventListener = new LensEventListener<DriverEvent>() {
    @Override
    public void onEvent(DriverEvent event) {
      // Need to restore session only in case of hive driver
      if (event instanceof DriverSessionStarted) {
        LOG.info("New driver event by driver " + event.getDriver());
        handleDriverSessionStart(event);
      }
    }
  };

  /**
   * Instantiates a new query execution service impl.
   *
   * @param cliService
   *          the cli service
   * @throws LensException
   *           the lens exception
   */
  public QueryExecutionServiceImpl(CLIService cliService) throws LensException {
    super(NAME, cliService);
  }

  /**
   * Initialize query acceptors and listeners.
   */
  private void initializeQueryAcceptorsAndListeners() {
    if (conf.getBoolean(LensConfConstants.QUERY_STATE_LOGGER_ENABLED, true)) {
      getEventService().addListenerForType(new QueryStatusLogger(), StatusChange.class);
      LOG.info("Registered query state logger");
    }
    // Add result formatter
    getEventService().addListenerForType(new ResultFormatter(this), QueryExecuted.class);
    getEventService().addListenerForType(new QueryExecutionStatisticsGenerator(this, getEventService()),
        QueryEnded.class);
    getEventService().addListenerForType(new QueryEndNotifier(this, getCliService().getHiveConf()), QueryEnded.class);
    LOG.info("Registered query result formatter");
  }

  /**
   * Load drivers and selector.
   *
   * @throws LensException
   *           the lens exception
   */
  private void loadDriversAndSelector() throws LensException {
    conf.get(LensConfConstants.DRIVER_CLASSES);
    String[] driverClasses = conf.getStrings(LensConfConstants.DRIVER_CLASSES);
    if (driverClasses != null) {
      for (String driverClass : driverClasses) {
        try {
          Class<?> clazz = Class.forName(driverClass);
          LensDriver driver = (LensDriver) clazz.newInstance();
          driver.configure(conf);

          if (driver instanceof HiveDriver) {
            driver.registerDriverEventListener(driverEventListener);
          }

          drivers.put(driverClass, driver);
          LOG.info("Driver for " + driverClass + " is loaded");
        } catch (Exception e) {
          LOG.warn("Could not load the driver:" + driverClass, e);
          throw new LensException("Could not load driver " + driverClass, e);
        }
      }
    } else {
      throw new LensException("No drivers specified");
    }
    driverSelector = new CubeDriver.MinQueryCostSelector();
  }

  protected LensEventService getEventService() {
    if (eventService == null) {
      eventService = (LensEventService) LensServices.get().getService(LensEventService.NAME);
      if (eventService == null) {
        throw new NullPointerException("Could not get event service");
      }
    }
    return eventService;
  }

  private synchronized MetricsService getMetrics() {
    if (metricsService == null) {
      metricsService = (MetricsService) LensServices.get().getService(MetricsService.NAME);
      if (metricsService == null) {
        throw new NullPointerException("Could not get metrics service");
      }
    }
    return metricsService;
  }

  private synchronized StatisticsService getStatisticsService() {
    if (statisticsService == null) {
      statisticsService = (StatisticsService) LensServices.get().getService(StatisticsService.STATS_SVC_NAME);
      if (statisticsService == null) {
        throw new NullPointerException("Could not get statistics service");
      }
    }
    return statisticsService;
  }

  /**
   * Incr counter.
   *
   * @param counter
   *          the counter
   */
  private void incrCounter(String counter) {
    getMetrics().incrCounter(QueryExecutionService.class, counter);
  }

  /**
   * Decr counter.
   *
   * @param counter
   *          the counter
   */
  private void decrCounter(String counter) {
    getMetrics().decrCounter(QueryExecutionService.class, counter);
  }

  /**
   * The Class QueryStatusLogger.
   */
  public static class QueryStatusLogger implements LensEventListener<StatusChange> {

    /** The Constant STATUS_LOG. */
    public static final Log STATUS_LOG = LogFactory.getLog(QueryStatusLogger.class);

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lens.server.api.events.LensEventListener#onEvent(org.apache.lens.server.api.events.LensEvent)
     */
    @Override
    public void onEvent(StatusChange event) throws LensException {
      STATUS_LOG.info(event.toString());
    }
  }

  /**
   * The Class FinishedQuery.
   */
  private class FinishedQuery implements Delayed {

    /** The ctx. */
    private final QueryContext ctx;

    /** The finish time. */
    private final Date finishTime;

    /**
     * Instantiates a new finished query.
     *
     * @param ctx
     *          the ctx
     */
    FinishedQuery(QueryContext ctx) {
      this.ctx = ctx;
      this.finishTime = new Date();
      ctx.setEndTime(this.finishTime.getTime());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Delayed o) {
      return (int) (this.finishTime.getTime() - ((FinishedQuery) o).finishTime.getTime());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Delayed#getDelay(java.util.concurrent.TimeUnit)
     */
    @Override
    public long getDelay(TimeUnit units) {
      int size = finishedQueries.size();
      if (size > maxFinishedQueries) {
        return 0;
      } else {
        return Integer.MAX_VALUE;
      }
    }

    /**
     * @return the finishTime
     */
    public Date getFinishTime() {
      return finishTime;
    }

    /**
     * @return the ctx
     */
    public QueryContext getCtx() {
      return ctx;
    }
  }

  /**
   * The Class QuerySubmitter.
   */
  private class QuerySubmitter implements Runnable {

    /** The paused for test. */
    private boolean pausedForTest = false;

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      LOG.info("Starting QuerySubmitter thread");
      while (!pausedForTest && !stopped && !querySubmitter.isInterrupted()) {
        try {
          QueryContext ctx = acceptedQueries.take();
          synchronized (ctx) {
            if (ctx.getStatus().getStatus().equals(Status.QUEUED)) {
              LOG.info("Launching query:" + ctx.getDriverQuery());
              try {
                // acquire session before any query operation.
                acquire(ctx.getLensSessionIdentifier());
                if (ctx.getSelectedDriver() == null) {
                  rewriteAndSelect(ctx);
                } else {
                  LOG.info("Submitting to already selected driver");
                }
                ctx.getSelectedDriver().executeAsync(ctx);
              } catch (Exception e) {
                LOG.error("Error launching query " + ctx.getQueryHandle(), e);
                String reason = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                setFailedStatus(ctx, "Launching query failed", reason);
                continue;
              } finally {
                release(ctx.getLensSessionIdentifier());
              }
              setLaunchedStatus(ctx);
              LOG.info("Launched query " + ctx.getQueryHandle());
            }
          }
        } catch (InterruptedException e) {
          LOG.info("Query Submitter has been interrupted, exiting");
          return;
        } catch (Exception e) {
          incrCounter(QUERY_SUBMITTER_COUNTER);
          LOG.error("Error in query submitter", e);
        }
      }
      LOG.info("QuerySubmitter exited");
    }

  }

  // used in tests
  /**
   * Pause query submitter.
   */
  public void pauseQuerySubmitter() {
    querySubmitterRunnable.pausedForTest = true;
  }

  /**
   * The Class StatusPoller.
   */
  private class StatusPoller implements Runnable {

    /** The poll interval. */
    long pollInterval = 1000;

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      LOG.info("Starting Status poller thread");
      while (!stopped && !statusPoller.isInterrupted()) {
        try {
          List<QueryContext> launched = new ArrayList<QueryContext>();
          launched.addAll(launchedQueries);
          for (QueryContext ctx : launched) {
            if (stopped || statusPoller.isInterrupted()) {
              return;
            }
            LOG.info("Polling status for " + ctx.getQueryHandle());
            try {
              // session is not required to update status of the query
              // acquire(ctx.getLensSessionIdentifier());
              updateStatus(ctx.getQueryHandle());
            } catch (LensException e) {
              LOG.error("Error updating status ", e);
            } finally {
              // release(ctx.getLensSessionIdentifier());
            }
          }
          Thread.sleep(pollInterval);
        } catch (InterruptedException e) {
          LOG.info("Status poller has been interrupted, exiting");
          return;
        } catch (Exception e) {
          incrCounter(STATUS_UPDATE_COUNTER);
          LOG.error("Error in status poller", e);
        }
      }
      LOG.info("StatusPoller exited");
    }
  }

  /**
   * Sets the failed status.
   *
   * @param ctx
   *          the ctx
   * @param statusMsg
   *          the status msg
   * @param reason
   *          the reason
   * @throws LensException
   *           the lens exception
   */
  void setFailedStatus(QueryContext ctx, String statusMsg, String reason) throws LensException {
    QueryStatus before = ctx.getStatus();
    ctx.setStatus(new QueryStatus(0.0f, QueryStatus.Status.FAILED, statusMsg, false, null, reason));
    updateFinishedQuery(ctx, before);
    fireStatusChangeEvent(ctx, ctx.getStatus(), before);
  }

  private void setLaunchedStatus(QueryContext ctx) throws LensException {
    QueryStatus before = ctx.getStatus();
    ctx.setStatus(new QueryStatus(ctx.getStatus().getProgress(), QueryStatus.Status.LAUNCHED, "launched on the driver",
        false, null, null));
    launchedQueries.add(ctx);
    ctx.setLaunchTime(System.currentTimeMillis());
    fireStatusChangeEvent(ctx, ctx.getStatus(), before);
  }

  /**
   * Sets the cancelled status.
   *
   * @param ctx
   *          the ctx
   * @param statusMsg
   *          the status msg
   * @throws LensException
   *           the lens exception
   */
  private void setCancelledStatus(QueryContext ctx, String statusMsg) throws LensException {
    QueryStatus before = ctx.getStatus();
    ctx.setStatus(new QueryStatus(0.0f, QueryStatus.Status.CANCELED, statusMsg, false, null, null));
    updateFinishedQuery(ctx, before);
    fireStatusChangeEvent(ctx, ctx.getStatus(), before);
  }

  /**
   * Update finished query.
   *
   * @param ctx
   *          the ctx
   * @param before
   *          the before
   */
  private void updateFinishedQuery(QueryContext ctx, QueryStatus before) {
    // before would be null in case of server restart
    if (before != null) {
      if (before.getStatus().equals(Status.QUEUED)) {
        acceptedQueries.remove(ctx);
      } else {
        launchedQueries.remove(ctx);
      }
    }
    finishedQueries.add(new FinishedQuery(ctx));
  }

  void setSuccessState(QueryContext ctx) throws LensException {
    QueryStatus before = ctx.getStatus();
    ctx.setStatus(new QueryStatus(1.0f, QueryStatus.Status.SUCCESSFUL, "Query is successful!", ctx
        .isResultAvailableInDriver(), null, null));
    updateFinishedQuery(ctx, before);
    fireStatusChangeEvent(ctx, ctx.getStatus(), before);
  }

  /**
   * Update status.
   *
   * @param handle
   *          the handle
   * @throws LensException
   *           the lens exception
   */
  private void updateStatus(final QueryHandle handle) throws LensException {
    QueryContext ctx = allQueries.get(handle);
    if (ctx != null) {
      synchronized (ctx) {
        QueryStatus before = ctx.getStatus();
        if (!ctx.getStatus().getStatus().equals(QueryStatus.Status.QUEUED) && !ctx.getDriverStatus().isFinished()
            && !ctx.getStatus().isFinished()) {
          LOG.info("Updating status for " + ctx.getQueryHandle());
          try {
            ctx.getSelectedDriver().updateStatus(ctx);
            ctx.setStatus(ctx.getDriverStatus().toQueryStatus());
          } catch (LensException exc) {
            // Driver gave exception while updating status
            setFailedStatus(ctx, "Status update failed", exc.getMessage());
            LOG.error("Status update failed for " + handle, exc);
          }
          // query is successfully executed by driver and
          // if query result need not persisted, move the query to succeeded state
          if (ctx.getStatus().getStatus().equals(QueryStatus.Status.EXECUTED) && !ctx.isPersistent()) {
            setSuccessState(ctx);
          } else {
            if (ctx.getStatus().isFinished()) {
              updateFinishedQuery(ctx, before);
            }
            fireStatusChangeEvent(ctx, ctx.getStatus(), before);
          }
        }
      }
    }
  }

  /**
   * New status change event.
   *
   * @param ctx
   *          the ctx
   * @param prevState
   *          the prev state
   * @param currState
   *          the curr state
   * @return the status change
   */
  private StatusChange newStatusChangeEvent(QueryContext ctx, QueryStatus.Status prevState, QueryStatus.Status currState) {
    QueryHandle query = ctx.getQueryHandle();
    switch (currState) {
    case CANCELED:
      // TODO: correct username. put who cancelled it, not the submitter. Similar for others
      return new QueryCancelled(ctx.getEndTime(), prevState, currState, query, ctx.getSubmittedUser(), null);
    case CLOSED:
      return new QueryClosed(ctx.getClosedTime(), prevState, currState, query, ctx.getSubmittedUser(), null);
    case FAILED:
      return new QueryFailed(ctx.getEndTime(), prevState, currState, query, ctx.getSubmittedUser(), null);
    case LAUNCHED:
      return new QueryLaunched(ctx.getLaunchTime(), prevState, currState, query);
    case QUEUED:
      return new QueryQueued(ctx.getSubmissionTime(), prevState, currState, query, ctx.getSubmittedUser());
    case RUNNING:
      return new QueryRunning(System.currentTimeMillis() - ctx.getDriverStatus().getDriverStartTime(), prevState,
          currState, query);
    case EXECUTED:
      return new QueryExecuted(ctx.getDriverStatus().getDriverFinishTime(), prevState, currState, query);
    case SUCCESSFUL:
      return new QuerySuccess(ctx.getEndTime(), prevState, currState, query);
    default:
      LOG.warn("Query " + query + " transitioned to " + currState + " state from " + prevState + " state");
      return null;
    }
  }

  /**
   * If query status has changed, fire a specific StatusChange event.
   *
   * @param ctx
   *          the ctx
   * @param current
   *          the current
   * @param before
   *          the before
   */
  private void fireStatusChangeEvent(QueryContext ctx, QueryStatus current, QueryStatus before) {
    if (ctx == null || current == null) {
      return;
    }

    QueryStatus.Status prevState = before.getStatus();
    QueryStatus.Status currentStatus = current.getStatus();
    if (currentStatus.equals(prevState)) {
      // No need to fire event since the state hasn't changed
      return;
    }

    StatusChange event = newStatusChangeEvent(ctx, prevState, currentStatus);
    if (event != null) {
      try {
        getEventService().notifyEvent(event);
      } catch (LensException e) {
        LOG.warn("LensEventService encountered error while handling event: " + event.getEventId(), e);
      }
    }
  }

  /**
   * The Class QueryPurger.
   */
  private class QueryPurger implements Runnable {

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      LOG.info("Starting Query purger thread");
      while (!stopped && !queryPurger.isInterrupted()) {
        FinishedQuery finished = null;
        try {
          finished = finishedQueries.take();
        } catch (InterruptedException e) {
          LOG.info("QueryPurger has been interrupted, exiting");
          return;
        }
        try {
          FinishedLensQuery finishedQuery = new FinishedLensQuery(finished.getCtx());
          if (finished.ctx.getStatus().getStatus() == Status.SUCCESSFUL) {
            if (finished.ctx.getStatus().isResultSetAvailable()) {
              LensResultSet set = getResultset(finished.getCtx().getQueryHandle());
              if (set != null && PersistentResultSet.class.isAssignableFrom(set.getClass())) {
                LensResultSetMetadata metadata = set.getMetadata();
                String outputPath = ((PersistentResultSet) set).getOutputPath();
                int rows = set.size();
                finishedQuery.setMetadataClass(metadata.getClass().getName());
                finishedQuery.setResult(outputPath);
                finishedQuery.setMetadata(mapper.writeValueAsString(metadata));
                finishedQuery.setRows(rows);
              }
            }
          }
          try {
            lensServerDao.insertFinishedQuery(finishedQuery);
            LOG.info("Saved query " + finishedQuery.getHandle() + " to DB");
          } catch (Exception e) {
            LOG.warn("Exception while purging query ", e);
            finishedQueries.add(finished);
            continue;
          }

          synchronized (finished.ctx) {
            finished.ctx.setFinishedQueryPersisted(true);
            try {
              if (finished.getCtx().getSelectedDriver() != null) {
                finished.getCtx().getSelectedDriver().closeQuery(finished.getCtx().getQueryHandle());
              }
            } catch (Exception e) {
              LOG.warn("Exception while closing query with selected driver.", e);
            }
            allQueries.remove(finished.getCtx().getQueryHandle());
            resultSets.remove(finished.getCtx().getQueryHandle());
          }
          fireStatusChangeEvent(finished.getCtx(),
              new QueryStatus(1f, Status.CLOSED, "Query purged", false, null, null), finished.getCtx().getStatus());
          LOG.info("Query purged: " + finished.getCtx().getQueryHandle());
        } catch (LensException e) {
          incrCounter(QUERY_PURGER_COUNTER);
          LOG.error("Error closing  query ", e);
        } catch (Exception e) {
          incrCounter(QUERY_PURGER_COUNTER);
          LOG.error("Error in query purger", e);
        }
      }
      LOG.info("QueryPurger exited");
    }
  }

  /**
   * The Class PreparedQueryPurger.
   */
  private class PreparedQueryPurger implements Runnable {

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      LOG.info("Starting Prepared Query purger thread");
      while (!stopped && !prepareQueryPurger.isInterrupted()) {
        try {
          PreparedQueryContext prepared = preparedQueryQueue.take();
          destroyPreparedQuery(prepared);
          LOG.info("Purged prepared query: " + prepared.getPrepareHandle());
        } catch (LensException e) {
          incrCounter(PREPARED_QUERY_PURGER_COUNTER);
          LOG.error("Error closing prepared query ", e);
        } catch (InterruptedException e) {
          LOG.info("PreparedQueryPurger has been interrupted, exiting");
          return;
        } catch (Exception e) {
          incrCounter(PREPARED_QUERY_PURGER_COUNTER);
          LOG.error("Error in prepared query purger", e);
        }
      }
      LOG.info("PreparedQueryPurger exited");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.CompositeService#init(org.apache.hadoop.hive.conf.HiveConf)
   */
  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    this.conf = hiveConf;
    initializeQueryAcceptorsAndListeners();
    try {
      loadDriversAndSelector();
    } catch (LensException e) {
      throw new IllegalStateException("Could not load drivers");
    }
    maxFinishedQueries = conf.getInt(LensConfConstants.MAX_NUMBER_OF_FINISHED_QUERY,
        LensConfConstants.DEFAULT_FINISHED_QUERIES);
    initalizeFinishedQueryStore(conf);
    LOG.info("Query execution service initialized");
  }

  /**
   * Initalize finished query store.
   *
   * @param conf
   *          the conf
   */
  private void initalizeFinishedQueryStore(Configuration conf) {
    this.lensServerDao = new LensServerDAO();
    this.lensServerDao.init(conf);
    try {
      this.lensServerDao.createFinishedQueriesTable();
    } catch (Exception e) {
      LOG.warn("Unable to create finished query table, query purger will not purge queries", e);
    }
    SimpleModule module = new SimpleModule("HiveColumnModule", new Version(1, 0, 0, null));
    module.addSerializer(ColumnDescriptor.class, new JsonSerializer<ColumnDescriptor>() {
      @Override
      public void serialize(ColumnDescriptor columnDescriptor, JsonGenerator jsonGenerator,
          SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("name", columnDescriptor.getName());
        jsonGenerator.writeStringField("comment", columnDescriptor.getComment());
        jsonGenerator.writeNumberField("position", columnDescriptor.getOrdinalPosition());
        jsonGenerator.writeStringField("type", columnDescriptor.getType().getName());
        jsonGenerator.writeEndObject();
      }
    });
    module.addDeserializer(ColumnDescriptor.class, new JsonDeserializer<ColumnDescriptor>() {
      @Override
      public ColumnDescriptor deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
          throws IOException, JsonProcessingException {
        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);
        org.apache.hive.service.cli.Type t = org.apache.hive.service.cli.Type.getType(node.get("type").asText());
        return new ColumnDescriptor(node.get("name").asText(), node.get("comment").asText(), new TypeDescriptor(t),
            node.get("position").asInt());
      }
    });
    mapper.registerModule(module);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.LensService#prepareStopping()
   */
  public void prepareStopping() {
    super.prepareStopping();
    querySubmitter.interrupt();
    statusPoller.interrupt();
    queryPurger.interrupt();
    prepareQueryPurger.interrupt();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.CompositeService#stop()
   */
  public synchronized void stop() {
    super.stop();
    for (Thread th : new Thread[] { querySubmitter, statusPoller, queryPurger, prepareQueryPurger }) {
      try {
        LOG.debug("Waiting for" + th.getName());
        th.join();
      } catch (InterruptedException e) {
        LOG.error("Error waiting for thread: " + th.getName(), e);
      }
    }
    LOG.info("Query execution service stopped");
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.CompositeService#start()
   */
  public synchronized void start() {
    // recover query configurations from session
    synchronized (allQueries) {
      for (QueryContext ctx : allQueries.values()) {
        try {
          if (sessionMap.containsKey(ctx.getLensSessionIdentifier())) {
            // try setting configuration if the query session is still not closed
            ctx.setConf(getLensConf(getSessionHandle(ctx.getLensSessionIdentifier()), ctx.getQconf()));
          } else {
            ctx.setConf(getLensConf(ctx.getQconf()));
          }
        } catch (LensException e) {
          LOG.error("Could not set query conf");
        }
      }
    }
    super.start();
    querySubmitter.start();
    statusPoller.start();
    queryPurger.start();
    prepareQueryPurger.start();
  }

  /**
   * Rewrite and select.
   *
   * @param ctx
   *          the ctx
   * @throws LensException
   *           the lens exception
   */
  private void rewriteAndSelect(QueryContext ctx) throws LensException {
    Map<LensDriver, String> driverQueries = RewriteUtil.rewriteQuery(ctx.getUserQuery(), drivers.values(),
        ctx.getConf());

    // 2. select driver to run the query
    LensDriver driver = driverSelector.select(drivers.values(), driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  /**
   * Rewrite and select.
   *
   * @param ctx
   *          the ctx
   * @throws LensException
   *           the lens exception
   */
  private void rewriteAndSelect(PreparedQueryContext ctx) throws LensException {
    Map<LensDriver, String> driverQueries = RewriteUtil.rewriteQuery(ctx.getUserQuery(), drivers.values(),
        ctx.getConf());

    // 2. select driver to run the query
    LensDriver driver = driverSelector.select(drivers.values(), driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  /**
   * Accept.
   *
   * @param query
   *          the query
   * @param conf
   *          the conf
   * @param submitOp
   *          the submit op
   * @throws LensException
   *           the lens exception
   */
  private void accept(String query, Configuration conf, SubmitOp submitOp) throws LensException {
    // run through all the query acceptors, and throw Exception if any of them
    // return false
    for (QueryAcceptor acceptor : queryAcceptors) {
      String cause = "";
      String rejectionCause = acceptor.accept(query, conf, submitOp);
      if (rejectionCause != null) {
        getEventService().notifyEvent(new QueryRejected(System.currentTimeMillis(), query, rejectionCause, null));
        throw new LensException("Query not accepted because " + cause);
      }
    }
    getEventService().notifyEvent(new QueryAccepted(System.currentTimeMillis(), null, query, null));
  }

  /**
   * Gets the resultset from dao.
   *
   * @param queryHandle
   *          the query handle
   * @return the resultset from dao
   * @throws LensException
   *           the lens exception
   */
  private LensResultSet getResultsetFromDAO(QueryHandle queryHandle) throws LensException {
    FinishedLensQuery query = lensServerDao.getQuery(queryHandle.toString());
    if (query != null) {
      if (query.getResult() == null) {
        throw new NotFoundException("InMemory Query result purged " + queryHandle);
      }
      try {
        Class<LensResultSetMetadata> mdKlass = (Class<LensResultSetMetadata>) Class.forName(query.getMetadataClass());
        return new LensPersistentResult(mapper.readValue(query.getMetadata(), mdKlass), query.getResult(),
            query.getRows());
      } catch (Exception e) {
        throw new LensException(e);
      }
    }
    throw new NotFoundException("Query not found: " + queryHandle);
  }

  /**
   * Gets the resultset.
   *
   * @param queryHandle
   *          the query handle
   * @return the resultset
   * @throws LensException
   *           the lens exception
   */
  private LensResultSet getResultset(QueryHandle queryHandle) throws LensException {
    QueryContext ctx = allQueries.get(queryHandle);
    if (ctx == null) {
      return getResultsetFromDAO(queryHandle);
    } else {
      synchronized (ctx) {
        if (ctx.isFinishedQueryPersisted()) {
          return getResultsetFromDAO(queryHandle);
        }
        LensResultSet resultSet = resultSets.get(queryHandle);
        if (resultSet == null) {
          if (ctx.isPersistent() && ctx.getQueryOutputFormatter() != null) {
            resultSets
                .put(queryHandle, new LensPersistentResult(ctx.getQueryOutputFormatter().getMetadata(), ctx
                    .getQueryOutputFormatter().getFinalOutputPath().toString(), ctx.getQueryOutputFormatter()
                    .getNumRows()));
          } else if (allQueries.get(queryHandle).isResultAvailableInDriver()) {
            resultSet = allQueries.get(queryHandle).getSelectedDriver().fetchResultSet(allQueries.get(queryHandle));
            resultSets.put(queryHandle, resultSet);
          } else {
            throw new NotFoundException("Result set not available for query:" + queryHandle);
          }
        }
      }
      return resultSets.get(queryHandle);
    }
  }

  /**
   * Gets the driver resultset.
   *
   * @param queryHandle
   *          the query handle
   * @return the driver resultset
   * @throws LensException
   *           the lens exception
   */
  LensResultSet getDriverResultset(QueryHandle queryHandle) throws LensException {
    return allQueries.get(queryHandle).getSelectedDriver().fetchResultSet(allQueries.get(queryHandle));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#prepare(org.apache.lens.api.LensSessionHandle,
   * java.lang.String, org.apache.lens.api.LensConf, java.lang.String)
   */
  @Override
  public QueryPrepareHandle prepare(LensSessionHandle sessionHandle, String query, LensConf lensConf, String queryName)
      throws LensException {
    try {
      acquire(sessionHandle);
      PreparedQueryContext prepared = prepareQuery(sessionHandle, query, lensConf, SubmitOp.PREPARE);
      prepared.setQueryName(queryName);
      prepared.getSelectedDriver().prepare(prepared);
      return prepared.getPrepareHandle();
    } finally {
      release(sessionHandle);
    }
  }

  /**
   * Prepare query.
   *
   * @param sessionHandle
   *          the session handle
   * @param query
   *          the query
   * @param lensConf
   *          the lens conf
   * @param op
   *          the op
   * @return the prepared query context
   * @throws LensException
   *           the lens exception
   */
  private PreparedQueryContext prepareQuery(LensSessionHandle sessionHandle, String query, LensConf lensConf,
      SubmitOp op) throws LensException {
    Configuration conf = getLensConf(sessionHandle, lensConf);
    accept(query, conf, op);
    PreparedQueryContext prepared = new PreparedQueryContext(query, getSession(sessionHandle).getLoggedInUser(), conf,
        lensConf);
    rewriteAndSelect(prepared);
    preparedQueries.put(prepared.getPrepareHandle(), prepared);
    preparedQueryQueue.add(prepared);
    incrCounter(PREPARED_QUERIES_COUNTER);
    return prepared;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lens.server.api.query.QueryExecutionService#explainAndPrepare(org.apache.lens.api.LensSessionHandle,
   * java.lang.String, org.apache.lens.api.LensConf, java.lang.String)
   */
  @Override
  public QueryPlan explainAndPrepare(LensSessionHandle sessionHandle, String query, LensConf lensConf, String queryName)
      throws LensException {
    try {
      LOG.info("ExlainAndPrepare: " + sessionHandle.toString() + " query: " + query);
      acquire(sessionHandle);
      PreparedQueryContext prepared = prepareQuery(sessionHandle, query, lensConf, SubmitOp.EXPLAIN_AND_PREPARE);
      prepared.setQueryName(queryName);
      QueryPlan plan = prepared.getSelectedDriver().explainAndPrepare(prepared).toQueryPlan();
      plan.setPrepareHandle(prepared.getPrepareHandle());
      return plan;
    } catch (LensException e) {
      LOG.info("Explain and prepare failed", e);
      QueryPlan plan;
      if (e.getCause() != null && e.getCause().getMessage() != null) {
        plan = new QueryPlan(true, e.getCause().getMessage());
      } else {
        plan = new QueryPlan(true, e.getMessage());
      }
      return plan;
    } catch (UnsupportedEncodingException e) {
      throw new LensException(e);
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lens.server.api.query.QueryExecutionService#executePrepareAsync(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryPrepareHandle, org.apache.lens.api.LensConf, java.lang.String)
   */
  @Override
  public QueryHandle executePrepareAsync(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle,
      LensConf conf, String queryName) throws LensException {
    try {
      LOG.info("ExecutePrepareAsync: " + sessionHandle.toString() + " query:" + prepareHandle.getPrepareHandleId());
      acquire(sessionHandle);
      PreparedQueryContext pctx = getPreparedQueryContext(sessionHandle, prepareHandle);
      Configuration qconf = getLensConf(sessionHandle, conf);
      accept(pctx.getUserQuery(), qconf, SubmitOp.EXECUTE);
      QueryContext ctx = createContext(pctx, getSession(sessionHandle).getLoggedInUser(), conf, qconf);
      if (StringUtils.isNotBlank(queryName)) {
        // Override previously set query name
        ctx.setQueryName(queryName);
      } else {
        ctx.setQueryName(pctx.getQueryName());
      }
      return executeAsyncInternal(sessionHandle, ctx);
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#executePrepare(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryPrepareHandle, long, org.apache.lens.api.LensConf, java.lang.String)
   */
  @Override
  public QueryHandleWithResultSet executePrepare(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle,
      long timeoutMillis, LensConf conf, String queryName) throws LensException {
    try {
      LOG.info("ExecutePrepare: " + sessionHandle.toString() + " query:" + prepareHandle.getPrepareHandleId()
          + " timeout:" + timeoutMillis);
      acquire(sessionHandle);
      PreparedQueryContext pctx = getPreparedQueryContext(sessionHandle, prepareHandle);
      Configuration qconf = getLensConf(sessionHandle, conf);
      QueryContext ctx = createContext(pctx, getSession(sessionHandle).getLoggedInUser(), conf, qconf);
      if (StringUtils.isNotBlank(queryName)) {
        // Override previously set query name
        ctx.setQueryName(queryName);
      } else {
        ctx.setQueryName(pctx.getQueryName());
      }
      return executeTimeoutInternal(sessionHandle, ctx, timeoutMillis, qconf);
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#executeAsync(org.apache.lens.api.LensSessionHandle,
   * java.lang.String, org.apache.lens.api.LensConf, java.lang.String)
   */
  @Override
  public QueryHandle executeAsync(LensSessionHandle sessionHandle, String query, LensConf conf, String queryName)
      throws LensException {
    try {
      LOG.info("ExecuteAsync: " + sessionHandle.toString() + " query: " + query);
      acquire(sessionHandle);
      Configuration qconf = getLensConf(sessionHandle, conf);
      accept(query, qconf, SubmitOp.EXECUTE);
      QueryContext ctx = createContext(query, getSession(sessionHandle).getLoggedInUser(), conf, qconf);
      ctx.setQueryName(queryName);
      return executeAsyncInternal(sessionHandle, ctx);
    } finally {
      release(sessionHandle);
    }
  }

  /**
   * Creates the context.
   *
   * @param query
   *          the query
   * @param userName
   *          the user name
   * @param conf
   *          the conf
   * @param qconf
   *          the qconf
   * @return the query context
   * @throws LensException
   *           the lens exception
   */
  protected QueryContext createContext(String query, String userName, LensConf conf, Configuration qconf)
      throws LensException {
    QueryContext ctx = new QueryContext(query, userName, conf, qconf);
    return ctx;
  }

  /**
   * Creates the context.
   *
   * @param pctx
   *          the pctx
   * @param userName
   *          the user name
   * @param conf
   *          the conf
   * @param qconf
   *          the qconf
   * @return the query context
   * @throws LensException
   *           the lens exception
   */
  protected QueryContext createContext(PreparedQueryContext pctx, String userName, LensConf conf, Configuration qconf)
      throws LensException {
    QueryContext ctx = new QueryContext(pctx, userName, conf, qconf);
    return ctx;
  }

  /**
   * Execute async internal.
   *
   * @param sessionHandle
   *          the session handle
   * @param ctx
   *          the ctx
   * @return the query handle
   * @throws LensException
   *           the lens exception
   */
  private QueryHandle executeAsyncInternal(LensSessionHandle sessionHandle, QueryContext ctx) throws LensException {
    ctx.setLensSessionIdentifier(sessionHandle.getPublicId().toString());
    QueryStatus before = ctx.getStatus();
    ctx.setStatus(new QueryStatus(0.0, QueryStatus.Status.QUEUED, "Query is queued", false, null, null));
    acceptedQueries.add(ctx);
    allQueries.put(ctx.getQueryHandle(), ctx);
    fireStatusChangeEvent(ctx, ctx.getStatus(), before);
    LOG.info("Returning handle " + ctx.getQueryHandle().getHandleId());
    return ctx.getQueryHandle();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#updateQueryConf(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryHandle, org.apache.lens.api.LensConf)
   */
  @Override
  public boolean updateQueryConf(LensSessionHandle sessionHandle, QueryHandle queryHandle, LensConf newconf)
      throws LensException {
    try {
      LOG.info("UpdateQueryConf:" + sessionHandle.toString() + " query: " + queryHandle);
      acquire(sessionHandle);
      QueryContext ctx = getQueryContext(sessionHandle, queryHandle);
      if (ctx != null && ctx.getStatus().getStatus() == QueryStatus.Status.QUEUED) {
        ctx.updateConf(newconf.getProperties());
        // TODO COnf changed event tobe raised
        return true;
      } else {
        return false;
      }
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#updateQueryConf(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryPrepareHandle, org.apache.lens.api.LensConf)
   */
  @Override
  public boolean updateQueryConf(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle, LensConf newconf)
      throws LensException {
    try {
      LOG.info("UpdatePreparedQueryConf:" + sessionHandle.toString() + " query: " + prepareHandle);
      acquire(sessionHandle);
      PreparedQueryContext ctx = getPreparedQueryContext(sessionHandle, prepareHandle);
      ctx.updateConf(newconf.getProperties());
      return true;
    } finally {
      release(sessionHandle);
    }
  }

  /**
   * Gets the query context.
   *
   * @param sessionHandle
   *          the session handle
   * @param queryHandle
   *          the query handle
   * @return the query context
   * @throws LensException
   *           the lens exception
   */
  private QueryContext getQueryContext(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException {
    try {
      acquire(sessionHandle);
      QueryContext ctx = allQueries.get(queryHandle);
      if (ctx == null) {
        FinishedLensQuery query = lensServerDao.getQuery(queryHandle.toString());
        if (query == null) {
          throw new NotFoundException("Query not found " + queryHandle);
        }
        QueryContext finishedCtx = new QueryContext(query.getUserQuery(), query.getSubmitter(), conf,
            query.getSubmissionTime());
        finishedCtx.setQueryHandle(queryHandle);
        finishedCtx.setEndTime(query.getEndTime());
        finishedCtx.setStatusSkippingTransitionTest(new QueryStatus(0.0, QueryStatus.Status.valueOf(query.getStatus()),
            query.getErrorMessage() == null ? "" : query.getErrorMessage(), query.getResult() != null, null, null));
        finishedCtx.getDriverStatus().setDriverStartTime(query.getDriverStartTime());
        finishedCtx.getDriverStatus().setDriverFinishTime(query.getDriverEndTime());
        finishedCtx.setResultSetPath(query.getResult());
        finishedCtx.setQueryName(query.getQueryName());
        return finishedCtx;
      }
      updateStatus(queryHandle);
      return ctx;
    } finally {
      release(sessionHandle);
    }
  }

  /**
   * Gets the query context.
   *
   * @param queryHandle
   *          the query handle
   * @return the query context
   */
  QueryContext getQueryContext(QueryHandle queryHandle) {
    return allQueries.get(queryHandle);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#getQuery(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public LensQuery getQuery(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException {
    return getQueryContext(sessionHandle, queryHandle).toLensQuery();
  }

  /**
   * Gets the prepared query context.
   *
   * @param sessionHandle
   *          the session handle
   * @param prepareHandle
   *          the prepare handle
   * @return the prepared query context
   * @throws LensException
   *           the lens exception
   */
  private PreparedQueryContext getPreparedQueryContext(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle)
      throws LensException {
    try {
      acquire(sessionHandle);
      PreparedQueryContext ctx = preparedQueries.get(prepareHandle);
      if (ctx == null) {
        throw new NotFoundException("Prepared query not found " + prepareHandle);
      }
      return ctx;
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#getPreparedQuery(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryPrepareHandle)
   */
  @Override
  public LensPreparedQuery getPreparedQuery(LensSessionHandle sessionHandle, QueryPrepareHandle prepareHandle)
      throws LensException {
    return getPreparedQueryContext(sessionHandle, prepareHandle).toPreparedQuery();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#execute(org.apache.lens.api.LensSessionHandle,
   * java.lang.String, long, org.apache.lens.api.LensConf, java.lang.String)
   */
  @Override
  public QueryHandleWithResultSet execute(LensSessionHandle sessionHandle, String query, long timeoutMillis,
      LensConf conf, String queryName) throws LensException {
    try {
      LOG.info("Blocking execute " + sessionHandle.toString() + " query: " + query + " timeout: " + timeoutMillis);
      acquire(sessionHandle);
      Configuration qconf = getLensConf(sessionHandle, conf);
      accept(query, qconf, SubmitOp.EXECUTE);
      QueryContext ctx = createContext(query, getSession(sessionHandle).getLoggedInUser(), conf, qconf);
      ctx.setQueryName(queryName);
      return executeTimeoutInternal(sessionHandle, ctx, timeoutMillis, qconf);
    } finally {
      release(sessionHandle);
    }
  }

  /**
   * Execute timeout internal.
   *
   * @param sessionHandle
   *          the session handle
   * @param ctx
   *          the ctx
   * @param timeoutMillis
   *          the timeout millis
   * @param conf
   *          the conf
   * @return the query handle with result set
   * @throws LensException
   *           the lens exception
   */
  private QueryHandleWithResultSet executeTimeoutInternal(LensSessionHandle sessionHandle, QueryContext ctx,
      long timeoutMillis, Configuration conf) throws LensException {
    QueryHandle handle = executeAsyncInternal(sessionHandle, ctx);
    QueryHandleWithResultSet result = new QueryHandleWithResultSet(handle);
    // getQueryContext calls updateStatus, which fires query events if there's a change in status
    while (getQueryContext(sessionHandle, handle).getStatus().getStatus().equals(QueryStatus.Status.QUEUED)) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    QueryCompletionListener listener = new QueryCompletionListenerImpl(handle);
    getQueryContext(sessionHandle, handle).getSelectedDriver().registerForCompletionNotification(handle, timeoutMillis,
        listener);
    try {
      synchronized (listener) {
        listener.wait(timeoutMillis);
      }
    } catch (InterruptedException e) {
      LOG.info("Waiting thread interrupted");
    }
    if (getQueryContext(sessionHandle, handle).getStatus().isFinished()) {
      result.setResult(getResultset(handle).toQueryResult());
    }
    return result;

  }

  /**
   * The Class QueryCompletionListenerImpl.
   */
  class QueryCompletionListenerImpl implements QueryCompletionListener {

    /** The succeeded. */
    boolean succeeded = false;

    /** The handle. */
    QueryHandle handle;

    /**
     * Instantiates a new query completion listener impl.
     *
     * @param handle
     *          the handle
     */
    QueryCompletionListenerImpl(QueryHandle handle) {
      this.handle = handle;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.lens.server.api.driver.QueryCompletionListener#onCompletion(org.apache.lens.api.query.QueryHandle)
     */
    @Override
    public void onCompletion(QueryHandle handle) {
      synchronized (this) {
        succeeded = true;
        LOG.info("Query " + handle + " with time out succeeded");
        this.notify();
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lens.server.api.driver.QueryCompletionListener#onError(org.apache.lens.api.query.QueryHandle,
     * java.lang.String)
     */
    @Override
    public void onError(QueryHandle handle, String error) {
      synchronized (this) {
        succeeded = false;
        LOG.info("Query " + handle + " with time out failed");
        this.notify();
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lens.server.api.query.QueryExecutionService#getResultSetMetadata(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public QueryResultSetMetadata getResultSetMetadata(LensSessionHandle sessionHandle, QueryHandle queryHandle)
      throws LensException {
    try {
      LOG.info("GetResultSetMetadata: " + sessionHandle.toString() + " query: " + queryHandle);
      acquire(sessionHandle);
      LensResultSet resultSet = getResultset(queryHandle);
      if (resultSet != null) {
        return resultSet.getMetadata().toQueryResultSetMetadata();
      } else {
        throw new NotFoundException("Resultset metadata not found for query: (" + sessionHandle + ", " + queryHandle
            + ")");
      }
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#fetchResultSet(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryHandle, long, int)
   */
  @Override
  public QueryResult fetchResultSet(LensSessionHandle sessionHandle, QueryHandle queryHandle, long startIndex,
      int fetchSize) throws LensException {
    try {
      LOG.info("FetchResultSet:" + sessionHandle.toString() + " query:" + queryHandle);
      acquire(sessionHandle);
      return getResultset(queryHandle).toQueryResult();
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#closeResultSet(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public void closeResultSet(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException {
    try {
      LOG.info("CloseResultSet:" + sessionHandle.toString() + " query: " + queryHandle);
      acquire(sessionHandle);
      resultSets.remove(queryHandle);
      // Ask driver to close result set
      getQueryContext(queryHandle).getSelectedDriver().closeResultSet(queryHandle);
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#cancelQuery(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public boolean cancelQuery(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException {
    try {
      LOG.info("CancelQuery: " + sessionHandle.toString() + " query:" + queryHandle);
      acquire(sessionHandle);
      QueryContext ctx = getQueryContext(sessionHandle, queryHandle);
      if (ctx.getStatus().isFinished()) {
        return false;
      }
      synchronized (ctx) {
        if (ctx.getStatus().getStatus().equals(QueryStatus.Status.LAUNCHED)
            || ctx.getStatus().getStatus().equals(QueryStatus.Status.RUNNING)) {
          boolean ret = ctx.getSelectedDriver().cancelQuery(queryHandle);
          if (!ret) {
            return false;
          }
          setCancelledStatus(ctx, "Query is cancelled");
          return true;
        }
      }
    } finally {
      release(sessionHandle);
    }
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#getAllQueries(org.apache.lens.api.LensSessionHandle,
   * java.lang.String, java.lang.String, java.lang.String, long, long)
   */
  @Override
  public List<QueryHandle> getAllQueries(LensSessionHandle sessionHandle, String state, String userName,
      String queryName, long fromDate, long toDate) throws LensException {
    validateTimeRange(fromDate, toDate);
    userName = UtilityMethods.removeDomain(userName);
    try {
      acquire(sessionHandle);
      Status status = null;
      try {
        status = StringUtils.isBlank(state) ? null : Status.valueOf(state);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Bad state argument passed, possible" + " values are " + Status.values(), e);
      }
      boolean filterByStatus = status != null;
      queryName = queryName.toLowerCase();
      boolean filterByQueryName = StringUtils.isNotBlank(queryName);

      if (StringUtils.isBlank(userName)) {
        userName = getSession(sessionHandle).getLoggedInUser();
      }

      List<QueryHandle> all = new ArrayList<QueryHandle>(allQueries.keySet());
      Iterator<QueryHandle> itr = all.iterator();
      while (itr.hasNext()) {
        QueryHandle q = itr.next();
        QueryContext context = allQueries.get(q);
        long querySubmitTime = context.getSubmissionTime();
        if ((filterByStatus && status != context.getStatus().getStatus())
            || (filterByQueryName && !context.getQueryName().toLowerCase().contains(queryName))
            || (!"all".equalsIgnoreCase(userName) && !userName.equalsIgnoreCase(context.getSubmittedUser()))
            || (!(fromDate <= querySubmitTime && querySubmitTime <= toDate))) {
          itr.remove();
        }
      }

      // Unless user wants to get queries in 'non finished' state, get finished queries from DB as well
      if (status == null || status == Status.CANCELED || status == Status.SUCCESSFUL || status == Status.FAILED) {
        if ("all".equalsIgnoreCase(userName)) {
          userName = null;
        }
        List<QueryHandle> persistedQueries = lensServerDao.findFinishedQueries(state, userName, queryName, fromDate,
            toDate);
        if (persistedQueries != null && !persistedQueries.isEmpty()) {
          LOG.info("Adding persisted queries " + persistedQueries.size());
          all.addAll(persistedQueries);
        }
      }

      return all;
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lens.server.api.query.QueryExecutionService#getAllPreparedQueries(org.apache.lens.api.LensSessionHandle,
   * java.lang.String, java.lang.String, long, long)
   */
  @Override
  public List<QueryPrepareHandle> getAllPreparedQueries(LensSessionHandle sessionHandle, String user, String queryName,
      long fromDate, long toDate) throws LensException {
    validateTimeRange(fromDate, toDate);
    user = UtilityMethods.removeDomain(user);
    try {
      acquire(sessionHandle);
      List<QueryPrepareHandle> allPrepared = new ArrayList<QueryPrepareHandle>(preparedQueries.keySet());
      Iterator<QueryPrepareHandle> itr = allPrepared.iterator();
      while (itr.hasNext()) {
        QueryPrepareHandle q = itr.next();
        PreparedQueryContext preparedQueryContext = preparedQueries.get(q);

        if (StringUtils.isNotBlank(user)) {
          if ("all".equalsIgnoreCase(user)) {
            continue;
          } else if (user.equalsIgnoreCase(preparedQueryContext.getPreparedUser())) {
            continue;
          }
        }

        if (StringUtils.isNotBlank(queryName)) {
          if (preparedQueryContext.getQueryName().toLowerCase().contains(queryName.toLowerCase())) {
            continue;
          }
        }
        long queryPrepTime = preparedQueryContext.getPreparedTime().getTime();
        if (fromDate <= queryPrepTime && queryPrepTime <= toDate) {
          continue;
        }

        itr.remove();
      }
      return allPrepared;
    } finally {
      release(sessionHandle);
    }
  }

  /**
   * Validate time range.
   *
   * @param fromDate
   *          the from date
   * @param toDate
   *          the to date
   */
  private void validateTimeRange(long fromDate, long toDate) {
    if (fromDate >= toDate) {
      throw new BadRequestException("Invalid time range: [" + fromDate + ", " + toDate + "]");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#destroyPrepared(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryPrepareHandle)
   */
  @Override
  public boolean destroyPrepared(LensSessionHandle sessionHandle, QueryPrepareHandle prepared) throws LensException {
    try {
      LOG.info("DestroyPrepared: " + sessionHandle.toString() + " query:" + prepared);
      acquire(sessionHandle);
      destroyPreparedQuery(getPreparedQueryContext(sessionHandle, prepared));
      return true;
    } finally {
      release(sessionHandle);
    }
  }

  /**
   * Destroy prepared query.
   *
   * @param ctx
   *          the ctx
   * @throws LensException
   *           the lens exception
   */
  private void destroyPreparedQuery(PreparedQueryContext ctx) throws LensException {
    ctx.getSelectedDriver().closePreparedQuery(ctx.getPrepareHandle());
    preparedQueries.remove(ctx.getPrepareHandle());
    preparedQueryQueue.remove(ctx);
    decrCounter(PREPARED_QUERIES_COUNTER);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#explain(org.apache.lens.api.LensSessionHandle,
   * java.lang.String, org.apache.lens.api.LensConf)
   */
  @Override
  public QueryPlan explain(LensSessionHandle sessionHandle, String query, LensConf lensConf) throws LensException {
    try {
      LOG.info("Explain: " + sessionHandle.toString() + " query:" + query);
      acquire(sessionHandle);
      Configuration qconf = getLensConf(sessionHandle, lensConf);
      accept(query, qconf, SubmitOp.EXPLAIN);
      Map<LensDriver, String> driverQueries = RewriteUtil.rewriteQuery(query, drivers.values(), qconf);
      // select driver to run the query
      LensDriver selectedDriver = driverSelector.select(drivers.values(), driverQueries, conf);
      return selectedDriver.explain(driverQueries.get(selectedDriver), qconf).toQueryPlan();
    } catch (LensException e) {
      QueryPlan plan;
      if (e.getCause() != null && e.getCause().getMessage() != null) {
        plan = new QueryPlan(true, e.getCause().getMessage());
      } else {
        plan = new QueryPlan(true, e.getMessage());
      }
      return plan;
    } catch (UnsupportedEncodingException e) {
      throw new LensException(e);
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.LensService#addResource(org.apache.lens.api.LensSessionHandle, java.lang.String,
   * java.lang.String)
   */
  public void addResource(LensSessionHandle sessionHandle, String type, String path) throws LensException {
    try {
      acquire(sessionHandle);
      for (LensDriver driver : drivers.values()) {
        if (driver instanceof HiveDriver) {
          driver.execute(createAddResourceQuery(sessionHandle, type, path));
        }
      }
    } finally {
      release(sessionHandle);
    }
  }

  /**
   * Creates the add resource query.
   *
   * @param sessionHandle
   *          the session handle
   * @param type
   *          the type
   * @param path
   *          the path
   * @return the query context
   * @throws LensException
   *           the lens exception
   */
  private QueryContext createAddResourceQuery(LensSessionHandle sessionHandle, String type, String path)
      throws LensException {
    String command = "add " + type.toLowerCase() + " " + path;
    LensConf conf = new LensConf();
    conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
    QueryContext addQuery = new QueryContext(command, getSession(sessionHandle).getLoggedInUser(), getLensConf(
        sessionHandle, conf));
    addQuery.setLensSessionIdentifier(sessionHandle.getPublicId().toString());
    return addQuery;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.LensService#deleteResource(org.apache.lens.api.LensSessionHandle, java.lang.String,
   * java.lang.String)
   */
  public void deleteResource(LensSessionHandle sessionHandle, String type, String path) throws LensException {
    try {
      acquire(sessionHandle);
      String command = "delete " + type.toLowerCase() + " " + path;
      for (LensDriver driver : drivers.values()) {
        if (driver instanceof HiveDriver) {
          LensConf conf = new LensConf();
          conf.addProperty(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
          QueryContext addQuery = new QueryContext(command, getSession(sessionHandle).getLoggedInUser(), getLensConf(
              sessionHandle, conf));
          addQuery.setLensSessionIdentifier(sessionHandle.getPublicId().toString());
          driver.execute(addQuery);
        }
      }
    } finally {
      release(sessionHandle);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.LensService#readExternal(java.io.ObjectInput)
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    // Restore drivers
    synchronized (drivers) {
      int numDrivers = in.readInt();
      for (int i = 0; i < numDrivers; i++) {
        String driverClsName = in.readUTF();
        LensDriver driver = drivers.get(driverClsName);
        if (driver == null) {
          // this driver is removed in the current server restart
          // we will create an instance and read its state still.
          try {
            Class<? extends LensDriver> driverCls = (Class<? extends LensDriver>) Class.forName(driverClsName);
            driver = (LensDriver) driverCls.newInstance();
            driver.configure(conf);
          } catch (Exception e) {
            LOG.error("Could not instantiate driver:" + driverClsName);
            throw new IOException(e);
          }
          LOG.info("Driver state for " + driverClsName + " will be ignored");
        }
        driver.readExternal(in);
      }
    }

    // Restore queries
    synchronized (allQueries) {
      int numQueries = in.readInt();

      for (int i = 0; i < numQueries; i++) {
        QueryContext ctx = (QueryContext) in.readObject();
        boolean driverAvailable = in.readBoolean();
        if (driverAvailable) {
          String clsName = in.readUTF();
          ctx.setSelectedDriver(drivers.get(clsName));
        }
        allQueries.put(ctx.getQueryHandle(), ctx);
      }

      // populate the query queues
      for (QueryContext ctx : allQueries.values()) {
        switch (ctx.getStatus().getStatus()) {
        case NEW:
        case QUEUED:
          acceptedQueries.add(ctx);
          break;
        case LAUNCHED:
        case RUNNING:
          launchedQueries.add(ctx);
          break;
        case SUCCESSFUL:
        case FAILED:
        case CANCELED:
          updateFinishedQuery(ctx, null);
          break;
        case CLOSED:
          allQueries.remove(ctx.getQueryHandle());
        }
      }
      LOG.info("Recovered " + allQueries.size() + " queries");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.LensService#writeExternal(java.io.ObjectOutput)
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    super.writeExternal(out);
    // persist all drivers
    synchronized (drivers) {
      out.writeInt(drivers.size());
      for (LensDriver driver : drivers.values()) {
        out.writeUTF(driver.getClass().getName());
        driver.writeExternal(out);
      }
    }
    // persist allQueries
    synchronized (allQueries) {
      out.writeInt(allQueries.size());
      for (QueryContext ctx : allQueries.values()) {
        out.writeObject(ctx);
        boolean isDriverAvailable = (ctx.getSelectedDriver() != null);
        out.writeBoolean(isDriverAvailable);
        if (isDriverAvailable) {
          out.writeUTF(ctx.getSelectedDriver().getClass().getName());
        }
      }
    }
    LOG.info("Persisted " + allQueries.size() + " queries");
  }

  /**
   * Pipe.
   *
   * @param is
   *          the is
   * @param os
   *          the os
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void pipe(InputStream is, OutputStream os) throws IOException {
    int n;
    byte[] buffer = new byte[4096];
    while ((n = is.read(buffer)) > -1) {
      os.write(buffer, 0, n);
      os.flush();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.query.QueryExecutionService#getHttpResultSet(org.apache.lens.api.LensSessionHandle,
   * org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public Response getHttpResultSet(LensSessionHandle sessionHandle, QueryHandle queryHandle) throws LensException {
    final QueryContext ctx = getQueryContext(sessionHandle, queryHandle);
    LensResultSet result = getResultset(queryHandle);
    if (result instanceof LensPersistentResult) {
      final Path resultPath = new Path(((PersistentResultSet) result).getOutputPath());
      try {
        FileSystem fs = resultPath.getFileSystem(conf);
        if (fs.isDirectory(resultPath)) {
          throw new NotFoundException("Http result not available for query:" + queryHandle.toString());
        }
      } catch (IOException e) {
        LOG.warn("Unable to get status for Result Directory", e);
        throw new NotFoundException("Http result not available for query:" + queryHandle.toString());
      }
      String resultFSReadUrl = ctx.getConf().get(LensConfConstants.RESULT_FS_READ_URL);
      if (resultFSReadUrl != null) {
        try {
          URI resultReadPath = new URI(resultFSReadUrl + resultPath.toUri().getPath() + "?op=OPEN&user.name="
              + getSession(sessionHandle).getClusterUser());
          return Response.seeOther(resultReadPath)
              .header("content-disposition", "attachment; filename = " + resultPath.getName())
              .type(MediaType.APPLICATION_OCTET_STREAM).build();
        } catch (URISyntaxException e) {
          throw new LensException(e);
        }
      } else {
        StreamingOutput stream = new StreamingOutput() {
          @Override
          public void write(OutputStream os) throws IOException {
            FSDataInputStream fin = null;
            try {
              FileSystem fs = resultPath.getFileSystem(ctx.getConf());
              fin = fs.open(resultPath);
              pipe(fin, os);
            } finally {
              if (fin != null) {
                fin.close();
              }

            }
          }
        };
        return Response.ok(stream).header("content-disposition", "attachment; filename = " + resultPath.getName())
            .type(MediaType.APPLICATION_OCTET_STREAM).build();
      }
    } else {
      throw new NotFoundException("Http result not available for query:" + queryHandle.toString());
    }
  }

  /**
   * Allow drivers to release resources acquired for a session if any.
   *
   * @param sessionHandle
   *          the session handle
   */
  public void closeDriverSessions(LensSessionHandle sessionHandle) {
    for (LensDriver driver : drivers.values()) {
      if (driver instanceof HiveDriver) {
        ((HiveDriver) driver).closeSession(sessionHandle);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.LensService#closeSession(org.apache.lens.api.LensSessionHandle)
   */
  public void closeSession(LensSessionHandle sessionHandle) throws LensException {
    super.closeSession(sessionHandle);
    // Call driver session close in case some one closes sessions directly on query service
    closeDriverSessions(sessionHandle);
  }

  // Used in test code
  Collection<LensDriver> getDrivers() {
    return drivers.values();
  }

  @Override
  public long getQueuedQueriesCount() {
    return acceptedQueries.size();
  }

  @Override
  public long getRunningQueriesCount() {
    return launchedQueries.size();
  }

  @Override
  public long getFinishedQueriesCount() {
    return finishedQueries.size();
  }

  /**
   * Handle driver session start.
   *
   * @param event
   *          the event
   */
  protected void handleDriverSessionStart(DriverEvent event) {
    DriverSessionStarted sessionStarted = (DriverSessionStarted) event;
    if (!(event.getDriver() instanceof HiveDriver)) {
      return;
    }

    HiveDriver hiveDriver = (HiveDriver) event.getDriver();

    String lensSession = sessionStarted.getLensSessionID();
    LensSessionHandle sessionHandle = getSessionHandle(lensSession);
    if (sessionHandle == null) {
      LOG.warn("Lens session went away for sessionid:" + lensSession);
      return;
    }
    try {
      LensSessionImpl session = getSession(sessionHandle);
      acquire(sessionHandle);
      // Add resources for this session
      List<LensSessionImpl.ResourceEntry> resources = session.getLensSessionPersistInfo().getResources();
      if (resources != null && !resources.isEmpty()) {
        for (LensSessionImpl.ResourceEntry resource : resources) {
          LOG.info("Restoring resource " + resource + " for session " + lensSession);
          try {
            // Execute add resource query in blocking mode
            hiveDriver.execute(createAddResourceQuery(sessionHandle, resource.getType(), resource.getLocation()));
            resource.restoredResource();
            LOG.info("Restored resource " + resource + " for session " + lensSession);
          } catch (Exception exc) {
            LOG.error("Unable to add resource " + resource + " for session " + lensSession, exc);
          }
        }
      } else {
        LOG.info("No resources to restore for session " + lensSession);
      }
    } catch (Exception e) {
      LOG.warn(
          "Lens session went away! " + lensSession + " driver session: "
              + ((DriverSessionStarted) event).getDriverSessionID(), e);
    } finally {
      release(sessionHandle);
    }
  }
}
