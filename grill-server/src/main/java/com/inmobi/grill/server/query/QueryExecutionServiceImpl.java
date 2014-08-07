package com.inmobi.grill.server.query;

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

import com.inmobi.grill.server.GrillService;
import com.inmobi.grill.server.GrillServices;
import com.inmobi.grill.server.api.driver.*;
import com.inmobi.grill.server.api.events.GrillEventListener;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.api.metrics.MetricsService;
import com.inmobi.grill.server.api.query.PreparedQueryContext;
import com.inmobi.grill.server.api.query.QueryAccepted;
import com.inmobi.grill.server.api.query.QueryAcceptor;
import com.inmobi.grill.server.api.query.QueryCancelled;
import com.inmobi.grill.server.api.query.QueryClosed;
import com.inmobi.grill.server.api.query.QueryContext;
import com.inmobi.grill.server.api.query.QueryExecuted;
import com.inmobi.grill.server.api.query.QueryExecutionService;
import com.inmobi.grill.server.api.query.QueryFailed;
import com.inmobi.grill.server.api.query.QueryLaunched;
import com.inmobi.grill.server.api.query.QueryQueued;
import com.inmobi.grill.server.api.query.QueryRejected;
import com.inmobi.grill.server.api.query.QueryRunning;
import com.inmobi.grill.server.api.query.QuerySuccess;
import com.inmobi.grill.server.api.query.StatusChange;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.query.GrillPreparedQuery;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryHandleWithResultSet;
import com.inmobi.grill.api.query.QueryPlan;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.api.query.QueryResult;
import com.inmobi.grill.api.query.QueryResultSetMetadata;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.SubmitOp;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.driver.cube.CubeGrillDriver;
import com.inmobi.grill.driver.cube.RewriteUtil;
import com.inmobi.grill.driver.hive.HiveDriver;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.session.GrillSessionImpl;

public class QueryExecutionServiceImpl extends GrillService implements QueryExecutionService {
  public static final Log LOG = LogFactory.getLog(QueryExecutionServiceImpl.class);
  public static final String PREPARED_QUERIES_COUNTER = "prepared-queries";

  private static long millisInWeek = 7 * 24 * 60 * 60 * 1000;
  public static final String NAME = "query";

  private PriorityBlockingQueue<QueryContext> acceptedQueries =
      new PriorityBlockingQueue<QueryContext>();
  private List<QueryContext> launchedQueries = new ArrayList<QueryContext>();
  private DelayQueue<FinishedQuery> finishedQueries =
      new DelayQueue<FinishedQuery>();
  private DelayQueue<PreparedQueryContext> preparedQueryQueue =
      new DelayQueue<PreparedQueryContext>();
  private Map<QueryPrepareHandle, PreparedQueryContext> preparedQueries =
      new HashMap<QueryPrepareHandle, PreparedQueryContext>();
  private ConcurrentMap<QueryHandle, QueryContext> allQueries = 
      new ConcurrentHashMap<QueryHandle, QueryContext>();
  private Configuration conf;
  private final QuerySubmitter querySubmitterRunnable = new QuerySubmitter();
  protected final Thread querySubmitter = new Thread(querySubmitterRunnable,
      "QuerySubmitter");
  private final Thread statusPoller = new Thread(new StatusPoller(),
      "StatusPoller");
  private final Thread queryPurger = new Thread(new QueryPurger(),
      "QueryPurger");
  private final Thread prepareQueryPurger = new Thread(new PreparedQueryPurger(),
      "PrepareQueryPurger");
  private List<QueryAcceptor> queryAcceptors = new ArrayList<QueryAcceptor>();
  private final Map<String, GrillDriver> drivers = new HashMap<String, GrillDriver>();
  private DriverSelector driverSelector;
  private Map<QueryHandle, GrillResultSet> resultSets = new HashMap<QueryHandle, GrillResultSet>();
  private GrillEventService eventService;
  private MetricsService metricsService;

  public QueryExecutionServiceImpl(CLIService cliService) throws GrillException {
    super(NAME, cliService);
  }

  private void initializeQueryAcceptorsAndListeners() {
    if (conf.getBoolean(GrillConfConstants.GRILL_QUERY_STATE_LOGGER_ENABLED, true)) {
      getEventService().addListener(new QueryStatusLogger());
      LOG.info("Registered query state logger");
    }
    // Add result formatter
    getEventService().addListenerForType(new ResultFormatter(this), QueryExecuted.class);
    LOG.info("Registered query result formatter");
  }

  private void loadDriversAndSelector() throws GrillException {
    conf.get(GrillConfConstants.ENGINE_DRIVER_CLASSES);
    String[] driverClasses = conf.getStrings(
        GrillConfConstants.ENGINE_DRIVER_CLASSES);
    if (driverClasses != null) {
      for (String driverClass : driverClasses) {
        try {
          Class<?> clazz = Class.forName(driverClass);
          GrillDriver driver = (GrillDriver) clazz.newInstance();
          driver.configure(conf);
          drivers.put(driverClass, driver);
          LOG.info("Driver for " + driverClass + " is loaded");
        } catch (Exception e) {
          LOG.warn("Could not load the driver:" + driverClass, e);
          throw new GrillException("Could not load driver " + driverClass, e);
        }
      }
    } else {
      throw new GrillException("No drivers specified");
    }
    driverSelector = new CubeGrillDriver.MinQueryCostSelector();
  }

  protected GrillEventService getEventService() {
    if (eventService == null) {
      eventService = (GrillEventService) GrillServices.get().getService(GrillEventService.NAME);
      if (eventService == null) {
        throw new NullPointerException("Could not get event service");
      }
    }
    return eventService;
  }

  private synchronized MetricsService getMetrics() {
    if (metricsService == null) {
      metricsService = (MetricsService) GrillServices.get().getService(MetricsService.NAME);
      if (metricsService == null) {
        throw new NullPointerException("Could not get metrics service");
      }
    }
    return metricsService;
  }

  private void incrCounter(String counter) {
    getMetrics().incrCounter(QueryExecutionService.class, counter);
  }

  private void decrCounter(String counter) {
    getMetrics().decrCounter(QueryExecutionService.class, counter);
  }

  public static class QueryStatusLogger implements GrillEventListener<StatusChange> {
    public static final Log STATUS_LOG = LogFactory.getLog(QueryStatusLogger.class);
    @Override
    public void onEvent(StatusChange event) throws GrillException {
      STATUS_LOG.info(event.toString());
    }
  }

  private class FinishedQuery implements Delayed {
    private final QueryContext ctx;
    private final Date finishTime;
    FinishedQuery(QueryContext ctx) {
      this.ctx = ctx;
      this.finishTime = new Date();
      ctx.setEndTime(this.finishTime.getTime());
    }
    @Override
    public int compareTo(Delayed o) {
      return (int)(this.getDelay(TimeUnit.MILLISECONDS)
          - o.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public long getDelay(TimeUnit units) {
      long delayMillis;
      if (this.finishTime != null) {
        Date now = new Date();
        long elapsedMills = now.getTime() - this.finishTime.getTime();
        delayMillis = millisInWeek - elapsedMills;
        return units.convert(delayMillis, TimeUnit.MILLISECONDS);
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

  private class QuerySubmitter implements Runnable {
    private boolean pausedForTest = false;
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
                //acquire session before any query operation.
                acquire(ctx.getGrillSessionIdentifier());
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
                release(ctx.getGrillSessionIdentifier());
              }
              setLaunchedStatus(ctx);
              LOG.info("Launched query " + ctx.getQueryHandle());
            }
          }
        } catch (InterruptedException e) {
          LOG.info("Query Submitter has been interrupted, exiting");
          return;
        } catch (Exception e) {
          LOG.error("Error in query submitter", e);
        }
      }
      LOG.info("QuerySubmitter exited");
    }

  }

  // used in tests
  public void pauseQuerySubmitter() {
    querySubmitterRunnable.pausedForTest = true;
  }

  private class StatusPoller implements Runnable {
    long pollInterval = 1000;
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
              //acquire(ctx.getGrillSessionIdentifier());
              updateStatus(ctx.getQueryHandle());
            } catch (GrillException e) {
              LOG.error("Error updating status ", e);
            } finally {
              //release(ctx.getGrillSessionIdentifier());
            }
          }
          Thread.sleep(pollInterval);
        } catch (InterruptedException e) {
          LOG.info("Status poller has been interrupted, exiting");
          return;
        } catch (Exception e) {
          LOG.error("Error in status poller", e);
        }
      }
      LOG.info("StatusPoller exited");
    }
  }

  void setFailedStatus(QueryContext ctx, String statusMsg,
      String reason) throws GrillException {
    QueryStatus before = ctx.getStatus();
    ctx.setStatus(new QueryStatus(0.0f,
        QueryStatus.Status.FAILED,
        statusMsg, false, null, reason));
    updateFinishedQuery(ctx, before);
    fireStatusChangeEvent(ctx, ctx.getStatus(), before);
  }

  private void setLaunchedStatus(QueryContext ctx) throws GrillException {
    QueryStatus before = ctx.getStatus();
    ctx.setStatus(new QueryStatus(ctx.getStatus().getProgress(),
        QueryStatus.Status.LAUNCHED,
        "launched on the driver", false, null, null));
    launchedQueries.add(ctx);
    ctx.setLaunchTime(System.currentTimeMillis());
    fireStatusChangeEvent(ctx, ctx.getStatus(), before);
  }

  private void setCancelledStatus(QueryContext ctx, String statusMsg)
      throws GrillException {
    QueryStatus before = ctx.getStatus();
    ctx.setStatus(new QueryStatus(0.0f,
        QueryStatus.Status.CANCELED,
        statusMsg, false, null, null));
    updateFinishedQuery(ctx, before);
    fireStatusChangeEvent(ctx, ctx.getStatus(), before);
  }

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

  void setSuccessState(QueryContext ctx) throws GrillException {
    QueryStatus before = ctx.getStatus();
    ctx.setStatus(new QueryStatus(1.0f,
        QueryStatus.Status.SUCCESSFUL,
        "Query is successful!", ctx.isResultAvailableInDriver(), null, null));
    updateFinishedQuery(ctx, before);
    fireStatusChangeEvent(ctx, ctx.getStatus(), before);
  }

  private void updateStatus(final QueryHandle handle) throws GrillException {
    QueryContext ctx = allQueries.get(handle);
    if (ctx != null) {
      synchronized(ctx) {
        QueryStatus before = ctx.getStatus();
        if (!ctx.getStatus().getStatus().equals(QueryStatus.Status.QUEUED) &&
            !ctx.getDriverStatus().isFinished() &&
            !ctx.getStatus().isFinished()) {
          LOG.info("Updating status for " + ctx.getQueryHandle());
          try {
            ctx.getSelectedDriver().updateStatus(ctx);
            ctx.setStatus(ctx.getDriverStatus().toQueryStatus());
          } catch (GrillException exc) {
            // Driver gave exception while updating status
            QueryStatus failedStatus =
                new QueryStatus(1.0, Status.FAILED, "Status update failed",
                    false, "Status update failed", exc.getMessage());
            ctx.setStatus(failedStatus);
            LOG.error("Status update failed for " + handle + " reason: " + exc.getMessage());
          }
          //query is successfully executed by driver and
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

  private StatusChange newStatusChangeEvent(QueryContext ctx, QueryStatus.Status prevState,
      QueryStatus.Status currState) {
    QueryHandle query = ctx.getQueryHandle();
    switch (currState) {
    case CANCELED:
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
      return new QueryRunning(System.currentTimeMillis() - ctx.getDriverStatus().getDriverStartTime(), prevState, currState, query);
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
   * If query status has changed, fire a specific StatusChange event
   * @param ctx
   * @param current
   * @param before
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
      } catch (GrillException e) {
        LOG.warn("GrillEventService encountered error while handling event: " + event.getEventId(), e);
      }
    }
  }

  private class QueryPurger implements Runnable {
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
          // session is not required to close the query
          //acquire(finished.getCtx().getGrillSessionIdentifier());
          if (finished.getCtx().getSelectedDriver() != null) {
            finished.getCtx().getSelectedDriver().closeQuery(
                finished.getCtx().getQueryHandle());
          }
          allQueries.remove(finished.getCtx().getQueryHandle());
          fireStatusChangeEvent(finished.getCtx(),
              new QueryStatus(1f, Status.CLOSED, "Query purged", false, null, null),
              finished.getCtx().getStatus());
          LOG.info("Query purged: " + finished.getCtx().getQueryHandle());
        } catch (GrillException e) {
          LOG.error("Error closing  query ", e);
        }  catch (Exception e) {
          LOG.error("Error in query purger", e);
        } finally {
          /*try {
            release(finished.getCtx().getGrillSessionIdentifier());
          } catch (GrillException ignore) {
          }*/
        }
      }
      LOG.info("QueryPurger exited");
    }
  }

  private class PreparedQueryPurger implements Runnable {
    @Override
    public void run() {
      LOG.info("Starting Prepared Query purger thread");
      while (!stopped && !prepareQueryPurger.isInterrupted()) {
        try {
          PreparedQueryContext prepared = preparedQueryQueue.take();
          destroyPreparedQuery(prepared);
          LOG.info("Purged prepared query: " + prepared.getPrepareHandle());
        } catch (GrillException e) {
          LOG.error("Error closing prepared query ", e);
        } catch (InterruptedException e) {
          LOG.info("PreparedQueryPurger has been interrupted, exiting");
          return;
        } catch (Exception e) {
          LOG.error("Error in prepared query purger", e);
        }
      }
      LOG.info("PreparedQueryPurger exited");
    }
  }

  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    this.conf = hiveConf;
    initializeQueryAcceptorsAndListeners();
    try {
      loadDriversAndSelector();
    } catch (GrillException e) {
      throw new IllegalStateException("Could not load drivers");
    }
    LOG.info("Query execution service initialized");
  }

  public void prepareStopping() {
    super.prepareStopping();
    querySubmitter.interrupt();
    statusPoller.interrupt();
    queryPurger.interrupt();
    prepareQueryPurger.interrupt();
  }

  public synchronized void stop() {
    super.stop();
    for (Thread th : new Thread[]{querySubmitter, statusPoller, queryPurger, prepareQueryPurger}) {
      try {
        LOG.debug("Waiting for" + th.getName());
        th.join();
      } catch (InterruptedException e) {
        LOG.error("Error waiting for thread: " + th.getName(), e);
      }
    }
    LOG.info("Query execution service stopped");
  }

  public synchronized void start() {
    // recover query configurations from session
    synchronized (allQueries) {
      for (QueryContext ctx : allQueries.values()) {
        try {
          if (sessionMap.containsKey(ctx.getGrillSessionIdentifier())) {
            // try setting configuration if the query session is still not closed
            ctx.setConf(getGrillConf(getSessionHandle(
                ctx.getGrillSessionIdentifier()), ctx.getQconf()));
          } else {
            ctx.setConf(getGrillConf(ctx.getQconf()));
          }
        } catch (GrillException e) {
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

  private void rewriteAndSelect(QueryContext ctx) throws GrillException {
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        ctx.getUserQuery(), drivers.values(), ctx.getConf());

    // 2. select driver to run the query
    GrillDriver driver = driverSelector.select(drivers.values(), driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  private void rewriteAndSelect(PreparedQueryContext ctx) throws GrillException {
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        ctx.getUserQuery(), drivers.values(), ctx.getConf());

    // 2. select driver to run the query
    GrillDriver driver = driverSelector.select(drivers.values(), driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }


  private void accept(String query, Configuration conf, SubmitOp submitOp)
      throws GrillException {
    // run through all the query acceptors, and throw Exception if any of them
    // return false
    for (QueryAcceptor acceptor : queryAcceptors) {
      String cause = "";
      String rejectionCause = acceptor.accept(query, conf, submitOp);
      if (rejectionCause !=null) {
        getEventService().notifyEvent(new QueryRejected(System.currentTimeMillis(), query, rejectionCause, null));
        throw new GrillException("Query not accepted because " + cause);
      }
    }
    getEventService().notifyEvent(new QueryAccepted(System.currentTimeMillis(), null, query, null));
  }

  private GrillResultSet getResultset(QueryHandle queryHandle)
      throws GrillException {
    if (!allQueries.containsKey(queryHandle)) {
      throw new NotFoundException("Query not found: " + queryHandle);
    }

    GrillResultSet resultSet = resultSets.get(queryHandle);
    if (resultSet == null) {
      QueryContext ctx = allQueries.get(queryHandle);
      if (ctx.isPersistent() && ctx.getQueryOutputFormatter() != null) {
        resultSets.put(queryHandle, new GrillPersistentResult(
            ctx.getQueryOutputFormatter().getMetadata(),
            ctx.getQueryOutputFormatter().getFinalOutputPath().toString(),
            ctx.getQueryOutputFormatter().getNumRows()));
      } else if (allQueries.get(queryHandle).isResultAvailableInDriver()) {
        resultSet = allQueries.get(queryHandle).getSelectedDriver().
            fetchResultSet(allQueries.get(queryHandle));
        resultSets.put(queryHandle, resultSet);
      } else {
        throw new NotFoundException("Result set not available for query:" + queryHandle);
      }
    }   
    return resultSets.get(queryHandle);
  }

  GrillResultSet getDriverResultset(QueryHandle queryHandle)
      throws GrillException {
    return allQueries.get(queryHandle).getSelectedDriver().
        fetchResultSet(allQueries.get(queryHandle));
  }

  @Override
  public QueryPrepareHandle prepare(GrillSessionHandle sessionHandle, String query, GrillConf grillConf)
      throws GrillException {
    try {
      acquire(sessionHandle);
      PreparedQueryContext prepared =  prepareQuery(sessionHandle, query, grillConf, SubmitOp.PREPARE);
      prepared.getSelectedDriver().prepare(prepared);
      return prepared.getPrepareHandle();
    } finally {
      release(sessionHandle);
    }
  }

  private PreparedQueryContext prepareQuery(GrillSessionHandle sessionHandle,
      String query, GrillConf grillConf, SubmitOp op) throws GrillException {
    Configuration conf = getGrillConf(sessionHandle, grillConf);
    accept(query, conf, op);
    PreparedQueryContext prepared = new PreparedQueryContext(query,
        getSession(sessionHandle).getUserName(), conf, grillConf);
    rewriteAndSelect(prepared);
    preparedQueries.put(prepared.getPrepareHandle(), prepared);
    preparedQueryQueue.add(prepared);
    incrCounter(PREPARED_QUERIES_COUNTER);
    return prepared;
  }

  @Override
  public QueryPlan explainAndPrepare(GrillSessionHandle sessionHandle,
      String query, GrillConf grillConf) throws GrillException {
    try {
      LOG.info("ExlainAndPrepare: " + sessionHandle.toString() + " query: " + query);
      acquire(sessionHandle);
      PreparedQueryContext prepared = prepareQuery(sessionHandle, query,
          grillConf, SubmitOp.EXPLAIN_AND_PREPARE);
      QueryPlan plan = prepared.getSelectedDriver().explainAndPrepare(prepared).toQueryPlan();
      plan.setPrepareHandle(prepared.getPrepareHandle());
      return plan;
    } catch (UnsupportedEncodingException e) {
      throw new GrillException(e);
    } finally {
      release(sessionHandle);
    }
  }

  @Override
  public QueryHandle executePrepareAsync(GrillSessionHandle sessionHandle,
      QueryPrepareHandle prepareHandle, GrillConf conf)
          throws GrillException {
    try {
      LOG.info("ExecutePrepareAsync: " + sessionHandle.toString() +
          " query:" + prepareHandle.getPrepareHandleId());
      acquire(sessionHandle);
      PreparedQueryContext pctx = getPreparedQueryContext(sessionHandle, prepareHandle);
      Configuration qconf = getGrillConf(sessionHandle, conf);
      accept(pctx.getUserQuery(), qconf, SubmitOp.EXECUTE);
      QueryContext ctx = createContext(pctx,
          getSession(sessionHandle).getUserName(), conf, qconf);
      return executeAsyncInternal(sessionHandle, ctx, qconf);
    } finally {
      release(sessionHandle);
    }
  }

  @Override
  public QueryHandleWithResultSet executePrepare(GrillSessionHandle sessionHandle,
      QueryPrepareHandle prepareHandle, long timeoutMillis,
      GrillConf conf) throws GrillException {
    try {
      LOG.info("ExecutePrepare: " + sessionHandle.toString() +
          " query:" + prepareHandle.getPrepareHandleId() + " timeout:" + timeoutMillis);
      acquire(sessionHandle);
      PreparedQueryContext pctx = getPreparedQueryContext(sessionHandle, prepareHandle);
      Configuration qconf = getGrillConf(sessionHandle, conf);
      QueryContext ctx = createContext(pctx,
          getSession(sessionHandle).getUserName(), conf, qconf);
      return executeTimeoutInternal(sessionHandle, ctx, timeoutMillis, qconf);
    } finally {
      release(sessionHandle);
    }
  }

  @Override
  public QueryHandle executeAsync(GrillSessionHandle sessionHandle, String query,
      GrillConf conf) throws GrillException {
    try {
      LOG.info("ExecuteAsync: "  + sessionHandle.toString() + " query: " + query);
      acquire(sessionHandle);
      Configuration qconf = getGrillConf(sessionHandle, conf);
      accept(query, qconf, SubmitOp.EXECUTE);
      QueryContext ctx = createContext(query,
          getSession(sessionHandle).getUserName(), conf, qconf);
      return executeAsyncInternal(sessionHandle, ctx, qconf);
    } finally {
      release(sessionHandle);
    }
  }


  protected QueryContext createContext(String query, String userName,
      GrillConf conf, Configuration qconf) throws GrillException {
    QueryContext ctx = new QueryContext(query,userName, conf, qconf);
    return ctx;
  }

  protected QueryContext createContext(PreparedQueryContext pctx, String userName,
      GrillConf conf, Configuration qconf) throws GrillException {
    QueryContext ctx = new QueryContext(pctx, userName, conf, qconf);
    return ctx;
  }

  private QueryHandle executeAsyncInternal(GrillSessionHandle sessionHandle, QueryContext ctx,
      Configuration qconf) throws GrillException {
    ctx.setGrillSessionIdentifier(sessionHandle.getPublicId().toString());
    QueryStatus before = ctx.getStatus();
    ctx.setStatus(new QueryStatus(0.0,
        QueryStatus.Status.QUEUED,
        "Query is queued", false, null, null));
    acceptedQueries.add(ctx);
    allQueries.put(ctx.getQueryHandle(), ctx);
    fireStatusChangeEvent(ctx, ctx.getStatus(), before);
    LOG.info("Returning handle " + ctx.getQueryHandle().getHandleId());
    return ctx.getQueryHandle();
  }

  @Override
  public boolean updateQueryConf(GrillSessionHandle sessionHandle, QueryHandle queryHandle, GrillConf newconf)
      throws GrillException {
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

  @Override
  public boolean updateQueryConf(GrillSessionHandle sessionHandle, QueryPrepareHandle prepareHandle, GrillConf newconf)
      throws GrillException {
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

  private QueryContext getQueryContext(GrillSessionHandle sessionHandle, QueryHandle queryHandle)
      throws GrillException {
    try {
      acquire(sessionHandle);
      QueryContext ctx = allQueries.get(queryHandle);
      if (ctx == null) {
        throw new NotFoundException("Query not found " + queryHandle);
      }
      updateStatus(queryHandle);
      return ctx;
    } finally {
      release(sessionHandle);
    }
  }

  QueryContext getQueryContext(QueryHandle queryHandle) {
    return allQueries.get(queryHandle);
  }

  @Override
  public GrillQuery getQuery(GrillSessionHandle sessionHandle, QueryHandle queryHandle)
      throws GrillException {
    return getQueryContext(sessionHandle, queryHandle).toGrillQuery();
  }

  private PreparedQueryContext getPreparedQueryContext(GrillSessionHandle sessionHandle, 
      QueryPrepareHandle prepareHandle)
          throws GrillException {
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

  @Override
  public GrillPreparedQuery getPreparedQuery(GrillSessionHandle sessionHandle, 
      QueryPrepareHandle prepareHandle)
          throws GrillException {
    return getPreparedQueryContext(sessionHandle, prepareHandle).toPreparedQuery();
  }

  @Override
  public QueryHandleWithResultSet execute(GrillSessionHandle sessionHandle, String query, long timeoutMillis,
      GrillConf conf) throws GrillException {
    try {
      LOG.info("Blocking execute " + sessionHandle.toString() + " query: " 
          + query + " timeout: " + timeoutMillis);
      acquire(sessionHandle);
      Configuration qconf = getGrillConf(sessionHandle, conf);
      accept(query, qconf, SubmitOp.EXECUTE);
      QueryContext ctx = createContext(query,
          getSession(sessionHandle).getUserName(), conf, qconf);
      return executeTimeoutInternal(sessionHandle, ctx, timeoutMillis, qconf);
    } finally {
      release(sessionHandle);
    }
  }

  private QueryHandleWithResultSet executeTimeoutInternal(GrillSessionHandle sessionHandle, QueryContext ctx, long timeoutMillis,
      Configuration conf) throws GrillException {
    QueryHandle handle = executeAsyncInternal(sessionHandle, ctx, conf);
    QueryHandleWithResultSet result = new QueryHandleWithResultSet(handle);
    // getQueryContext calls updateStatus, which fires query events if there's a change in status
    while (getQueryContext(sessionHandle, handle).getStatus().getStatus().equals(
        QueryStatus.Status.QUEUED)) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    QueryCompletionListener listener = new QueryCompletionListenerImpl();
    getQueryContext(sessionHandle, handle).getSelectedDriver().
    registerForCompletionNotification(handle, timeoutMillis, listener);
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

  class QueryCompletionListenerImpl implements QueryCompletionListener {
    boolean succeeded = false;
    QueryCompletionListenerImpl() {
    }
    @Override
    public void onCompletion(QueryHandle handle) {
      synchronized (this) {
        succeeded = true;
        LOG.info("Query with time out succeeded");
        this.notify();
      }
    }

    @Override
    public void onError(QueryHandle handle, String error) {
      synchronized (this) {
        succeeded = false;
        LOG.info("Query with time out failed");
        this.notify();
      }
    }
  }

  @Override
  public QueryResultSetMetadata getResultSetMetadata(GrillSessionHandle sessionHandle, QueryHandle queryHandle)
      throws GrillException {
    try {
      LOG.info("GetResultSetMetadata: " + sessionHandle.toString() + " query: " + queryHandle);
      acquire(sessionHandle);
      GrillResultSet resultSet = getResultset(queryHandle);
      if (resultSet != null) {
        return resultSet.getMetadata().toQueryResultSetMetadata();
      } else {
        throw new NotFoundException("Resultset metadata not found for query: ("
            + sessionHandle + ", " + queryHandle + ")");
      }
    } finally {
      release(sessionHandle);
    }
  }

  @Override
  public QueryResult fetchResultSet(GrillSessionHandle sessionHandle, QueryHandle queryHandle, long startIndex,
      int fetchSize) throws GrillException {
    try {
      LOG.info("FetchResultSet:" + sessionHandle.toString() + " query:" + queryHandle);
      acquire(sessionHandle);
      return getResultset(queryHandle).toQueryResult();
    } finally {
      release(sessionHandle);
    }
  }

  @Override
  public void closeResultSet(GrillSessionHandle sessionHandle, QueryHandle queryHandle) throws GrillException {
    try {
      LOG.info("CloseResultSet:" + sessionHandle.toString() +" query: " + queryHandle);
      acquire(sessionHandle);
      resultSets.remove(queryHandle);
      // Ask driver to close result set
      getQueryContext(queryHandle).getSelectedDriver().closeResultSet(queryHandle);
    } finally {
      release(sessionHandle);
    }
  }

  @Override
  public boolean cancelQuery(GrillSessionHandle sessionHandle, QueryHandle queryHandle) throws GrillException {
    try {
      LOG.info("CancelQuery: " + sessionHandle.toString() + " query:" + queryHandle);
      acquire(sessionHandle);
      QueryContext ctx = getQueryContext(sessionHandle, queryHandle);
      if (ctx.getStatus().isFinished()) {
        return false;
      }
      synchronized (ctx) {
        if (ctx.getStatus().getStatus().equals(
            QueryStatus.Status.LAUNCHED) || ctx.getStatus().getStatus().equals(
                QueryStatus.Status.RUNNING)) {
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

  @Override
  public List<QueryHandle> getAllQueries(GrillSessionHandle sessionHandle, String state, String user)
      throws GrillException {
    try {
      acquire(sessionHandle);
      Status status = null;
      try {
        status = StringUtils.isBlank(state) ? null : Status.valueOf(state);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Bad state argument passed, possible" +
            " values are " + Status.values(), e);
      }
      boolean filterByStatus = status != null;
      boolean filterByUser = StringUtils.isNotBlank(user);

      List<QueryHandle> all = new ArrayList<QueryHandle>(allQueries.keySet());
      Iterator<QueryHandle> itr = all.iterator();
      while (itr.hasNext()) {
        QueryHandle q = itr.next();
        if ( (filterByStatus && status != allQueries.get(q).getStatus().getStatus())
            || (filterByUser && !user.equalsIgnoreCase(allQueries.get(q).getSubmittedUser()))
            ) {
          itr.remove();
        }
      }
      return all;
    } finally {
      release(sessionHandle);
    }
  }

  @Override
  public List<QueryPrepareHandle> getAllPreparedQueries(GrillSessionHandle sessionHandle, String user)
      throws GrillException {
    try {
      acquire(sessionHandle);
      List<QueryPrepareHandle> allPrepared = new ArrayList<QueryPrepareHandle>(preparedQueries.keySet());
      Iterator<QueryPrepareHandle> itr = allPrepared.iterator();
      while (itr.hasNext()) {
        QueryPrepareHandle q = itr.next();
        if (StringUtils.isNotBlank(user) && !user.equalsIgnoreCase(preparedQueries.get(q).getPreparedUser())) {
          itr.remove();
        }
      }
      return allPrepared;
    } finally {
      release(sessionHandle);
    }
  }

  @Override
  public boolean destroyPrepared(GrillSessionHandle sessionHandle, QueryPrepareHandle prepared)
      throws GrillException {
    try {
      LOG.info("DestroyPrepared: " + sessionHandle.toString() + " query:" + prepared);
      acquire(sessionHandle);
      destroyPreparedQuery(getPreparedQueryContext(sessionHandle, prepared));
      return true;
    } finally {
      release(sessionHandle);
    }
  }

  private void destroyPreparedQuery(PreparedQueryContext ctx) throws GrillException {
    ctx.getSelectedDriver().closePreparedQuery(ctx.getPrepareHandle());
    preparedQueries.remove(ctx.getPrepareHandle());
    preparedQueryQueue.remove(ctx);
    decrCounter(PREPARED_QUERIES_COUNTER);
  }

  @Override
  public QueryPlan explain(GrillSessionHandle sessionHandle, String query,
      GrillConf grillConf) throws GrillException {
    try {
      LOG.info("Explain: " + sessionHandle.toString() + " query:" + query);
      acquire(sessionHandle);
      Configuration qconf = getGrillConf(sessionHandle, grillConf);
      accept(query, qconf, SubmitOp.EXPLAIN);
      Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
          query, drivers.values(), qconf);
      // select driver to run the query
      GrillDriver selectedDriver = driverSelector.select(drivers.values(), driverQueries, conf);
      return selectedDriver.explain(driverQueries.get(selectedDriver), qconf).toQueryPlan();
    } catch (UnsupportedEncodingException e) {
      throw new GrillException(e);
    } finally {
      release(sessionHandle);
    }
  }

  public void addResource(GrillSessionHandle sessionHandle, String type, String path) throws GrillException {
    try {
      acquire(sessionHandle);
      String command = "add " + type.toLowerCase() + " " + path;
      for (GrillDriver driver : drivers.values()) {
        if (driver instanceof HiveDriver) {
          GrillConf conf = new GrillConf();
          conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
          QueryContext addQuery = new QueryContext(command,
              getSession(sessionHandle).getUserName(),
              getGrillConf(sessionHandle, conf));
          addQuery.setGrillSessionIdentifier(sessionHandle.getPublicId().toString());
          driver.execute(addQuery);
        }
      }
    } finally {
      release(sessionHandle);
    }
  }

  public void deleteResource(GrillSessionHandle sessionHandle, String type, String path) throws GrillException {
    try {
      acquire(sessionHandle);
      String command = "delete " + type.toLowerCase() + " " + path;
      for (GrillDriver driver : drivers.values()) {
        if (driver instanceof HiveDriver) {
          GrillConf conf = new GrillConf();
          conf.addProperty(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "false");
          QueryContext addQuery = new QueryContext(command,
              getSession(sessionHandle).getUserName(),
              getGrillConf(sessionHandle, conf));
          addQuery.setGrillSessionIdentifier(sessionHandle.getPublicId().toString());
          driver.execute(addQuery);
        }
      }
    } finally {
      release(sessionHandle);
    }
  }

  @Override
  public void readExternal(ObjectInput in)
      throws IOException, ClassNotFoundException {
    super.readExternal(in);
    // Restore drivers
    synchronized (drivers) {
      int numDrivers = in.readInt();
      for (int i =0; i < numDrivers; i++) {
        String driverClsName = in.readUTF();
        GrillDriver driver = drivers.get(driverClsName);
        if (driver == null) { 
          // this driver is removed in the current server restart
          // we will create an instance and read its state still.
          try {
            Class<? extends GrillDriver> driverCls = 
                (Class<? extends GrillDriver>)Class.forName(driverClsName);
            driver = (GrillDriver) driverCls.newInstance();
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

      for (int i =0; i < numQueries; i++) {
        QueryContext ctx = (QueryContext)in.readObject();
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
        case CLOSED :
          allQueries.remove(ctx.getQueryHandle());
        }
      }
      LOG.info("Recovered " + allQueries.size() + " queries");
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    super.writeExternal(out);
    // persist all drivers
    synchronized (drivers) {
      out.writeInt(drivers.size());
      for (GrillDriver driver : drivers.values()) {
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

  private void pipe(InputStream is, OutputStream os) throws IOException {
    int n;
    byte[] buffer = new byte[4096];
    while ((n = is.read(buffer)) > -1) {
      os.write(buffer, 0, n);
      os.flush();
    }
  }

  @Override
  public Response getHttpResultSet(GrillSessionHandle sessionHandle,
      QueryHandle queryHandle) throws GrillException {
    final QueryContext ctx = getQueryContext(sessionHandle, queryHandle);
    GrillResultSet result = getResultset(queryHandle);
    if (result instanceof GrillPersistentResult) {
      final Path resultPath = new Path(((PersistentResultSet)result).getOutputPath());
      String resultFSReadUrl = ctx.getConf().get(GrillConfConstants.RESULT_FS_READ_URL);
      if (resultFSReadUrl != null) {
        try {
          URI resultReadPath = new URI(resultFSReadUrl + 
              resultPath.toUri().getPath() +
              "?op=OPEN&user.name="+getSession(sessionHandle).getUserName());
          return Response.seeOther(resultReadPath)
              .header("content-disposition","attachment; filename = "+ resultPath.getName())
              .type(MediaType.APPLICATION_OCTET_STREAM).build();
        } catch (URISyntaxException e) {
          throw new GrillException(e);
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
        return Response.ok(stream)
            .header("content-disposition","attachment; filename = "+ resultPath.getName())
            .type(MediaType.APPLICATION_OCTET_STREAM).build();
      }
    } else {
      throw new NotFoundException("Http result not available for query:" + queryHandle.toString());
    }
  }

  /**
   * Allow drivers to release resources acquired for a session if any.
   * @param sessionHandle
   */
  public void closeDriverSessions(GrillSessionHandle sessionHandle) {
    for (GrillDriver driver : drivers.values()) {
      if (driver instanceof HiveDriver) {
        ((HiveDriver) driver).closeSession(sessionHandle);
      }
    }
  }

  public void closeSession(GrillSessionHandle sessionHandle) throws GrillException {
    super.closeSession(sessionHandle);
    // Call driver session close in case some one closes sessions directly on query service
    closeDriverSessions(sessionHandle);
  }

  // Used in test code
  Collection<GrillDriver> getDrivers(){
    return drivers.values();
  }
}
