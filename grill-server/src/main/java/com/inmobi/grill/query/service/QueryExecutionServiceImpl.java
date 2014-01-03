package com.inmobi.grill.query.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.NotFoundException;

import com.inmobi.grill.server.api.events.GrillEventListener;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.api.events.query.*;
import com.inmobi.grill.service.GrillServices;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;

import com.inmobi.grill.api.DriverSelector;
import com.inmobi.grill.api.GrillConfConstants;
import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.GrillResultSetMetadata;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.api.PreparedQueryContext;
import com.inmobi.grill.api.QueryCompletionListener;
import com.inmobi.grill.api.QueryContext;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QueryHandleWithResultSet;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.driver.cube.CubeGrillDriver;
import com.inmobi.grill.driver.cube.RewriteUtil;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.QueryExecutionService;
import com.inmobi.grill.service.GrillService;

public class QueryExecutionServiceImpl extends GrillService implements QueryExecutionService {
  static {
    Configuration.addDefaultResource("grill-default.xml");
    Configuration.addDefaultResource("grill-site.xml");
  }

  public static final Log LOG = LogFactory.getLog(QueryExecutionServiceImpl.class);

  private static long millisInWeek = 7 * 24 * 60 * 60 * 1000;

  private PriorityBlockingQueue<QueryContext> acceptedQueries =
      new PriorityBlockingQueue<QueryContext>();
  private List<QueryContext> launchedQueries = new ArrayList<QueryContext>();
  private DelayQueue<FinishedQuery> finishedQueries =
      new DelayQueue<FinishedQuery>();
  private DelayQueue<PreparedQueryContext> preparedQueryQueue =
      new DelayQueue<PreparedQueryContext>();
  private Map<QueryPrepareHandle, PreparedQueryContext> preparedQueries =
      new HashMap<QueryPrepareHandle, PreparedQueryContext>();
  private ConcurrentMap<QueryHandle, QueryContext> allQueries = new ConcurrentHashMap<QueryHandle, QueryContext>();
  private Configuration conf;
  private final Thread querySubmitter = new Thread(new QuerySubmitter(),
      "QuerySubmitter");
  private final Thread statusPoller = new Thread(new StatusPoller(),
      "StatusPoller");
  private final Thread queryPurger = new Thread(new QueryPurger(),
      "QueryPurger");
  private final Thread prepareQueryPurger = new Thread(new PreparedQueryPurger(),
      "PrepareQueryPurger");
  private boolean stopped = false;
  private List<QueryAcceptor> queryAcceptors = new ArrayList<QueryAcceptor>();
  private final List<GrillDriver> drivers = new ArrayList<GrillDriver>();
  private DriverSelector driverSelector;
  private Map<QueryHandle, GrillResultSet> resultSets = new HashMap<QueryHandle, GrillResultSet>();
  private GrillEventService eventService;

  public QueryExecutionServiceImpl(CLIService cliService) throws GrillException {
    super("query", cliService);
  }

  private void initializeQueryAcceptorsAndListeners() {
    if (conf.getBoolean(GrillConfConstants.GRILL_QUERY_STATE_LOGGER_ENABLED, true)) {
      getEventService().addListener(new QueryStatusLogger());
      LOG.info("Registered query state logger");
    }
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
          drivers.add(driver);
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

  private synchronized GrillEventService getEventService() {
    if (eventService == null) {
      eventService = (GrillEventService) GrillServices.get().getService(GrillEventService.NAME);
      if (eventService == null) {
        throw new NullPointerException("Could not get event service");
      }
    }
    return eventService;
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
    @Override
    public void run() {
      LOG.info("Starting QuerySubmitter thread");
      while (!stopped && !querySubmitter.isInterrupted()) {
        try {
          QueryContext ctx = acceptedQueries.take();
          LOG.info("Launching query:" + ctx.getDriverQuery());
          rewriteAndSelect(ctx);
          ctx.getSelectedDriver().executeAsync(ctx);
          QueryStatus before = ctx.getStatus();
          ctx.setStatus(new QueryStatus(ctx.getStatus().getProgress(),
            QueryStatus.Status.LAUNCHED,
            "launched on the driver", false));
          launchedQueries.add(ctx);
          fireStatusChangeEvent(ctx, ctx.getStatus(), before);
        } catch (GrillException e) {
          LOG.error("Error launching query ", e);
        } catch (InterruptedException e) {
          LOG.info("Query Submitter has been interrupted, exiting");
          return;
        } catch (Exception e) {
          LOG.error("Error in query submitter", e);
        }
      }
    }
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
            try {
              updateStatus(ctx.getQueryHandle());
            } catch (GrillException e) {
              LOG.error("Error updating status ", e);
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
    }
  }

  private void updateStatus(final QueryHandle handle) throws GrillException {
    QueryContext ctx = allQueries.get(handle);
    if (ctx != null) {
      synchronized(handle.getHandleId()) {
        QueryStatus before = ctx.getStatus();
        if (!ctx.getStatus().getStatus().equals(QueryStatus.Status.QUEUED) &&
            !ctx.getStatus().isFinished()) {
          ctx.setStatus(ctx.getSelectedDriver().getStatus(ctx.getQueryHandle()));
        }

        if (ctx.getStatus().isFinished()) {
          updateFinishedQuery(ctx);
        }
        fireStatusChangeEvent(ctx, ctx.getStatus(), before);
      }
    }
  }

  private StatusChange newStatusChangeEvent(QueryContext ctx, QueryStatus.Status prevState,
                                            QueryStatus.Status currState) {
    QueryHandle query = ctx.getQueryHandle();
    // TODO Get event time from status
    switch (currState) {
      case CANCELED:
        return new QueryCancelled(ctx.getCancelTime(), prevState, currState, query, ctx.getSubmittedUser(), null);
      case CLOSED:
        return new QueryClosed(ctx.getClosedTime(), prevState, currState, query, ctx.getSubmittedUser(), null);
      case FAILED:
        return new QueryFailed(ctx.getEndTime(), prevState, currState, query, ctx.getSubmittedUser(), null);
      case LAUNCHED:
        return new QueryLaunched(ctx.getLaunchTime(), prevState, currState, query);
      case QUEUED:
        long time = ctx.getSubmissionTime() == null ? System.currentTimeMillis() : ctx.getSubmissionTime().getTime();
        return new QueryQueued(time, prevState, currState, query, ctx.getSubmittedUser());
      case RUNNING:
        return new QueryRunning(ctx.getRunningTime(), prevState, currState, query);
      case SUCCESSFUL:
        return new QuerySuccess(ctx.getEndTime(), prevState, currState, query);
      case UNKNOWN:
      default:
        LOG.warn("Query " + query + " transitioned to UNKNOWN state from " + prevState + " state");
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

    QueryStatus.Status prevState = before == null ? Status.UNKNOWN : before.getStatus();
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

  private void updateFinishedQuery(QueryContext ctx) throws GrillException {
    launchedQueries.remove(ctx);
    finishedQueries.add(new FinishedQuery(ctx));
    notifyAllListeners();
  }

  private class QueryPurger implements Runnable {
    @Override
    public void run() {
      LOG.info("Starting Query purger thread");
      while (!stopped && !queryPurger.isInterrupted()) {
        try {
          FinishedQuery finished = finishedQueries.take();
          notifyAllListeners();
          finished.getCtx().getSelectedDriver().closeQuery(
              finished.getCtx().getQueryHandle());
          allQueries.remove(finished.getCtx().getQueryHandle());
          fireStatusChangeEvent(finished.getCtx(),
            new QueryStatus(1f, Status.CLOSED, "Query purged", false),
            finished.getCtx().getStatus());
          notifyAllListeners();
        } catch (GrillException e) {
          LOG.error("Error closing  query ", e);
        } catch (InterruptedException e) {
          LOG.info("QueryPurger has been interrupted, exiting");
          return;
        } catch (Exception e) {
          LOG.error("Error in query purger", e);
        }
      }
    }
  }

  private class PreparedQueryPurger implements Runnable {
    @Override
    public void run() {
      LOG.info("Starting Prepared Query purger thread");
      while (!stopped && !prepareQueryPurger.isInterrupted()) {
        try {
          PreparedQueryContext prepared = preparedQueryQueue.take();
          prepared.getSelectedDriver().closePreparedQuery(prepared.getPrepareHandle());
          preparedQueries.remove(prepared.getPrepareHandle());
          notifyAllListeners();
        } catch (GrillException e) {
          LOG.error("Error closing prepared query ", e);
        } catch (InterruptedException e) {
          LOG.info("PreparedQueryPurger has been interrupted, exiting");
          return;
        } catch (Exception e) {
          LOG.error("Error in prepared query purger", e);
        }
      }
    }
  }

  public void notifyAllListeners() throws GrillException {
    //TODO 
  }
  /*
 // @Override
  public String getName() {
    return "query";
  }

 // @Override
  public void init() throws GrillException {
    initializeQueryAcceptorsAndListeners();
    loadDriversAndSelector();
  }

 // @Override
  public void start() throws GrillException {
    querySubmitter.start();
    statusPoller.start();
    queryPurger.start();
    prepareQueryPurger.start();
  }

 // @Override
  public void stop() throws GrillException {
    this.stopped = true;

    querySubmitter.interrupt();
    statusPoller.interrupt();
    queryPurger.interrupt();
    prepareQueryPurger.interrupt();

    try {
      querySubmitter.join();
      statusPoller.join();
      queryPurger.join();
      prepareQueryPurger.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
   */
  public synchronized void init(HiveConf hiveConf) {
    super.init(hiveConf);
    this.conf = hiveConf;
    initializeQueryAcceptorsAndListeners();
    try {
      loadDriversAndSelector();
    } catch (GrillException e) {
      throw new IllegalStateException("Could not load drivers");
    }
  }

  public synchronized void stop() {
    super.stop();
    querySubmitter.interrupt();
    statusPoller.interrupt();
    queryPurger.interrupt();
    prepareQueryPurger.interrupt();

    try {
      querySubmitter.join();
      statusPoller.join();
      queryPurger.join();
      prepareQueryPurger.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public synchronized void start() {
    super.start();
    querySubmitter.start();
    statusPoller.start();
    queryPurger.start();
    prepareQueryPurger.start();
  }

  private void rewriteAndSelect(QueryContext ctx) throws GrillException {
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        ctx.getUserQuery(), drivers);

    // 2. select driver to run the query
    GrillDriver driver = driverSelector.select(drivers, driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  private void rewriteAndSelect(PreparedQueryContext ctx) throws GrillException {
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        ctx.getUserQuery(), drivers);

    // 2. select driver to run the query
    GrillDriver driver = driverSelector.select(drivers, driverQueries, conf);

    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }


  private void accept(String query, Configuration conf, SubmitOp submitOp)
      throws GrillException {
    // run through all the query acceptors, and throw Exception if any of them
    // return false
    for (QueryAcceptor acceptor : queryAcceptors) {
      String cause = "";
      boolean accept = acceptor.doAccept(query, conf, submitOp);
      if (!accept) {
        getEventService().notifyEvent(new QueryRejected(System.currentTimeMillis(), query, acceptor.getRejectionCause(), null));
        throw new GrillException("Query not accepted because " + cause);
      }
    }
    getEventService().notifyEvent(new QueryAccepted(System.currentTimeMillis(), null, query, null));
  }

  private Configuration getQueryConf(GrillSessionHandle sessionHandle, QueryConf queryConf) throws GrillException {
    HiveConf sessionConf;
    try {
      sessionConf = getSessionManager().getSession(sessionHandle.getSessionHandle()).getHiveConf();
    } catch (HiveSQLException e) {
      throw new GrillException(e);
    }
    Configuration qconf = new Configuration(sessionConf);
    if (queryConf != null && !queryConf.getProperties().isEmpty()) {
      for (Map.Entry<String, String> entry : queryConf.getProperties().entrySet()) {
        qconf.set(entry.getKey(), entry.getValue());
      }
    }
    return qconf;
  }

  private GrillResultSet getResultset(QueryHandle queryHandle)
      throws GrillException {
    GrillResultSet resultSet = resultSets.get(queryHandle);
    if (resultSet == null) {
      if (allQueries.get(queryHandle).getStatus().hasResultSet()) {
        resultSet = allQueries.get(queryHandle).getSelectedDriver().
            fetchResultSet(allQueries.get(queryHandle));
        resultSets.put(queryHandle, resultSet);
      } else {
        throw new NotFoundException("Result set not available for query:" + queryHandle);
      }
    }   
    return resultSets.get(queryHandle);
  }

  /* @Override
  public QueryPlan explain(String query, QueryConf queryConf)
      throws GrillException {
    Configuration qconf = getQueryConf(queryConf);
    accept(query, qconf, SubmitOp.EXPLAIN);
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        query, drivers);
    // select driver to run the query
    return driverSelector.select(drivers, driverQueries, conf).explain(query, qconf);
  } */

  @Override
  public QueryPrepareHandle prepare(GrillSessionHandle sessionHandle, String query, QueryConf queryConf)
      throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      Configuration qconf = getQueryConf(sessionHandle, queryConf);
      accept(query, qconf, SubmitOp.PREPARE);
      PreparedQueryContext prepared = new PreparedQueryContext(query, null, qconf);
      rewriteAndSelect(prepared);
      preparedQueries.put(prepared.getPrepareHandle(), prepared);
      preparedQueryQueue.add(prepared);
      prepared.getSelectedDriver().prepare(prepared);
      System.out.println("################### returning " + prepared.getPrepareHandle());
      return prepared.getPrepareHandle();
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public QueryPlan explainAndPrepare(GrillSessionHandle sessionHandle, String query, QueryConf queryConf)
      throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      Configuration qconf = getQueryConf(sessionHandle, queryConf);
      accept(query, qconf, SubmitOp.EXPLAIN_AND_PREPARE);
      PreparedQueryContext prepared = new PreparedQueryContext(query, null, qconf);
      rewriteAndSelect(prepared);
      preparedQueries.put(prepared.getPrepareHandle(), prepared);
      preparedQueryQueue.add(prepared);
      return prepared.getSelectedDriver().explainAndPrepare(prepared);
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public QueryHandle executePrepareAsync(GrillSessionHandle sessionHandle,
      QueryPrepareHandle prepareHandle, QueryConf queryConf)
          throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      PreparedQueryContext pctx = getPreparedQueryContext(sessionHandle, prepareHandle);
      Configuration qconf = getQueryConf(sessionHandle, queryConf);
      accept(pctx.getUserQuery(), qconf, SubmitOp.EXECUTE);
      QueryContext ctx = new QueryContext(pctx, getSession(sessionHandle.getSessionHandle()).getUserName(), qconf);
      ctx.setStatus(new QueryStatus(0.0,
          QueryStatus.Status.QUEUED,
          "Query is queued", false));
      acceptedQueries.add(ctx);
      allQueries.put(ctx.getQueryHandle(), ctx);
      fireStatusChangeEvent(ctx, ctx.getStatus(), null);
      LOG.info("Returning handle " + ctx.getQueryHandle().getHandleId());
      return ctx.getQueryHandle();
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public QueryHandle executeAsync(GrillSessionHandle sessionHandle, String query,
      QueryConf queryConf) throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      Configuration qconf = getQueryConf(sessionHandle, queryConf);
      accept(query, qconf, SubmitOp.EXECUTE);

      QueryContext ctx = new QueryContext(query, getSession(sessionHandle.getSessionHandle()).getUserName(), qconf);
      acceptedQueries.add(ctx);
      ctx.setStatus(new QueryStatus(0.0,
        QueryStatus.Status.QUEUED,
        "Query is queued", false));
      allQueries.put(ctx.getQueryHandle(), ctx);
      fireStatusChangeEvent(ctx, ctx.getStatus(), null);
      LOG.info("Returning handle " + ctx.getQueryHandle().getHandleId());
      return ctx.getQueryHandle();
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public boolean updateQueryConf(GrillSessionHandle sessionHandle, QueryHandle queryHandle, QueryConf newconf)
      throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      QueryContext ctx = getQueryContext(sessionHandle, queryHandle);
      if (ctx != null && ctx.getStatus().getStatus() == QueryStatus.Status.QUEUED) {
        ctx.updateConf(newconf.getProperties());
        notifyAllListeners();
        return true;
      } else {
        notifyAllListeners();
        return false;
      }
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public boolean updateQueryConf(GrillSessionHandle sessionHandle, QueryPrepareHandle prepareHandle, QueryConf newconf)
      throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      PreparedQueryContext ctx = getPreparedQueryContext(sessionHandle, prepareHandle);
      ctx.updateConf(newconf.getProperties());
      return true;
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public QueryContext getQueryContext(GrillSessionHandle sessionHandle, QueryHandle queryHandle)
      throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      QueryContext ctx = allQueries.get(queryHandle);
      if (ctx == null) {
        throw new NotFoundException("Query not found " + queryHandle);
      }
      updateStatus(queryHandle);
      return ctx;
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public PreparedQueryContext getPreparedQueryContext(GrillSessionHandle sessionHandle, 
      QueryPrepareHandle prepareHandle)
          throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      PreparedQueryContext ctx = preparedQueries.get(prepareHandle);
      if (ctx == null) {
        throw new NotFoundException("Prepared query not found " + prepareHandle);
      }
      return ctx;
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public QueryHandleWithResultSet execute(GrillSessionHandle sessionHandle, String query, long timeoutMillis,
      QueryConf conf) throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      QueryHandle handle = executeAsync(sessionHandle, query, conf);
      QueryHandleWithResultSet result = new QueryHandleWithResultSet(handle);
      // getQueryContext calls updateStatus, which fires query events if there's a change in status
      while (!getQueryContext(sessionHandle, handle).getStatus().getStatus().equals(
          QueryStatus.Status.LAUNCHED)) {
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
        result.SetResult("Finished");
      }
      return result;
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  class QueryCompletionListenerImpl implements QueryCompletionListener {
    QueryCompletionListenerImpl() {
    }
    @Override
    public void onCompletion(QueryHandle handle) {
      synchronized (this) {
        this.notify();
      }
    }

    @Override
    public void onError(QueryHandle handle, String error) {
      synchronized (this) {
        this.notify();
      }
    }
  }

  @Override
  public GrillResultSetMetadata getResultSetMetadata(GrillSessionHandle sessionHandle, QueryHandle queryHandle)
      throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      GrillResultSet resultSet = getResultset(queryHandle);
      if (resultSet != null) {
        return resultSet.getMetadata();
      }
      return null;
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public GrillResultSet fetchResultSet(GrillSessionHandle sessionHandle, QueryHandle queryHandle, long startIndex,
      int fetchSize) throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      GrillResultSet resultSet = getResultset(queryHandle);
      return resultSet;
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public void closeResultSet(GrillSessionHandle sessionHandle, QueryHandle queryHandle) throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      resultSets.remove(queryHandle);
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public boolean cancelQuery(GrillSessionHandle sessionHandle, QueryHandle queryHandle) throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      QueryContext ctx = getQueryContext(sessionHandle, queryHandle);
      if (ctx.getStatus().getStatus().equals(
          QueryStatus.Status.LAUNCHED)) {
        boolean ret = getQueryContext(sessionHandle, queryHandle).getSelectedDriver().cancelQuery(queryHandle);
        if (!ret) {
          return false;
        }
      } else {
        acceptedQueries.remove(ctx);
      }
      QueryStatus before = ctx.getStatus();
      ctx.setStatus(new QueryStatus(0.0, Status.CANCELED, "Cancelled", false));
      fireStatusChangeEvent(ctx, ctx.getStatus(), before);
      updateFinishedQuery(ctx);
      return true;
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public List<QueryHandle> getAllQueries(GrillSessionHandle sessionHandle, String state, String user)
      throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      List<QueryHandle> all = new ArrayList<QueryHandle>();
      all.addAll(allQueries.keySet());
      return all;
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public List<QueryPrepareHandle> getAllPreparedQueries(GrillSessionHandle sessionHandle, String user)
      throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      List<QueryPrepareHandle> allPrepared = new ArrayList<QueryPrepareHandle>();
      allPrepared.addAll(preparedQueries.keySet());
      return allPrepared;
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public boolean destroyPrepared(GrillSessionHandle sessionHandle, QueryPrepareHandle prepared)
      throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      PreparedQueryContext ctx = getPreparedQueryContext(sessionHandle, prepared);
      ctx.getSelectedDriver().closePreparedQuery(prepared);
      preparedQueries.remove(prepared);
      preparedQueryQueue.remove(ctx);
      return true;
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }

  @Override
  public QueryPlan explain(GrillSessionHandle sessionHandle, String query,
      QueryConf queryConf) throws GrillException {
    try {
      acquire(sessionHandle.getSessionHandle());
      Configuration qconf = getQueryConf(sessionHandle, queryConf);
      accept(query, qconf, SubmitOp.EXPLAIN);
      Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
          query, drivers);
      // select driver to run the query
      return driverSelector.select(drivers, driverQueries, conf).explain(query, qconf);
    } finally {
      release(sessionHandle.getSessionHandle());
    }
  }
}
