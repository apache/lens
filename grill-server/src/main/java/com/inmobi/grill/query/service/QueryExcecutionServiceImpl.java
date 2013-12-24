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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;

import com.inmobi.grill.api.DriverSelector;
import com.inmobi.grill.api.GrillConfConstants;
import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.GrillResultSetMetadata;
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
import com.inmobi.grill.server.api.GrillService;
import com.inmobi.grill.server.api.QueryExecutionService;

public class QueryExcecutionServiceImpl extends GrillService implements QueryExecutionService {
  static {
    Configuration.addDefaultResource("grill-default.xml");
    Configuration.addDefaultResource("grill-site.xml");
  }

  public static final Log LOG = LogFactory.getLog(QueryExcecutionServiceImpl.class);

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
  private List<QueryChangeListener> queryChangeListeners;
  private final List<GrillDriver> drivers = new ArrayList<GrillDriver>();
  private DriverSelector driverSelector;
  private Map<QueryHandle, GrillResultSet> resultSets = new HashMap<QueryHandle, GrillResultSet>();

  public QueryExcecutionServiceImpl(CLIService cliService) throws GrillException {
    super("query", cliService);
  }

  private void initializeQueryAcceptorsAndListeners() {
    //TODO
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
          ctx.setStatus(new QueryStatus(ctx.getStatus().getProgress(),
              QueryStatus.Status.LAUNCHED,
              "launched on the driver", false));
          launchedQueries.add(ctx);
          notifyAllListeners();
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

  private void updateStatus(QueryHandle handle) throws GrillException {
    QueryContext ctx = allQueries.get(handle);
    if (ctx != null) {
      if (!ctx.getStatus().getStatus().equals(QueryStatus.Status.QUEUED) &&
          !ctx.getStatus().isFinished()) {
        ctx.setStatus(ctx.getSelectedDriver().getStatus(ctx.getQueryHandle()));
        notifyAllListeners();
        if (ctx.getStatus().isFinished()) {
          updateFinishedQuery(ctx);
        }
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
      boolean accept = acceptor.doAccept(query, conf, submitOp, cause);
      if (!accept) {
        throw new GrillException("Query not accepted because " + cause);
      }
    }
    notifyAllListeners();
  }

  private Configuration getQueryConf(Configuration sessionConf, QueryConf queryConf) {
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
  public QueryPrepareHandle prepare(String query, QueryConf queryConf)
      throws GrillException {
    Configuration qconf = getQueryConf(this.conf, queryConf);
    accept(query, qconf, SubmitOp.PREPARE);
    PreparedQueryContext prepared = new PreparedQueryContext(query, null, qconf);
    rewriteAndSelect(prepared);
    preparedQueries.put(prepared.getPrepareHandle(), prepared);
    preparedQueryQueue.add(prepared);
    prepared.getSelectedDriver().prepare(prepared);
    System.out.println("################### returning " + prepared.getPrepareHandle());
    return prepared.getPrepareHandle();
  }

  @Override
  public QueryPlan explainAndPrepare(String query, QueryConf queryConf)
      throws GrillException {
    Configuration qconf = getQueryConf(this.conf, queryConf);
    accept(query, qconf, SubmitOp.EXPLAIN_AND_PREPARE);
    PreparedQueryContext prepared = new PreparedQueryContext(query, null, qconf);
    rewriteAndSelect(prepared);
    preparedQueries.put(prepared.getPrepareHandle(), prepared);
    preparedQueryQueue.add(prepared);
    return prepared.getSelectedDriver().explainAndPrepare(prepared);
  }

  @Override
  public QueryHandle executePrepareAsync(
      QueryPrepareHandle prepareHandle, QueryConf queryConf)
          throws GrillException {
    PreparedQueryContext pctx = getPreparedQueryContext(prepareHandle);
    Configuration qconf = getQueryConf(this.conf, queryConf);
    accept(pctx.getUserQuery(), qconf, SubmitOp.EXECUTE);
    QueryContext ctx = new QueryContext(pctx, "user", qconf);
    ctx.setStatus(new QueryStatus(0.0,
        QueryStatus.Status.QUEUED,
        "Query is queued", false));
    acceptedQueries.add(ctx);
    allQueries.put(ctx.getQueryHandle(), ctx);
    notifyAllListeners();
    LOG.info("Returning handle " + ctx.getQueryHandle().getHandleId());
    return ctx.getQueryHandle();
  }

  @Override
  public QueryHandle executeAsync(String query,
      QueryConf queryConf) throws GrillException {
    Configuration qconf = getQueryConf(this.conf, queryConf);
    accept(query, qconf, SubmitOp.EXECUTE);

    QueryContext ctx = new QueryContext(query, "user", qconf);
    acceptedQueries.add(ctx);
    ctx.setStatus(new QueryStatus(0.0,
        QueryStatus.Status.QUEUED,
        "Query is queued", false));
    allQueries.put(ctx.getQueryHandle(), ctx);
    notifyAllListeners();
    LOG.info("Returning handle " + ctx.getQueryHandle().getHandleId());
    return ctx.getQueryHandle();
  }

  @Override
  public boolean updateQueryConf(QueryHandle queryHandle, QueryConf newconf)
      throws GrillException {
    QueryContext ctx = getQueryContext(queryHandle);
    if (ctx != null && ctx.getStatus().getStatus() == QueryStatus.Status.QUEUED) {
      ctx.updateConf(newconf.getProperties());
      notifyAllListeners();
      return true;
    } else {
      notifyAllListeners();
      return false;
    }
  }

  @Override
  public boolean updateQueryConf(QueryPrepareHandle prepareHandle, QueryConf newconf)
      throws GrillException {
    PreparedQueryContext ctx = getPreparedQueryContext(prepareHandle);
    ctx.updateConf(newconf.getProperties());
    return true;
  }

  @Override
  public QueryContext getQueryContext(QueryHandle queryHandle)
      throws GrillException {
    QueryContext ctx = allQueries.get(queryHandle);
    if (ctx == null) {
      throw new NotFoundException("Query not found " + queryHandle);
    }
    updateStatus(queryHandle);
    return ctx;
  }

  @Override
  public PreparedQueryContext getPreparedQueryContext(
      QueryPrepareHandle prepareHandle)
          throws GrillException {
    PreparedQueryContext ctx = preparedQueries.get(prepareHandle);
    if (ctx == null) {
      throw new NotFoundException("Prepared query not found " + prepareHandle);
    }
    return ctx;
  }

  @Override
  public QueryHandleWithResultSet execute(String query, long timeoutMillis,
      QueryConf conf) throws GrillException {
    QueryHandle handle = executeAsync(query, conf);
    QueryHandleWithResultSet result = new QueryHandleWithResultSet(handle);
    while (!getQueryContext(handle).getStatus().getStatus().equals(
        QueryStatus.Status.LAUNCHED)) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    QueryCompletionListener listener = new QueryCompletionListenerImpl();
    getQueryContext(handle).getSelectedDriver().
        registerForCompletionNotification(handle, timeoutMillis, listener);
    try {
      synchronized (listener) {
        listener.wait(timeoutMillis);
      }
    } catch (InterruptedException e) {
      LOG.info("Waiting thread interrupted");
    }
    if (getQueryContext(handle).getStatus().isFinished()) {
      result.SetResult("Finished");
    }
    return result;
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
  public GrillResultSetMetadata getResultSetMetadata(QueryHandle queryHandle)
      throws GrillException {
    GrillResultSet resultSet = getResultset(queryHandle);
    if (resultSet != null) {
      return resultSet.getMetadata();
    }
    return null;
  }

  @Override
  public GrillResultSet fetchResultSet(QueryHandle queryHandle, long startIndex,
      int fetchSize) throws GrillException {
    GrillResultSet resultSet = getResultset(queryHandle);
    return resultSet;
  }

  @Override
  public void closeResultSet(QueryHandle queryHandle) throws GrillException {
    resultSets.remove(queryHandle);
  }

  @Override
  public boolean cancelQuery(QueryHandle queryHandle) throws GrillException {
    QueryContext ctx = getQueryContext(queryHandle);
    if (ctx.getStatus().getStatus().equals(
        QueryStatus.Status.LAUNCHED)) {
      boolean ret = getQueryContext(queryHandle).getSelectedDriver().cancelQuery(queryHandle);
      if (!ret) {
        return false;
      }
    } else {
      acceptedQueries.remove(ctx);
    }
    ctx.setStatus(new QueryStatus(0.0, Status.CANCELED, "Cancelled", false));
    updateFinishedQuery(ctx);
    return true;
  }

  @Override
  public List<QueryHandle> getAllQueries(String state, String user)
      throws GrillException {
    List<QueryHandle> all = new ArrayList<QueryHandle>();
    all.addAll(allQueries.keySet());
    return all;
  }

  @Override
  public List<QueryPrepareHandle> getAllPreparedQueries(String user)
      throws GrillException {
    List<QueryPrepareHandle> allPrepared = new ArrayList<QueryPrepareHandle>();
    allPrepared.addAll(preparedQueries.keySet());
    return allPrepared;
  }

  @Override
  public boolean destroyPrepared(QueryPrepareHandle prepared)
      throws GrillException {
    PreparedQueryContext ctx = getPreparedQueryContext(prepared);
    ctx.getSelectedDriver().closePreparedQuery(prepared);
    preparedQueries.remove(prepared);
    preparedQueryQueue.remove(ctx);
    return true;
  }

  @Override
  public QueryPlan explain(SessionHandle sessionHandle, String query,
      QueryConf queryConf) throws GrillException {
    HiveConf sessionConf;
    try {
      sessionConf = getSessionManager().getSession(sessionHandle).getHiveConf();
    } catch (HiveSQLException e) {
      throw new GrillException(e);
    }
    Configuration qconf = getQueryConf(sessionConf, queryConf);
    accept(query, qconf, SubmitOp.EXPLAIN);
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        query, drivers);
    // select driver to run the query
    return driverSelector.select(drivers, driverQueries, conf).explain(query, qconf);
  }
}
