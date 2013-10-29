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

import com.inmobi.grill.api.DriverSelector;
import com.inmobi.grill.api.GrillConfConstants;
import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.PreparedQueryContext;
import com.inmobi.grill.api.QueryContext;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QueryHandleWithResultSet;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.client.api.QueryResult;
import com.inmobi.grill.client.api.QueryResultSetMetadata;
import com.inmobi.grill.driver.cube.CubeGrillDriver;
import com.inmobi.grill.driver.cube.RewriteUtil;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.QueryExecutionService;

public class QueryExcecutionServiceImpl implements QueryExecutionService, Configurable {
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

  public QueryExcecutionServiceImpl() throws GrillException {
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
        }
      }
    }
  }

  private class StatusPoller implements Runnable {
    long pollInterval = 1000;
    @Override
    public void run() {
      while (!stopped && !statusPoller.isInterrupted()) {
        try {
          for (QueryContext ctx : launchedQueries) {
            try {
              updateStatus(ctx.getQueryHandle());
            } catch (GrillException e) {
              LOG.error("Error updating status ", e);
            }
          }
          Thread.sleep(pollInterval);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          return;
        }
      }
    }
  }

  private void updateStatus(QueryHandle handle) throws GrillException {
    QueryContext ctx = allQueries.get(handle);
    if (!ctx.getStatus().getStatus().equals(QueryStatus.Status.QUEUED) &&
        !ctx.getStatus().isFinished()) {
      ctx.setStatus(ctx.getSelectedDriver().getStatus(ctx.getQueryHandle()));
      notifyAllListeners();
      if (ctx.getStatus().isFinished()) {
        finishedQueries.add(new FinishedQuery(ctx));
        notifyAllListeners();
      }
    }
  }

  private class QueryPurger implements Runnable {
    @Override
    public void run() {
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
          return;
        }
      }
    }
  }

  private class PreparedQueryPurger implements Runnable {
    @Override
    public void run() {
      while (!stopped && !prepareQueryPurger.isInterrupted()) {
        try {
          PreparedQueryContext prepared = preparedQueryQueue.take();
          prepared.getSelectedDriver().closePreparedQuery(prepared.getPrepareHandle());
          preparedQueries.remove(prepared.getPrepareHandle());
          notifyAllListeners();
        } catch (GrillException e) {
          LOG.error("Error closing prepared query ", e);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  public void notifyAllListeners() throws GrillException {
    //TODO 
  }

  @Override
  public String getName() {
    return "query";
  }

  @Override
  public void init() throws GrillException {
    initializeQueryAcceptorsAndListeners();
    loadDriversAndSelector();
  }

  @Override
  public void start() throws GrillException {
    querySubmitter.start();
    statusPoller.start();
    queryPurger.start();
    prepareQueryPurger.start();
  }

  @Override
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

  private Configuration getQueryConf(QueryConf queryConf) {
    Configuration qconf = this.conf;
    if (queryConf != null && !queryConf.getProperties().isEmpty()) {
      qconf = new Configuration(this.conf);
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
      resultSet = allQueries.get(queryHandle).getSelectedDriver().
          fetchResultSet(allQueries.get(queryHandle));
      resultSets.put(queryHandle, resultSet);
    }   
    return resultSets.get(queryHandle);
  }

  @Override
  public QueryPlan explain(String query, QueryConf queryConf)
      throws GrillException {
    Configuration qconf = getQueryConf(queryConf);
    accept(query, qconf, SubmitOp.EXPLAIN);
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        query, drivers);
    // select driver to run the query
    return driverSelector.select(drivers, driverQueries, conf).explain(query, qconf);
  }

  @Override
  public QueryPrepareHandle prepare(String query, QueryConf queryConf)
      throws GrillException {
    Configuration qconf = getQueryConf(queryConf);
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
    Configuration qconf = getQueryConf(queryConf);
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
    Configuration qconf = getQueryConf(queryConf);
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
    Configuration qconf = getQueryConf(queryConf);
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
  public QueryHandleWithResultSet execute(String query, long timeoutmillis,
      QueryConf conf) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryResultSetMetadata getResultSetMetadata(QueryHandle queryHandle)
      throws GrillException {
    // TODO Auto-generated method stub
    GrillResultSet resultSet = getResultset(queryHandle);
    return null;
  }

  @Override
  public QueryResult fetchResultSet(QueryHandle queryHandle, long startIndex,
      int fetchSize) throws GrillException {
    // TODO Auto-generated method stub
    GrillResultSet resultSet = getResultset(queryHandle);
    return null;
  }

  @Override
  public void closeResultSet(QueryHandle queryHandle) throws GrillException {
    resultSets.remove(queryHandle);
  }

  @Override
  public boolean cancelQuery(QueryHandle queryHandle) throws GrillException {
    return getQueryContext(queryHandle).getSelectedDriver().cancelQuery(queryHandle);
  }

  @Override
  public List<QueryHandle> getAllQueries(String state, String user)
      throws GrillException {
    List<QueryHandle> all = new ArrayList<QueryHandle>();
    all.addAll(allQueries.keySet());
    return all;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

  }

  @Override
  public Configuration getConf() {
    return this.conf;
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
}
