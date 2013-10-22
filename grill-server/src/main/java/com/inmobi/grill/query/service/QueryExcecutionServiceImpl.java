package com.inmobi.grill.query.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.client.api.APIResult;
import com.inmobi.grill.client.api.QueryConf;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.client.api.QueryList;
import com.inmobi.grill.client.api.QueryPlan;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.client.api.QueryResult;
import com.inmobi.grill.client.api.QueryResultSetMetadata;
import com.inmobi.grill.api.QueryHandleWithResultSet;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.driver.cube.CubeGrillDriver;
import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.QueryExecutionService;

public class QueryExcecutionServiceImpl implements QueryExecutionService {
  public static final Log LOG = LogFactory.getLog(QueryExcecutionServiceImpl.class);

  public enum Priority {
    VERY_HIGH,
    HIGH,
    NORMAL,
    LOW,
    VERY_LOW
  }
  private static long millisInWeek = 7 * 24 * 60 * 60 * 1000;

  private PriorityBlockingQueue<QueryContext> acceptedQueries =
      new PriorityBlockingQueue<QueryContext>();
  private List<QueryContext> launchedQueries = new ArrayList<QueryContext>();
  private DelayQueue<FinishedQuery> finishedQueries =
      new DelayQueue<FinishedQuery>();
  private Map<QueryHandle, QueryContext> allQueries = new HashMap<QueryHandle, QueryContext>();
  private final Configuration conf;
  private CubeGrillDriver cubeDriver;
  private final Thread querySubmitter = new Thread(new QuerySubmitter(),
      "QuerySubmitter");
  private final Thread statusPoller = new Thread(new StatusPoller(),
      "StatusPoller");
  private final Thread queryPurger = new Thread(new QueryPurger(),
      "QueryPurger");
  private boolean stopped = false;
  private List<QueryAcceptor> queryAcceptors;
  private List<QueryChangeListener> queryChangeListeners;
  
  QueryExcecutionServiceImpl(Configuration conf) throws GrillException{
    this.conf = conf;
    //this.cubeDriver = new CubeGrillDriver(conf);
    initializeQueryAcceptorsAndListeners();
  }

  private void initializeQueryAcceptorsAndListeners() {
    //TODO
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

  public QueryHandle accept(String query, Configuration conf)
      throws GrillException {
    notifyAllListeners();
    // run through all the query acceptors, and throw Exception if any of them
    // return false
  /*  for (QueryAcceptor acceptor : queryAcceptors) {
      String cause = "";
      boolean accept = acceptor.doAccept(query, conf, cause);
      if (!accept) {
        throw new GrillException("Query not accepted because " + cause);
      }
    }*/
    notifyAllListeners();
    QueryContext ctx = new QueryContext(query, "user", conf);
    acceptedQueries.add(ctx);
    ctx.setStatus(new QueryStatus(0.0,
        com.inmobi.grill.api.QueryStatus.Status.QUEUED,
        "Query is queued", false));
   // allQueries.put(ctx.getQueryHandle(), ctx);
    notifyAllListeners();
    System.out.println("Returning handle " + ctx.getQueryHandle().getHandleId());
    return ctx.getQueryHandle();
  }

  private class QuerySubmitter implements Runnable {
    @Override
    public void run() {
      while (!stopped && !querySubmitter.isInterrupted()) {
        try {
          QueryContext ctx = acceptedQueries.take();
          notifyAllListeners();
//          QueryHandle handle = cubeDriver.executeAsync(ctx.getUserQuery(),
//              ctx.getConf());
//          ctx.setStatus(cubeDriver.getStatus(handle));
          launchedQueries.add(ctx);
          notifyAllListeners();
        } catch (GrillException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }

  private class StatusPoller implements Runnable {
    long pollInterval = 10;
    @Override
    public void run() {
      while (!stopped && !statusPoller.isInterrupted()) {
        try {
          for (QueryContext ctx : launchedQueries) {
            try {
              notifyAllListeners();
              //ctx.setStatus(cubeDriver.getStatus(ctx.getQueryHandle()));
              notifyAllListeners();
              if (ctx.getStatus().isFinished()) {
                finishedQueries.add(new FinishedQuery(ctx));
                notifyAllListeners();
              }
            } catch (GrillException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
          Thread.sleep(pollInterval);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          return;
        }
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
          purgeQuery(finished.getCtx());
          notifyAllListeners();
        } catch (GrillException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          return;
        }
      }
    }
  }

  public void purgeQuery(QueryContext ctx) throws GrillException {
    //cubeDriver.closeQuery(ctx.getQueryHandle());
  }

  public void notifyAllListeners() throws GrillException {
    //TODO 
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void start() throws GrillException {
    // TODO Auto-generated method stub
    querySubmitter.start();
    statusPoller.start();
    queryPurger.start();
  }

  @Override
  public void stop() throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public QueryPlan explain(String query, QueryConf conf) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryPrepareHandle prepare(String query, QueryConf conf) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryPlan explainAndPrepare(String query, QueryConf conf)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryHandle executePrepareAsync(
      String prepareHandle, QueryConf conf)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;    
  }

  @Override
  public QueryHandle executeAsync(String query,
      QueryConf conf) throws GrillException {
    return accept(query, this.conf);
    //TODO
  }

  @Override
  public boolean updateQueryConf(String queryHandle, QueryConf newconf)
      throws GrillException {
    // TODO Auto-generated method stub
    return true;
  }

  @Override
  public QueryHandleWithResultSet execute(String query, long timeoutmillis,
      QueryConf conf) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryStatus getStatus(String queryHandle)
      throws GrillException {
    return new QueryStatus(1.0, QueryStatus.Status.SUCCESSFUL, "dummy query success", false);
  }

  @Override
  public QueryResultSetMetadata getResultSetMetadata(String queryHandle)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryResult fetchResultSet(String queryHandle, long startIndex,
      int fetchSize) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void closeResultSet(String queryHandle) throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean cancelQuery(String queryHandle) throws GrillException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public QueryList getAllQueries(String state, String user)
      throws GrillException {
    // TODO Auto-generated method stub
    return new QueryList(APIResult.Status.SUCCEEDED, "success");
  }
}
