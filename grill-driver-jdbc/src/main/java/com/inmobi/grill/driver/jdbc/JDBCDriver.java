package com.inmobi.grill.driver.jdbc;


import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.server.api.driver.DriverQueryPlan;
import com.inmobi.grill.server.api.driver.GrillDriver;
import com.inmobi.grill.server.api.driver.GrillResultSet;
import com.inmobi.grill.server.api.driver.QueryCompletionListener;
import com.inmobi.grill.server.api.query.PreparedQueryContext;
import com.inmobi.grill.server.api.query.QueryContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.inmobi.grill.driver.jdbc.JDBCDriverConfConstants.*;
/**
 * This driver is responsible for running queries against databases which can be queried using the JDBC API.
 */
public class JDBCDriver implements GrillDriver {
  public static final Logger LOG = Logger.getLogger(JDBCDriver.class);
  public static final AtomicInteger thid = new AtomicInteger();

  private ExecutorService asyncQueryPool;
  private ConcurrentHashMap<QueryHandle, JdbcQueryContext> queryContextMap;
  private ConcurrentHashMap<Class<? extends QueryRewriter>, QueryRewriter> rewriterCache;
  private Configuration conf;
  
  /**
   * Data related to a query submitted to JDBCDriver
   */
  protected class JdbcQueryContext {
    private QueryContext grillContext;
    private Future<QueryResult> resultFuture;
    private String rewrittenQuery;
    private boolean isPrepared = false;
    private QueryCompletionListener listener;
    
    public boolean isPrepared() {
      return isPrepared;
    }

    public void setPrepared(boolean isPrepared) {
      this.isPrepared = isPrepared;
    }

    public QueryContext getGrillContext() {
      return grillContext;
    }

    public void setGrillContext(QueryContext grillContext) {
      this.grillContext = grillContext;
    }

    public Future<QueryResult> getResultFuture() {
      return resultFuture;
    }

    public void setResultFuture(Future<QueryResult> resultFuture) {
      this.resultFuture = resultFuture;
    }

    public String getRewrittenQuery() {
      return rewrittenQuery;
    }

    public void setRewrittenQuery(String rewrittenQuery) {
      this.rewrittenQuery = rewrittenQuery;
    }
    
    public void setCompletionListener(QueryCompletionListener listener) {
      this.listener = listener;
    }
    
    public void notifyError(Throwable th) {
      if (listener != null) {
        listener.onError(grillContext.getQueryHandle(), th.getMessage());
      }
    }
    
    public void notifyComplete() {
      if (listener != null) {
        listener.onCompletion(grillContext.getQueryHandle());
      }
    }
  }

  /**
   * Result of a query and associated resources like statement and connection.
   * After the results are consumed, close() should be called to close the statement and connection
   */
  protected class QueryResult {
    private ResultSet resultSet;
    private Throwable error;
    private Connection conn;
    private Statement stmt;

    public ResultSet getResultSet() throws GrillException {
      if (error != null) {
        throw new GrillException(error);
      }
      return resultSet;
    }
    
    protected void close() {
      try {
        if (stmt != null) {
          try {
            stmt.close();
          } catch (SQLException e) {
            LOG.error("Error closing SQL statement", e);
          }
        }
      } finally {
        if (conn != null) {
          try {
            conn.close();
          } catch (SQLException e) {
            LOG.error("Error closing SQL Connection", e);
          }
        }
      }
    }
    
    protected GrillResultSet createResultSet() {
      return new JdbcResultSet(resultSet);
    }
  }

  /**
   * Callabled that returns query result after running the query. This is used for async queries.
   */
  protected class QueryCallable implements Callable<QueryResult> {
    private final JdbcQueryContext queryContext;
    public QueryCallable(JdbcQueryContext queryContext) {
      this.queryContext = queryContext;
    }

    @Override
    public QueryResult call() {
      Statement stmt = null;
      Connection conn = null;
      QueryResult result = new QueryResult();
      try {
        conn = getConnection(queryContext.getGrillContext().getConf());
        result.conn = conn;
      } catch (GrillException e) {
        LOG.error("Error obtaining connection: " + e.getMessage(), e);
        result.error = e;
      }

      if (conn != null) {
        try {
          stmt = getStatement(conn);
          result.stmt = stmt;
          result.resultSet = stmt.executeQuery(queryContext.getRewrittenQuery());
          queryContext.notifyComplete();
        } catch (SQLException sqlEx) {
          LOG.error("Error executing SQL query: " + sqlEx.getMessage(), sqlEx);
          result.error = sqlEx;
          queryContext.notifyError(sqlEx);
        }
      }
      return result;
    }
    
    public Statement getStatement(Connection conn) throws SQLException {
      Statement stmt = 
          queryContext.isPrepared() ? conn.prepareStatement(queryContext.getRewrittenQuery())
              : conn.createStatement();
      stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
      return stmt;
    }
  }
  
  protected class DummyQueryRewriter implements QueryRewriter {
    @Override
    public String rewrite(Configuration conf, String query) throws GrillException {
      return query;
    }
  }

  /**
   * Get driver configuration
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Configure driver with {@link org.apache.hadoop.conf.Configuration} passed
   *
   * @param conf The configuration object
   */
  @Override
  public void configure(Configuration conf) throws GrillException {
    this.conf = conf;
    init(conf);
  }

  protected void init(Configuration conf) {
    queryContextMap = new ConcurrentHashMap<QueryHandle, JdbcQueryContext>();
    rewriterCache = new ConcurrentHashMap<Class<? extends QueryRewriter>, QueryRewriter>();
    asyncQueryPool = Executors.newCachedThreadPool(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread th = new Thread(runnable);
        th.setName("grill-driver-jdbc-" + thid.incrementAndGet());
        return th;
      }
    });
  }

  protected synchronized Connection getConnection(Configuration conf) throws GrillException {
    //TODO
    return null;
  }

  protected synchronized QueryRewriter getQueryRewriter(Configuration conf) throws GrillException {
    QueryRewriter rewriter;
    Class<? extends QueryRewriter> queryRewriterClass = 
        conf.getClass(JDBC_QUERY_REWRITER_CLASS, DummyQueryRewriter.class, QueryRewriter.class);
    if (rewriterCache.containsKey(queryRewriterClass)) {
      rewriter =  rewriterCache.get(queryRewriterClass);
    } else {
      try {
        rewriter = queryRewriterClass.newInstance();
      } catch (Exception e) {
        LOG.error("Unable to create rewriter object", e);
        throw new GrillException(e);
      }
      rewriterCache.put(queryRewriterClass, rewriter);
    }
    return rewriter;
  }
  
  protected JdbcQueryContext getQueryContext(QueryHandle handle) throws GrillException {
     JdbcQueryContext ctx = queryContextMap.get(handle);
     if (ctx == null) {
       throw new GrillException("Query not found:" + handle.getHandleId());
     }
     return ctx;
  }

  protected String rewriteQuery(String query, Configuration conf) throws GrillException {
    QueryRewriter rewriter = getQueryRewriter(conf);
    String rewrittenQuery = rewriter.rewrite(conf, query);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Query: " + query + " rewrittenQuery: " + rewrittenQuery);
    }
    return rewrittenQuery;
  }

  /**
   * Explain the given query
   *
   * @param query The query should be in HiveQL(SQL like)
   * @param conf  The query configuration
   * @return The query plan object;
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public DriverQueryPlan explain(String query, Configuration conf) throws GrillException {
    //TODO
    return null;
  }

  /**
   * Prepare the given query
   *
   * @param pContext
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public void prepare(PreparedQueryContext pContext) throws GrillException {
    // Only create a prepared statement and then close it
    String rewrittenQuery = rewriteQuery(pContext.getUserQuery(), pContext.getConf());
    Connection conn = null;
    PreparedStatement stmt = null;
    try {
      conn = getConnection(pContext.getConf());
      stmt = conn.prepareStatement(rewrittenQuery);
    } catch (SQLException sql) {
      throw new GrillException(sql);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          LOG.error("Error closing statement: " + pContext.getPrepareHandle(), e);
        }
      }
      
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          LOG.error("Error closing connection: " + pContext.getPrepareHandle(), e);
        }
      }
    }
  }

  /**
   * Explain and prepare the given query
   *
   * @param pContext
   * @return The query plan object;
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws GrillException {
    //TODO
    return null;
  }

  /**
   * Close the prepare query specified by the prepared handle,
   * releases all the resources held by the prepared query.
   *
   * @param handle The query handle
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public void closePreparedQuery(QueryPrepareHandle handle) throws GrillException {
    // Do nothing
  }

  /**
   * Blocking execute of the query
   *
   * @param context
   * @return returns the result set
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public GrillResultSet execute(QueryContext context) throws GrillException {
    String rewrittenQuery = rewriteQuery(context.getUserQuery(), context.getConf());
    JdbcQueryContext queryContext = new JdbcQueryContext();
    queryContext.setPrepared(false);
    queryContext.setGrillContext(context);
    queryContext.setRewrittenQuery(rewrittenQuery);
    QueryResult result = new QueryCallable(queryContext).call();
    LOG.info("Execute " + context.getQueryHandle());
    return result.createResultSet();
  }

  /**
   * Asynchronously execute the query
   *
   * @param context The query context
   * @return a query handle, which can used to know the status.
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public void executeAsync(QueryContext context) throws GrillException {
    String rewrittenQuery = rewriteQuery(context.getUserQuery(), context.getConf());
    JdbcQueryContext jdbcCtx = new JdbcQueryContext();
    jdbcCtx.setGrillContext(context);
    jdbcCtx.setRewrittenQuery(rewrittenQuery);
    try {
      Future<QueryResult> future = asyncQueryPool.submit(new QueryCallable(jdbcCtx));
      jdbcCtx.setResultFuture(future);
    } catch (RejectedExecutionException e) {
      LOG.error("Query execution rejected: " + context.getQueryHandle() + " reason:" 
          + e.getMessage(), e);
      throw new GrillException("Query execution rejected: " + context.getQueryHandle() + " reason:" 
          + e.getMessage(), e);
    }
    queryContextMap.put(context.getQueryHandle(), jdbcCtx);
  }

  /**
   * Register for query completion notification
   *
   * @param handle
   * @param timeoutMillis
   * @param listener
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public void registerForCompletionNotification(QueryHandle handle, long timeoutMillis, 
      QueryCompletionListener listener) throws GrillException {
    getQueryContext(handle).setCompletionListener(listener);
  }

  /**
   * Get status of the query, specified by the handle
   *
   * @param handle The query handle
   * @return query status
   */
  @Override
  public QueryStatus getStatus(QueryHandle handle) throws GrillException {
    JdbcQueryContext ctx = getQueryContext(handle);

    if (ctx.getResultFuture().isDone()) {
      try {
        QueryResult result = ctx.getResultFuture().get();
        if (result.error != null) {
          return new QueryStatus(1.0, Status.FAILED, result.error.getMessage(), false);
        }
        return new QueryStatus(1.0, Status.SUCCESSFUL, handle.getHandleId() + " successful", true);
      } catch (InterruptedException e) {
        throw new GrillException("Unexpected interrupt", e);
      } catch (ExecutionException e) {
        return new QueryStatus(1.0, Status.FAILED, e.getMessage(), false);
      }
    } else {
      return new QueryStatus(0.0, Status.RUNNING, handle.getHandleId() + " is running", false);
    }
  }

  /**
   * Fetch the results of the query, specified by the handle
   *
   * @param context@return returns the result set
   */
  @Override
  public GrillResultSet fetchResultSet(QueryContext context) throws GrillException {
    JdbcQueryContext ctx = getQueryContext(context.getQueryHandle());
    QueryResult result = blockingGetResult(ctx);
    return result.createResultSet();
  }

  protected QueryResult blockingGetResult(JdbcQueryContext context) throws GrillException {
    Future<QueryResult> future = context.getResultFuture();
    QueryHandle queryHandle = context.getGrillContext().getQueryHandle();
    Configuration conf = context.getGrillContext().getConf();
    long timeout = conf.getInt(JDBC_RESULTSET_WAIT_TIMEOUT, Integer.MAX_VALUE);
    try {
      
      return future.get(timeout, TimeUnit.SECONDS);
    
    } catch (InterruptedException e) {
      throw new GrillException("Interrupted while getting resultset for query "
          + queryHandle.getHandleId(), e);
    } catch (ExecutionException e) {
      throw new GrillException("Error while executing query "
          + queryHandle.getHandleId() + " in background", e);
    } catch (TimeoutException e) {
      throw new GrillException("Timed out while getting resultset of query "
          + queryHandle.getHandleId(), e);
    } catch (CancellationException e) {
      throw new GrillException("Query was already cancelled "
          + queryHandle.getHandleId(), e);
    }
  }

  /**
   * Close the resultset for the query
   *
   * @param handle The query handle
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public void closeResultSet(QueryHandle handle) throws GrillException {
    blockingGetResult(getQueryContext(handle)).close();
  }

  /**
   * Cancel the execution of the query, specified by the handle
   *
   * @param handle The query handle.
   * @return true if cancel was successful, false otherwise
   */
  @Override
  public boolean cancelQuery(QueryHandle handle) throws GrillException {
    LOG.info("Cancelling query: " + handle);
    return getQueryContext(handle).getResultFuture().cancel(true);
  }

  /**
   * Close the query specified by the handle, releases all the resources
   * held by the query.
   *
   * @param handle The query handle
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public void closeQuery(QueryHandle handle) throws GrillException {
    try {
      LOG.info("Closing query " + handle.getHandleId());
      JdbcQueryContext ctx = getQueryContext(handle);
      ctx.getResultFuture().cancel(true);
      blockingGetResult(ctx).close();
    } finally {
      queryContextMap.remove(handle);
    }
  }

  /**
   * Close the driver, releasing all resouces used up by the driver
   *
   * @throws com.inmobi.grill.api.GrillException
   */
  @Override
  public void close() throws GrillException {
    List<QueryHandle> toClose = new ArrayList<QueryHandle>(queryContextMap.keySet());
    
    try {
      for (QueryHandle query : toClose) {
        try {
          closeQuery(query);
        } catch (GrillException e) {
          LOG.warn("Error closing query : " + query.getHandleId(), e);
        }
      }
    } finally {
      queryContextMap.clear();
    }
  }
}
