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
package org.apache.lens.driver.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryCost;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.driver.cube.RewriteUtil;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;
import lombok.Setter;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TMP_FILE;
import static org.apache.lens.driver.jdbc.JDBCDriverConfConstants.*;

/**
 * This driver is responsible for running queries against databases which can be queried using the JDBC API.
 */
public class JDBCDriver implements LensDriver {

  /** The Constant LOG. */
  public static final Logger LOG = Logger.getLogger(JDBCDriver.class);

  /** The Constant thid. */
  public static final AtomicInteger thid = new AtomicInteger();

  /** The connection provider. */
  private ConnectionProvider connectionProvider;

  /** The configured. */
  boolean configured = false;

  /** The async query pool. */
  private ExecutorService asyncQueryPool;

  /** The query context map. */
  private ConcurrentHashMap<QueryHandle, JdbcQueryContext> queryContextMap;

  /** The rewriter cache. */
  private ConcurrentHashMap<Class<? extends QueryRewriter>, QueryRewriter> rewriterCache;

  /** The conf. */
  private Configuration conf;

  /**
   * Data related to a query submitted to JDBCDriver.
   */
  protected class JdbcQueryContext {

    /** The lens context. */
    @Getter
    private final QueryContext lensContext;

    /** The result future. */
    @Getter
    @Setter
    private Future<QueryResult> resultFuture;

    /** The rewritten query. */
    @Getter
    @Setter
    private String rewrittenQuery;

    /** The is prepared. */
    @Getter
    @Setter
    private boolean isPrepared;

    /** The is cancelled. */
    @Getter
    @Setter
    private boolean isCancelled;

    /** The is closed. */
    @Getter
    private boolean isClosed;

    /** The listener. */
    @Getter
    @Setter
    private QueryCompletionListener listener;

    /** The query result. */
    @Getter
    @Setter
    private QueryResult queryResult;

    /** The start time. */
    @Getter
    @Setter
    private long startTime;

    /** The end time. */
    @Getter
    @Setter
    private long endTime;

    /**
     * Instantiates a new jdbc query context.
     *
     * @param context
     *          the context
     */
    public JdbcQueryContext(QueryContext context) {
      this.lensContext = context;
    }

    /**
     * Notify error.
     *
     * @param th
     *          the th
     */
    public void notifyError(Throwable th) {
      // If query is closed in another thread while the callable is still waiting for result
      // set, then it throws an SQLException in the callable. We don't want to send that exception
      if (listener != null && !isClosed) {
        listener.onError(lensContext.getQueryHandle(), th.getMessage());
      }
    }

    /**
     * Notify complete.
     */
    public void notifyComplete() {
      if (listener != null) {
        listener.onCompletion(lensContext.getQueryHandle());
      }
    }

    /**
     * Close result.
     */
    public void closeResult() {
      if (queryResult != null) {
        queryResult.close();
      }
      isClosed = true;
    }
  }

  /**
   * Result of a query and associated resources like statement and connection. After the results are consumed, close()
   * should be called to close the statement and connection
   */
  protected class QueryResult {

    /** The result set. */
    private ResultSet resultSet;

    /** The error. */
    private Throwable error;

    /** The conn. */
    private Connection conn;

    /** The stmt. */
    private Statement stmt;

    /** The is closed. */
    private boolean isClosed;

    /** The lens result set. */
    private JDBCResultSet lensResultSet;

    /**
     * Close.
     */
    protected synchronized void close() {
      if (isClosed) {
        return;
      }

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
      isClosed = true;
    }

    /**
     * Gets the lens result set.
     *
     * @param closeAfterFetch
     *          the close after fetch
     * @return the lens result set
     * @throws LensException
     *           the lens exception
     */
    protected synchronized LensResultSet getLensResultSet(boolean closeAfterFetch) throws LensException {
      if (error != null) {
        throw new LensException("Query failed!", error);
      }
      if (lensResultSet == null) {
        lensResultSet = new JDBCResultSet(this, resultSet, closeAfterFetch);
      }
      return lensResultSet;
    }
  }

  /**
   * Callabled that returns query result after running the query. This is used for async queries.
   */
  protected class QueryCallable implements Callable<QueryResult> {

    /** The query context. */
    private final JdbcQueryContext queryContext;

    /**
     * Instantiates a new query callable.
     *
     * @param queryContext
     *          the query context
     */
    public QueryCallable(JdbcQueryContext queryContext) {
      this.queryContext = queryContext;
      queryContext.setStartTime(System.currentTimeMillis());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public QueryResult call() {
      Statement stmt = null;
      Connection conn = null;
      QueryResult result = new QueryResult();
      try {
        queryContext.setQueryResult(result);

        try {
          conn = getConnection();
          result.conn = conn;
        } catch (LensException e) {
          LOG.error("Error obtaining connection: " + e.getMessage(), e);
          result.error = e;
        }

        if (conn != null) {
          try {
            stmt = getStatement(conn);
            result.stmt = stmt;
            Boolean isResultAvailable = stmt.execute(queryContext.getRewrittenQuery());
            if (isResultAvailable) {
              result.resultSet = stmt.getResultSet();
            }
            queryContext.notifyComplete();
          } catch (SQLException sqlEx) {
            if (queryContext.isClosed()) {
              LOG.info("Ignored exception on already closed query: " + queryContext.getLensContext().getQueryHandle()
                  + " - " + sqlEx);
            } else {
              LOG.error("Error executing SQL query: " + queryContext.getLensContext().getQueryHandle() + " reason: "
                  + sqlEx.getMessage(), sqlEx);
              result.error = sqlEx;
              // Close connection in case of failed queries. For successful queries, connection is closed
              // When result set is closed or driver.closeQuery is called
              result.close();
              queryContext.notifyError(sqlEx);
            }
          }
        }
      } finally {
        queryContext.setEndTime(System.currentTimeMillis());
      }
      return result;
    }

    /**
     * Gets the statement.
     *
     * @param conn
     *          the conn
     * @return the statement
     * @throws SQLException
     *           the SQL exception
     */
    public Statement getStatement(Connection conn) throws SQLException {
      Statement stmt = queryContext.isPrepared() ? conn.prepareStatement(queryContext.getRewrittenQuery()) : conn
          .createStatement();
      stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
      return stmt;
    }
  }

  /**
   * The Class DummyQueryRewriter.
   */
  public static class DummyQueryRewriter implements QueryRewriter {

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.lens.driver.jdbc.QueryRewriter#rewrite(org.apache.hadoop.conf.Configuration, java.lang.String)
     */
    @Override
    public String rewrite(Configuration conf, String query) throws LensException {
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
   * @param conf
   *          The configuration object
   * @throws LensException
   *           the lens exception
   */
  @Override
  public void configure(Configuration conf) throws LensException {
    this.conf = new Configuration(conf);
    this.conf.addResource("jdbcdriver-default.xml");
    this.conf.addResource("jdbcdriver-site.xml");
    init(conf);
    configured = true;
    LOG.info("JDBC Driver configured");
  }

  /**
   * Inits the.
   *
   * @param conf
   *          the conf
   * @throws LensException
   *           the lens exception
   */
  protected void init(Configuration conf) throws LensException {
    queryContextMap = new ConcurrentHashMap<QueryHandle, JdbcQueryContext>();
    rewriterCache = new ConcurrentHashMap<Class<? extends QueryRewriter>, QueryRewriter>();
    asyncQueryPool = Executors.newCachedThreadPool(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread th = new Thread(runnable);
        th.setName("lens-driver-jdbc-" + thid.incrementAndGet());
        return th;
      }
    });

    Class<? extends ConnectionProvider> cpClass = conf.getClass(JDBC_CONNECTION_PROVIDER,
        DataSourceConnectionProvider.class, ConnectionProvider.class);
    try {
      connectionProvider = cpClass.newInstance();
    } catch (Exception e) {
      LOG.error("Error initializing connection provider: " + e.getMessage(), e);
      throw new LensException(e);
    }
  }

  /**
   * Check configured.
   *
   * @throws IllegalStateException
   *           the illegal state exception
   */
  protected void checkConfigured() throws IllegalStateException {
    if (!configured) {
      throw new IllegalStateException("JDBC Driver is not configured!");
    }
  }

  protected synchronized Connection getConnection() throws LensException {
    try {
      // Add here to cover the path when the queries are executed it does not
      // use the driver conf
      return connectionProvider.getConnection(conf);
    } catch (SQLException e) {
      throw new LensException(e);
    }
  }

  /**
   * Gets the query rewriter.
   *
   * @param conf
   *          the conf
   * @return the query rewriter
   * @throws LensException
   *           the lens exception
   */
  protected synchronized QueryRewriter getQueryRewriter(Configuration conf) throws LensException {
    QueryRewriter rewriter;
    Class<? extends QueryRewriter> queryRewriterClass = conf.getClass(JDBC_QUERY_REWRITER_CLASS,
        DummyQueryRewriter.class, QueryRewriter.class);
    if (rewriterCache.containsKey(queryRewriterClass)) {
      rewriter = rewriterCache.get(queryRewriterClass);
    } else {
      try {
        rewriter = queryRewriterClass.newInstance();
        LOG.info("Initialized :" + queryRewriterClass);
      } catch (Exception e) {
        LOG.error("Unable to create rewriter object", e);
        throw new LensException(e);
      }
      rewriterCache.put(queryRewriterClass, rewriter);
    }
    return rewriter;
  }

  /**
   * Gets the query context.
   *
   * @param handle
   *          the handle
   * @return the query context
   * @throws LensException
   *           the lens exception
   */
  protected JdbcQueryContext getQueryContext(QueryHandle handle) throws LensException {
    JdbcQueryContext ctx = queryContextMap.get(handle);
    if (ctx == null) {
      throw new LensException("Query not found:" + handle.getHandleId());
    }
    return ctx;
  }

  /**
   * Rewrite query.
   *
   * @param query
   *          the query
   * @param conf
   *          the conf
   * @return the string
   * @throws LensException
   *           the lens exception
   */
  protected String rewriteQuery(String query, Configuration conf) throws LensException {
    // check if it is select query
    try {
      ASTNode ast = HQLParser.parseHQL(query);
      if (ast.getToken().getType() != HiveParser.TOK_QUERY) {
        throw new LensException("Not allowed statement:" + query);
      } else {
        // check for insert clause
        ASTNode dest = HQLParser.findNodeByPath(ast, HiveParser.TOK_INSERT);
        if (dest != null && ((ASTNode) (dest.getChild(0).getChild(0).getChild(0))).getToken().getType() != TOK_TMP_FILE) {
          throw new LensException("Not allowed statement:" + query);
        }
      }
    } catch (ParseException e) {
      throw new LensException(e);
    }

    QueryRewriter rewriter = getQueryRewriter(conf);
    String rewrittenQuery = rewriter.rewrite(conf, query);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Query: " + query + " rewrittenQuery: " + rewrittenQuery);
    }
    return rewrittenQuery;
  }

  /**
   * Dummy JDBC query Plan class to get min cost selector working.
   */
  private static class JDBCQueryPlan extends DriverQueryPlan {
    @Override
    public String getPlan() {
      return "";
    }

    @Override
    public QueryCost getCost() {
      // this means that JDBC driver is only selected for tables with just DB storage.
      return new QueryCost(0, 0);
    }
  }

  /**
   * Explain the given query.
   *
   * @param query
   *          The query should be in HiveQL(SQL like)
   * @param conf
   *          The query configuration
   * @return The query plan object;
   * @throws LensException
   *           the lens exception
   */
  @Override
  public DriverQueryPlan explain(String query, Configuration conf) throws LensException {
    checkConfigured();
    conf = RewriteUtil.getFinalQueryConf(this, conf);
    String rewrittenQuery = rewriteQuery(query, conf);
    Configuration explainConf = new Configuration(conf);
    explainConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    String explainQuery = explainConf.get(JDBC_EXPLAIN_KEYWORD_PARAM, DEFAULT_JDBC_EXPLAIN_KEYWORD) + rewrittenQuery;
    LOG.info("Explain Query : " + explainQuery);
    QueryContext explainQueryCtx = new QueryContext(explainQuery, null, explainConf);

    QueryResult result = null;
    try {
      result = executeInternal(explainQueryCtx, explainQuery);
      if (result.error != null) {
        throw new LensException("Query explain failed!", result.error);
      }
    } finally {
      if (result != null) {
        result.close();
      }
    }

    return new JDBCQueryPlan();
  }

  /**
   * Prepare the given query.
   *
   * @param pContext
   *          the context
   * @throws LensException
   *           the lens exception
   */
  @Override
  public void prepare(PreparedQueryContext pContext) throws LensException {
    checkConfigured();
    // Only create a prepared statement and then close it
    String rewrittenQuery = rewriteQuery(pContext.getDriverQuery(), pContext.getConf());
    Connection conn = null;
    PreparedStatement stmt = null;
    try {
      conn = getConnection();
      stmt = conn.prepareStatement(rewrittenQuery);
    } catch (SQLException sql) {
      throw new LensException(sql);
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
    LOG.info("Prepared: " + pContext.getPrepareHandle());
  }

  /**
   * Explain and prepare the given query.
   *
   * @param pContext
   *          the context
   * @return The query plan object;
   * @throws LensException
   *           the lens exception
   */
  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
    checkConfigured();
    String rewritten = rewriteQuery(pContext.getDriverQuery(), conf);
    prepare(pContext);
    return new JDBCQueryPlan();
  }

  /**
   * Close the prepare query specified by the prepared handle, releases all the resources held by the prepared query.
   *
   * @param handle
   *          The query handle
   * @throws LensException
   *           the lens exception
   */
  @Override
  public void closePreparedQuery(QueryPrepareHandle handle) throws LensException {
    checkConfigured();
    // Do nothing
  }

  /**
   * Blocking execute of the query.
   *
   * @param context
   *          the context
   * @return returns the result set
   * @throws LensException
   *           the lens exception
   */
  @Override
  public LensResultSet execute(QueryContext context) throws LensException {
    checkConfigured();
    // Always use the driver rewritten query not user query. Since the
    // conf we are passing here is query context conf, we need to add jdbc xml in resource path
    String rewrittenQuery = rewriteQuery(context.getDriverQuery(), RewriteUtil.getFinalQueryConf(this, conf));
    LOG.info("Execute " + context.getQueryHandle());
    QueryResult result = executeInternal(context, rewrittenQuery);
    return result.getLensResultSet(true);

  }

  /**
   * Internally executing query.
   *
   * @param context
   *          the context
   * @param rewrittenQuery
   *          the rewritten query
   * @return returns the result set
   * @throws LensException
   *           the lens exception
   */

  private QueryResult executeInternal(QueryContext context, String rewrittenQuery) throws LensException {
    JdbcQueryContext queryContext = new JdbcQueryContext(context);
    queryContext.setPrepared(false);
    queryContext.setRewrittenQuery(rewrittenQuery);
    QueryResult result = new QueryCallable(queryContext).call();
    return result;
    // LOG.info("Execute " + context.getQueryHandle());
  }

  /**
   * Asynchronously execute the query.
   *
   * @param context
   *          The query context
   * @throws LensException
   *           the lens exception
   */
  @Override
  public void executeAsync(QueryContext context) throws LensException {
    checkConfigured();
    // Always use the driver rewritten query not user query. Since the
    // conf we are passing here is query context conf, we need to add jdbc xml in resource path
    String rewrittenQuery = rewriteQuery(context.getDriverQuery(), RewriteUtil.getFinalQueryConf(this, conf));
    JdbcQueryContext jdbcCtx = new JdbcQueryContext(context);
    jdbcCtx.setRewrittenQuery(rewrittenQuery);
    try {
      Future<QueryResult> future = asyncQueryPool.submit(new QueryCallable(jdbcCtx));
      jdbcCtx.setResultFuture(future);
    } catch (RejectedExecutionException e) {
      LOG.error("Query execution rejected: " + context.getQueryHandle() + " reason:" + e.getMessage(), e);
      throw new LensException("Query execution rejected: " + context.getQueryHandle() + " reason:" + e.getMessage(), e);
    }
    queryContextMap.put(context.getQueryHandle(), jdbcCtx);
    LOG.info("ExecuteAsync: " + context.getQueryHandle());
  }

  /**
   * Register for query completion notification.
   *
   * @param handle
   *          the handle
   * @param timeoutMillis
   *          the timeout millis
   * @param listener
   *          the listener
   * @throws LensException
   *           the lens exception
   */
  @Override
  public void registerForCompletionNotification(QueryHandle handle, long timeoutMillis, QueryCompletionListener listener)
      throws LensException {
    checkConfigured();
    getQueryContext(handle).setListener(listener);
  }

  /**
   * Get status of the query, specified by the handle.
   *
   * @param context
   *          The query handle
   * @throws LensException
   *           the lens exception
   */
  @Override
  public void updateStatus(QueryContext context) throws LensException {
    checkConfigured();
    JdbcQueryContext ctx = getQueryContext(context.getQueryHandle());
    context.getDriverStatus().setDriverStartTime(ctx.getStartTime());
    if (ctx.getResultFuture().isDone()) {
      // Since future is already done, this call should not block
      context.getDriverStatus().setProgress(1.0);
      context.getDriverStatus().setDriverFinishTime(ctx.getEndTime());
      if (ctx.isCancelled()) {
        context.getDriverStatus().setState(DriverQueryState.CANCELED);
        context.getDriverStatus().setStatusMessage(context.getQueryHandle() + " cancelled");
      } else if (ctx.getQueryResult() != null && ctx.getQueryResult().error != null) {
        context.getDriverStatus().setState(DriverQueryState.FAILED);
        context.getDriverStatus().setStatusMessage(ctx.getQueryResult().error.getMessage());
      } else {
        context.getDriverStatus().setState(DriverQueryState.SUCCESSFUL);
        context.getDriverStatus().setStatusMessage(context.getQueryHandle() + " successful");
        context.getDriverStatus().setResultSetAvailable(true);
      }
    } else {
      context.getDriverStatus().setProgress(0.0);
      context.getDriverStatus().setState(DriverQueryState.RUNNING);
      context.getDriverStatus().setStatusMessage(context.getQueryHandle() + " is running");
    }
  }

  /**
   * Fetch the results of the query, specified by the handle.
   *
   * @param context
   *          the context
   * @return returns the {@link LensResultSet}.
   * @throws LensException
   *           the lens exception
   */
  @Override
  public LensResultSet fetchResultSet(QueryContext context) throws LensException {
    checkConfigured();
    JdbcQueryContext ctx = getQueryContext(context.getQueryHandle());
    if (ctx.isCancelled()) {
      throw new LensException("Result set not available for cancelled query " + context.getQueryHandle());
    }

    Future<QueryResult> future = ctx.getResultFuture();
    QueryHandle queryHandle = context.getQueryHandle();

    try {
      return future.get().getLensResultSet(true);
    } catch (InterruptedException e) {
      throw new LensException("Interrupted while getting resultset for query " + queryHandle.getHandleId(), e);
    } catch (ExecutionException e) {
      throw new LensException("Error while executing query " + queryHandle.getHandleId() + " in background", e);
    } catch (CancellationException e) {
      throw new LensException("Query was already cancelled " + queryHandle.getHandleId(), e);
    }
  }

  /**
   * Close the resultset for the query.
   *
   * @param handle
   *          The query handle
   * @throws LensException
   *           the lens exception
   */
  @Override
  public void closeResultSet(QueryHandle handle) throws LensException {
    checkConfigured();
    getQueryContext(handle).closeResult();
  }

  /**
   * Cancel the execution of the query, specified by the handle.
   *
   * @param handle
   *          The query handle.
   * @return true if cancel was successful, false otherwise
   * @throws LensException
   *           the lens exception
   */
  @Override
  public boolean cancelQuery(QueryHandle handle) throws LensException {
    checkConfigured();
    JdbcQueryContext context = getQueryContext(handle);
    boolean cancelResult = context.getResultFuture().cancel(true);
    if (cancelResult) {
      context.setCancelled(true);
      // this is required because future.cancel does not guarantee
      // that finally block is always called.
      if (context.getEndTime() == 0) {
        context.setEndTime(System.currentTimeMillis());
      }
      context.closeResult();
      LOG.info("Cancelled query: " + handle);
    }
    return cancelResult;
  }

  /**
   * Close the query specified by the handle, releases all the resources held by the query.
   *
   * @param handle
   *          The query handle
   * @throws LensException
   *           the lens exception
   */
  @Override
  public void closeQuery(QueryHandle handle) throws LensException {
    checkConfigured();
    try {
      JdbcQueryContext ctx = getQueryContext(handle);
      ctx.getResultFuture().cancel(true);
      ctx.closeResult();
    } finally {
      queryContextMap.remove(handle);
    }
    LOG.info("Closed query " + handle.getHandleId());
  }

  /**
   * Close the driver, releasing all resouces used up by the driver.
   *
   * @throws LensException
   *           the lens exception
   */
  @Override
  public void close() throws LensException {
    checkConfigured();
    try {
      for (QueryHandle query : new ArrayList<QueryHandle>(queryContextMap.keySet())) {
        try {
          closeQuery(query);
        } catch (LensException e) {
          LOG.warn("Error closing query : " + query.getHandleId(), e);
        }
      }
    } finally {
      queryContextMap.clear();
    }
  }

  /**
   * Add a listener for driver events.
   *
   * @param driverEventListener
   *          the driver event listener
   */
  @Override
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
   */
  @Override
  public void readExternal(ObjectInput arg0) throws IOException, ClassNotFoundException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
   */
  @Override
  public void writeExternal(ObjectOutput arg0) throws IOException {
    // TODO Auto-generated method stub

  }
}
