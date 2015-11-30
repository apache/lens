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

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;

import static org.apache.lens.driver.jdbc.JDBCDriverConfConstants.*;
import static org.apache.lens.driver.jdbc.JDBCDriverConfConstants.ConnectionPoolProperties.*;
import static org.apache.lens.server.api.util.LensUtil.getImplementations;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.metrics.MethodMetricsContext;
import org.apache.lens.server.api.metrics.MethodMetricsFactory;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.WaitingQueriesSelectionPolicy;
import org.apache.lens.server.api.query.constraint.MaxConcurrentDriverQueriesConstraintFactory;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.query.rewrite.QueryRewriter;
import org.apache.lens.server.model.LogSegregationContext;
import org.apache.lens.server.model.MappedDiagnosticLogSegregationContext;

import org.apache.commons.lang3.StringUtils;

 import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import com.google.common.collect.ImmutableSet;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * This driver is responsible for running queries against databases which can be queried using the JDBC API.
 */
@Slf4j
public class JDBCDriver extends AbstractLensDriver {

  /** The Constant THID. */
  public static final AtomicInteger THID = new AtomicInteger();

  /** The connection provider. */
  private ConnectionProvider connectionProvider;

  /** The configured. */
  boolean configured = false;

  /** The async query pool. */
  private ExecutorService asyncQueryPool;

  /** The query context map. */
  private ConcurrentHashMap<QueryHandle, JdbcQueryContext> queryContextMap;

  /** The conf. */
  private Configuration conf;

  /** Configuration for estimate connection pool */
  private Configuration estimateConf;
  /** Estimate connection provider */
  private ConnectionProvider estimateConnectionProvider;

  private LogSegregationContext logSegregationContext;
  private DriverQueryHook queryHook;

  @Getter
  private ImmutableSet<QueryLaunchingConstraint> queryConstraints;
  private ImmutableSet<WaitingQueriesSelectionPolicy> selectionPolicies;

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

    private final LogSegregationContext logSegregationContext;

    /**
     * Instantiates a new jdbc query context.
     *
     * @param context the context
     */
    public JdbcQueryContext(QueryContext context, @NonNull final LogSegregationContext logSegregationContext) {
      this.logSegregationContext = logSegregationContext;
      this.lensContext = context;
    }

    /**
     * Notify error.
     *
     * @param th the th
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

    public String getQueryHandleString() {
      return this.lensContext.getQueryHandleString();
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
            log.error("Error closing SQL statement", e);
          }
        }
      } finally {
        if (conn != null) {
          try {
            conn.close();
          } catch (SQLException e) {
            log.error("Error closing SQL Connection", e);
          }
        }
      }
      isClosed = true;
    }

    /**
     * Gets the lens result set.
     *
     * @param closeAfterFetch the close after fetch
     * @return the lens result set
     * @throws LensException the lens exception
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
    private final LogSegregationContext logSegregationContext;

    /**
     * Instantiates a new query callable.
     *
     * @param queryContext the query context
     */
    public QueryCallable(JdbcQueryContext queryContext, @NonNull LogSegregationContext logSegregationContext) {
      this.queryContext = queryContext;
      this.logSegregationContext = logSegregationContext;
      queryContext.setStartTime(System.currentTimeMillis());
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public QueryResult call() {

      logSegregationContext.setLogSegragationAndQueryId(this.queryContext.getQueryHandleString());

      Statement stmt;
      Connection conn = null;
      QueryResult result = new QueryResult();
      try {
        queryContext.setQueryResult(result);

        try {
          conn = getConnection();
          result.conn = conn;
        } catch (LensException e) {
          log.error("Error obtaining connection: ", e);
          result.error = e;
        }

        if (conn != null) {
          try {
            stmt = createStatement(conn);
            result.stmt = stmt;
            Boolean isResultAvailable = stmt.execute(queryContext.getRewrittenQuery());
            if (isResultAvailable) {
              result.resultSet = stmt.getResultSet();
            }
            queryContext.notifyComplete();
          } catch (SQLException sqlEx) {
            if (queryContext.isClosed()) {
              log.info("Ignored exception on already closed query : {} - {}",
                queryContext.getLensContext().getQueryHandle(), sqlEx.getMessage());
            } else {
              log.error("Error executing SQL query: {} reason: {}", queryContext.getLensContext().getQueryHandle(),
                sqlEx.getMessage(), sqlEx);
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
     * Create statement used to issue the query
     *
     * @param conn pre created SQL Connection object
     * @return statement
     * @throws SQLException the SQL exception
     */
    public Statement createStatement(Connection conn) throws SQLException {
      Statement stmt;

      boolean enabledRowRetrieval = queryContext.getLensContext().getSelectedDriverConf().getBoolean(
        JDBCDriverConfConstants.JDBC_ENABLE_RESULTSET_STREAMING_RETRIEVAL,
        JDBCDriverConfConstants.DEFAULT_JDBC_ENABLE_RESULTSET_STREAMING_RETRIEVAL);

      if (enabledRowRetrieval) {
        log.info("JDBC streaming retrieval is enabled for {}", queryContext.getLensContext().getQueryHandle());
        if (queryContext.isPrepared()) {
          stmt = conn.prepareStatement(queryContext.getRewrittenQuery(),
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        } else {
          stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        }
        stmt.setFetchSize(Integer.MIN_VALUE);
      } else {
        stmt = queryContext.isPrepared() ? conn.prepareStatement(queryContext.getRewrittenQuery())
          : conn.createStatement();

        // Get default fetch size from conf if not overridden in query conf
        int fetchSize = queryContext.getLensContext().getSelectedDriverConf().getInt(
          JDBCDriverConfConstants.JDBC_FETCH_SIZE, JDBCDriverConfConstants.DEFAULT_JDBC_FETCH_SIZE);
        stmt.setFetchSize(fetchSize);
      }

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
     * @see org.apache.lens.server.api.query.QueryRewriter#rewrite
     * (java.lang.String, org.apache.hadoop.conf.Configuration)
     */
    @Override
    public String rewrite(String query, Configuration queryConf, HiveConf metastoreConf) throws LensException {
      return query;
    }

    @Override
    public void init(Configuration rewriteConf) {
    }
  }

  /**
   * Get driver configuration
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#configure(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void configure(Configuration conf, String driverType, String driverName) throws LensException {
    super.configure(conf, driverType, driverName);
    this.conf = new Configuration(conf);
    this.conf.addResource("jdbcdriver-default.xml");
    this.conf.addResource(getDriverResourcePath("jdbcdriver-site.xml"));
    init(conf);
    try {
      queryHook = this.conf.getClass(
        JDBC_QUERY_HOOK_CLASS, NoOpDriverQueryHook.class, DriverQueryHook.class
      ).newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new LensException("Can't instantiate driver query hook for hivedriver with given class", e);
    }
    configured = true;
    log.info("JDBC Driver {} configured", getFullyQualifiedName());
  }

  /**
   * Inits the.
   *
   * @param conf the conf
   * @throws LensException the lens exception
   */
  protected void init(Configuration conf) throws LensException {

    final int maxPoolSize = parseInt(this.conf.get(JDBC_POOL_MAX_SIZE.getConfigKey()));
    final int maxConcurrentQueries
      = parseInt(this.conf.get(MaxConcurrentDriverQueriesConstraintFactory.MAX_CONCURRENT_QUERIES_KEY));
    checkState(maxPoolSize == maxConcurrentQueries, "maxPoolSize:" + maxPoolSize + " maxConcurrentQueries:"
      + maxConcurrentQueries);

    queryContextMap = new ConcurrentHashMap<>();
    asyncQueryPool = Executors.newCachedThreadPool(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread th = new Thread(runnable);
        th.setName("lens-driver-jdbc-" + THID.incrementAndGet());
        return th;
      }
    });

    Class<? extends ConnectionProvider> cpClass = conf.getClass(JDBC_CONNECTION_PROVIDER,
      DataSourceConnectionProvider.class, ConnectionProvider.class);
    try {
      connectionProvider = cpClass.newInstance();
      estimateConnectionProvider = cpClass.newInstance();
    } catch (Exception e) {
      log.error("Error initializing connection provider: ", e);
      throw new LensException(e);
    }
    this.logSegregationContext = new MappedDiagnosticLogSegregationContext();
    this.queryConstraints = getImplementations(QUERY_LAUNCHING_CONSTRAINT_FACTORIES_KEY, this.conf);
    this.selectionPolicies = getImplementations(WAITING_QUERIES_SELECTION_POLICY_FACTORIES_KEY, this.conf);
  }

  /**
   * Check configured.
   *
   * @throws IllegalStateException the illegal state exception
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
   * @return the query rewriter
   * @throws LensException the lens exception
   */
  protected QueryRewriter getQueryRewriter() throws LensException {
    QueryRewriter rewriter;
    Class<? extends QueryRewriter> queryRewriterClass = conf.getClass(JDBC_QUERY_REWRITER_CLASS,
      DummyQueryRewriter.class, QueryRewriter.class);
    try {
      rewriter = queryRewriterClass.newInstance();
      log.info("{} Initialized :{}", getFullyQualifiedName(), queryRewriterClass);
    } catch (Exception e) {
      log.error("{} Unable to create rewriter object", getFullyQualifiedName(), e);
      throw new LensException(e);
    }
    rewriter.init(conf);
    return rewriter;
  }

  /**
   * Gets the query context.
   *
   * @param handle the handle
   * @return the query context
   * @throws LensException the lens exception
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
   * @return the string
   * @throws LensException the lens exception
   */
  protected String rewriteQuery(AbstractQueryContext ctx) throws LensException {
    if (ctx.getFinalDriverQuery(this) != null) {
      return ctx.getFinalDriverQuery(this);
    }
    String query = ctx.getDriverQuery(this);
    Configuration driverQueryConf = ctx.getDriverConf(this);
    MethodMetricsContext checkForAllowedQuery = MethodMetricsFactory.createMethodGauge(driverQueryConf, true,
      CHECK_ALLOWED_QUERY);
    // check if it is select query

    ASTNode ast = HQLParser.parseHQL(query, ctx.getHiveConf());
    if (ast.getToken().getType() != HiveParser.TOK_QUERY) {
      throw new LensException("Not allowed statement:" + query);
    } else {
      // check for insert clause
      ASTNode dest = HQLParser.findNodeByPath(ast, HiveParser.TOK_INSERT);
      if (dest != null
        && ((ASTNode) (dest.getChild(0).getChild(0).getChild(0))).getToken().getType() != HiveParser.TOK_TMP_FILE) {
        throw new LensException("Not allowed statement:" + query);
      }
    }
    checkForAllowedQuery.markSuccess();

    QueryRewriter rewriter = getQueryRewriter();
    String rewrittenQuery = rewriter.rewrite(query, driverQueryConf, ctx.getHiveConf());
    ctx.setFinalDriverQuery(this, rewrittenQuery);
    return rewrittenQuery;
  }

  static final QueryCost JDBC_DRIVER_COST = new FactPartitionBasedQueryCost(0);

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
      return JDBC_DRIVER_COST;
    }
  }

  private static final String VALIDATE_GAUGE = "validate-thru-prepare";
  private static final String COLUMNAR_SQL_REWRITE_GAUGE = "columnar-sql-rewrite";
  private static final String JDBC_PREPARE_GAUGE = "jdbc-prepare-statement";
  private static final String CHECK_ALLOWED_QUERY = "jdbc-check-allowed-query";

  @Override
  public QueryCost estimate(AbstractQueryContext qctx) throws LensException {
    MethodMetricsContext validateGauge = MethodMetricsFactory.createMethodGauge(qctx.getDriverConf(this), true,
      VALIDATE_GAUGE);
    validate(qctx);
    validateGauge.markSuccess();
    return JDBC_DRIVER_COST;
  }

  /**
   * Explain the given query.
   *
   * @param explainCtx The explain context
   * @return The query plan object;
   * @throws LensException the lens exception
   */
  @Override
  public DriverQueryPlan explain(AbstractQueryContext explainCtx) throws LensException {
    if (explainCtx.getDriverQuery(this) == null) {
      throw new NullPointerException("Null driver query for " + explainCtx.getUserQuery());
    }
    if (explainCtx.getDriverContext().getDriverQueryPlan(this) != null) {
      // explain called again and again
      return explainCtx.getDriverContext().getDriverQueryPlan(this);
    }
    checkConfigured();
    String explainQuery;
    String rewrittenQuery = rewriteQuery(explainCtx);
    Configuration explainConf = explainCtx.getDriverConf(this);
    String explainKeyword = explainConf.get(JDBC_EXPLAIN_KEYWORD_PARAM,
      DEFAULT_JDBC_EXPLAIN_KEYWORD);
    boolean explainBeforeSelect = explainConf.getBoolean(JDBC_EXPLAIN_KEYWORD_BEFORE_SELECT,
      DEFAULT_JDBC_EXPLAIN_KEYWORD_BEFORE_SELECT);

    if (explainBeforeSelect) {
      explainQuery = explainKeyword + " " + rewrittenQuery;
    } else {
      explainQuery = rewrittenQuery.replaceAll("select ", "select "
        + explainKeyword + " ");
    }
    log.info("{} Explain Query : {}", getFullyQualifiedName(), explainQuery);
    QueryContext explainQueryCtx = QueryContext.createContextWithSingleDriver(explainQuery, null,
      new LensConf(), explainConf, this, explainCtx.getLensSessionIdentifier(), false);
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
    JDBCQueryPlan jqp = new JDBCQueryPlan();
    explainCtx.getDriverContext().setDriverQueryPlan(this, jqp);
    return jqp;
  }

  /**
   * Validate query using prepare
   *
   * @param pContext
   * @throws LensException
   */
  public void validate(AbstractQueryContext pContext) throws LensException {
    if (pContext.getDriverQuery(this) == null) {
      throw new NullPointerException("Null driver query for " + pContext.getUserQuery());
    }
    boolean validateThroughPrepare = pContext.getDriverConf(this).getBoolean(JDBC_VALIDATE_THROUGH_PREPARE,
      DEFAULT_JDBC_VALIDATE_THROUGH_PREPARE);
    if (validateThroughPrepare) {
      PreparedStatement stmt = null;
      // Estimate queries need to get connection from estimate pool to make sure
      // we are not blocked by data queries.
      stmt = prepareInternal(pContext, true, true, "validate-");
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          throw new LensException();
        }
      }
    }
  }

  // Get key used for estimate key config
  protected String getEstimateKey(String jdbcKey) {
    return JDBC_DRIVER_PFX + "estimate." + jdbcKey.substring(JDBC_DRIVER_PFX.length());
  }

  // If any 'key' in 'keys' is set in conf, return its value.
  private static String getKeyOrFallBack(Configuration conf, String... keys) {
    for (String key : keys) {
      String val = conf.get(key);
      if (StringUtils.isNotBlank(val)) {
        return val;
      }
    }
    return null;
  }

  // Get connection config used by estimate pool.
  protected final Configuration getEstimateConnectionConf() {
    if (estimateConf == null) {
      Configuration tmpConf = new Configuration(conf);
      // Override JDBC settings in estimate conf, if set by user explicitly. Otherwise fall back to default JDBC pool
      // config
      for (String key : asList(JDBC_CONNECTION_PROPERTIES, JDBC_DB_URI, JDBC_DRIVER_CLASS, JDBC_USER, JDBC_PASSWORD,
        JDBC_POOL_MAX_SIZE.getConfigKey(), JDBC_POOL_IDLE_TIME.getConfigKey(),
        JDBC_MAX_IDLE_TIME_EXCESS_CONNECTIONS.getConfigKey(),
        JDBC_MAX_STATEMENTS_PER_CONNECTION.getConfigKey(), JDBC_GET_CONNECTION_TIMEOUT.getConfigKey())) {
        String val = getKeyOrFallBack(tmpConf, getEstimateKey(key), key);
        if (val != null) {
          tmpConf.set(key, val);
        }
      }
      /* We need to set password as empty string if it is not provided. Setting null on conf is not allowed */
      if (tmpConf.get(JDBC_PASSWORD) == null) {
        tmpConf.set(JDBC_PASSWORD, "");
      }
      estimateConf = tmpConf;
    }
    return estimateConf;
  }

  protected final Connection getEstimateConnection() throws SQLException {
    return estimateConnectionProvider.getConnection(getEstimateConnectionConf());
  }

  // For tests
  protected final ConnectionProvider getEstimateConnectionProvider() {
    return estimateConnectionProvider;
  }

  // For tests
  protected final ConnectionProvider getConnectionProvider() {
    return connectionProvider;
  }

  private final Map<QueryPrepareHandle, PreparedStatement> preparedQueries = new HashMap<>();

  /**
   * Internally prepare the query
   *
   * @param pContext
   * @return
   * @throws LensException
   */
  private PreparedStatement prepareInternal(AbstractQueryContext pContext) throws LensException {
    if (pContext.getDriverQuery(this) == null) {
      throw new NullPointerException("Null driver query for " + pContext.getUserQuery());
    }
    checkConfigured();
    return prepareInternal(pContext, false, false, "prepare-");
  }


  /**
   * Prepare statment on the database server
   * @param pContext query context
   * @param calledForEstimate set this to true if this call will use the estimate connection pool
   * @param checkConfigured set this to true if this call needs to check whether JDBC driver is configured
   * @param metricCallStack stack for metrics API
   * @return prepared statement
   * @throws LensException
   */
  private PreparedStatement prepareInternal(AbstractQueryContext pContext,
    boolean calledForEstimate,
    boolean checkConfigured,
    String metricCallStack) throws LensException {
    // Caller might have already verified configured status and driver query, so we don't have
    // to do this check twice. Caller must set checkConfigured to false in that case.
    if (checkConfigured) {
      if (pContext.getDriverQuery(this) == null) {
        throw new NullPointerException("Null driver query for " + pContext.getUserQuery());
      }
      checkConfigured();
    }

    // Only create a prepared statement and then close it
    MethodMetricsContext sqlRewriteGauge = MethodMetricsFactory.createMethodGauge(pContext.getDriverConf(this), true,
      metricCallStack + COLUMNAR_SQL_REWRITE_GAUGE);
    String rewrittenQuery = rewriteQuery(pContext);
    sqlRewriteGauge.markSuccess();
    MethodMetricsContext jdbcPrepareGauge = MethodMetricsFactory.createMethodGauge(pContext.getDriverConf(this), true,
      metricCallStack + JDBC_PREPARE_GAUGE);

    PreparedStatement stmt = null;
    Connection conn = null;
    try {
      conn = calledForEstimate ? getEstimateConnection() : getConnection();
      stmt = conn.prepareStatement(rewrittenQuery);
      if (stmt.getWarnings() != null) {
        throw new LensException(stmt.getWarnings());
      }
    } catch (SQLException sql) {
      throw new LensException(sql);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          log.error("Error closing connection: {}", rewrittenQuery, e);
        }
      }
      jdbcPrepareGauge.markSuccess();
    }
    log.info("Prepared: {}", rewrittenQuery);
    return stmt;
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
    if (preparedQueries.containsKey(pContext.getPrepareHandle())) {
      // already prepared
      return;
    }
    PreparedStatement stmt = prepareInternal(pContext);
    if (stmt != null) {
      preparedQueries.put(pContext.getPrepareHandle(), stmt);
    }
  }

  /**
   * Explain and prepare the given query.
   *
   * @param pContext the context
   * @return The query plan object;
   * @throws LensException the lens exception
   */
  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
    checkConfigured();
    prepare(pContext);
    return new JDBCQueryPlan();
  }

  /**
   * Close the prepare query specified by the prepared handle, releases all the resources held by the prepared query.
   *
   * @param handle The query handle
   * @throws LensException the lens exception
   */
  @Override
  public void closePreparedQuery(QueryPrepareHandle handle) throws LensException {
    checkConfigured();
    try {
      if (preparedQueries.get(handle) != null) {
        preparedQueries.get(handle).close();
      }
    } catch (SQLException e) {
      throw new LensException(e);
    }
  }

  /**
   * Blocking execute of the query.
   *
   * @param context the context
   * @return returns the result set
   * @throws LensException the lens exception
   */
  @Override
  public LensResultSet execute(QueryContext context) throws LensException {
    checkConfigured();

    String rewrittenQuery = rewriteQuery(context);
    log.info("{} Execute {}", getFullyQualifiedName(), context.getQueryHandle());
    QueryResult result = executeInternal(context, rewrittenQuery);
    return result.getLensResultSet(true);

  }

  /**
   * Internally executing query.
   *
   * @param context        the context
   * @param rewrittenQuery the rewritten query
   * @return returns the result set
   * @throws LensException the lens exception
   */

  private QueryResult executeInternal(QueryContext context, String rewrittenQuery) throws LensException {
    JdbcQueryContext queryContext = new JdbcQueryContext(context, logSegregationContext);
    queryContext.setPrepared(false);
    queryContext.setRewrittenQuery(rewrittenQuery);
    return new QueryCallable(queryContext, logSegregationContext).call();
    // LOG.info("Execute " + context.getQueryHandle());
  }

  /**
   * Asynchronously execute the query.
   *
   * @param context The query context
   * @throws LensException the lens exception
   */
  @Override
  public void executeAsync(QueryContext context) throws LensException {
    checkConfigured();
    // Always use the driver rewritten query not user query. Since the
    // conf we are passing here is query context conf, we need to add jdbc xml in resource path
    String rewrittenQuery = rewriteQuery(context);
    JdbcQueryContext jdbcCtx = new JdbcQueryContext(context, logSegregationContext);
    jdbcCtx.setRewrittenQuery(rewrittenQuery);
    queryHook.preLaunch(context);
    try {
      Future<QueryResult> future = asyncQueryPool.submit(new QueryCallable(jdbcCtx, logSegregationContext));
      jdbcCtx.setResultFuture(future);
    } catch (RejectedExecutionException e) {
      log.error("Query execution rejected: {} reason:{}", context.getQueryHandle(), e.getMessage(), e);
      throw new LensException("Query execution rejected: " + context.getQueryHandle() + " reason:" + e.getMessage(), e);
    }
    queryContextMap.put(context.getQueryHandle(), jdbcCtx);
    log.info("{} ExecuteAsync: {}", getFullyQualifiedName(), context.getQueryHandle());
  }

  /**
   * Register for query completion notification.
   *
   * @param handle        the handle
   * @param timeoutMillis the timeout millis
   * @param listener      the listener
   * @throws LensException the lens exception
   */
  @Override
  public void registerForCompletionNotification(QueryHandle handle, long timeoutMillis,
    QueryCompletionListener listener) throws LensException {
    checkConfigured();
    getQueryContext(handle).setListener(listener);
  }

  /**
   * Get status of the query, specified by the handle.
   *
   * @param context The query handle
   * @throws LensException the lens exception
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
        context.getDriverStatus().setStatusMessage("Query execution failed!");
        context.getDriverStatus().setErrorMessage(ctx.getQueryResult().error.getMessage());
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
   * @param context the context
   * @return returns the {@link LensResultSet}.
   * @throws LensException the lens exception
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
   * @param handle The query handle
   * @throws LensException the lens exception
   */
  @Override
  public void closeResultSet(QueryHandle handle) throws LensException {
    checkConfigured();
    getQueryContext(handle).closeResult();
  }

  /**
   * Cancel the execution of the query, specified by the handle.
   *
   * @param handle The query handle.
   * @return true if cancel was successful, false otherwise
   * @throws LensException the lens exception
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
      log.info("{} Cancelled query : {}", getFullyQualifiedName(), handle);
    }
    return cancelResult;
  }

  /**
   * Close the query specified by the handle, releases all the resources held by the query.
   *
   * @param handle The query handle
   * @throws LensException the lens exception
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
    log.info("{} Closed query {}", getFullyQualifiedName(), handle.getHandleId());
  }

  /**
   * Close the driver, releasing all resouces used up by the driver.
   *
   * @throws LensException the lens exception
   */
  @Override
  public void close() throws LensException {
    checkConfigured();
    try {
      for (QueryHandle query : new ArrayList<QueryHandle>(queryContextMap.keySet())) {
        try {
          closeQuery(query);
        } catch (LensException e) {
          log.warn("{} Error closing query : {}", getFullyQualifiedName(), query.getHandleId(), e);
        }
      }
      for (QueryPrepareHandle query : preparedQueries.keySet()) {
        try {
          try {
            preparedQueries.get(query).close();
          } catch (SQLException e) {
            throw new LensException();
          }
        } catch (LensException e) {
          log.warn("{} Error closing prapared query : {}", getFullyQualifiedName(), query, e);
        }
      }
    } finally {
      queryContextMap.clear();
    }
  }

  /**
   * Add a listener for driver events.
   *
   * @param driverEventListener the driver event listener
   */
  @Override
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {

  }

  @Override
  public ImmutableSet<WaitingQueriesSelectionPolicy> getWaitingQuerySelectionPolicies() {
    return this.selectionPolicies;
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
