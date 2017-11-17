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
import org.apache.lens.cube.query.cost.StaticCostCalculator;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.error.LensDriverErrorCode;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.metrics.MethodMetricsContext;
import org.apache.lens.server.api.metrics.MethodMetricsFactory;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.constraint.MaxConcurrentDriverQueriesConstraintFactory;
import org.apache.lens.server.api.query.cost.*;
import org.apache.lens.server.api.query.rewrite.QueryRewriter;
import org.apache.lens.server.api.util.LensUtil;
import org.apache.lens.server.model.LogSegregationContext;
import org.apache.lens.server.model.MappedDiagnosticLogSegregationContext;

import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import lombok.*;
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
  @Getter
  private ConcurrentHashMap<QueryHandle, JdbcQueryContext> queryContextMap;

  /** Configuration for estimate connection pool */
  private Configuration estimateConf;
  /** Estimate connection provider */
  private ConnectionProvider estimateConnectionProvider;

  private LogSegregationContext logSegregationContext;

  private boolean isStatementCancelSupported;

  QueryCostCalculator queryCostCalculator;

  /**
   * Data related to a query submitted to JDBCDriver.
   */
  @Data
  protected class JdbcQueryContext {

    /** The lens context. */
    private final QueryContext lensContext;

    /** The result future. */
    private Future<QueryResult> resultFuture;

    /** The rewritten query. */
    private String rewrittenQuery;

    /** The is prepared. */
    private boolean isPrepared;

    /** The is closed. */
    private boolean isClosed;

    /** The query result. */
    private QueryResult queryResult;

    /**
     * Close result.
     */
    public void closeResult() {
      if (queryResult != null) {
        queryResult.close();
      }
      isClosed = true;
    }

    public boolean cancel() {
      boolean ret;
      log.debug("Canceling resultFuture object");
      ret = resultFuture.cancel(true);
      log.debug("Done resultFuture cancel!");
      // queryResult object would be null if query is not yet launched - since we did future.cancel, no other cancel is
      // required.
      if (queryResult != null && queryResult.stmt != null && isStatementCancelSupported) {
        log.debug("Cancelling query through statement cancel");
        try {
          queryResult.stmt.cancel();
          log.debug("Done statement cancel!");
          ret = true;
        } catch (SQLFeatureNotSupportedException se) {
          log.warn("Statement cancel not supported", se);
        } catch(SQLException e) {
          log.warn("Statement cancel failed", e);
          ret = false;
        }
      }
      return ret;
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
    private InMemoryResultSet lensResultSet;

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
      queryContext.getLensContext().setDriverStatus(DriverQueryState.INITIALIZED);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public QueryResult call() {
      logSegregationContext.setLogSegragationAndQueryId(this.queryContext.getQueryHandleString());
      queryContext.getLensContext().setDriverStatus(DriverQueryState.RUNNING);
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
            if (queryContext.getLensContext().getDriverStatus().isCanceled()) {
              return result;
            }
            if (isResultAvailable) {
              result.resultSet = stmt.getResultSet();
            }
            queryContext.getLensContext().getDriverStatus().setResultSetAvailable(isResultAvailable);
            queryContext.getLensContext().setDriverStatus(DriverQueryState.SUCCESSFUL);
          } catch (Exception e) {
            if (queryContext.getLensContext().getDriverStatus().isCanceled()) {
              return result;
            }
            if (queryContext.isClosed()) {
              log.info("Ignored exception on already closed query : {} - {}",
                queryContext.getLensContext().getQueryHandle(), e.getMessage(), e);
            } else {
              log.error("Error executing SQL query: {} reason: {}", queryContext.getLensContext().getQueryHandle(),
                e.getMessage(), e);
              result.error = e;
              queryContext.getLensContext().setDriverStatus(DriverQueryState.FAILED, e.getMessage());
              // Close connection in case of failed queries. For successful queries, connection is closed
              // When result set is closed or driver.closeQuery is called
              result.close();
            }
          }
        }
      } finally {
        Long endTime = queryContext.getLensContext().getDriverStatus().getDriverFinishTime();
        if (endTime == null || endTime <= 0) {
          queryContext.getLensContext().getDriverStatus().setDriverFinishTime(System.currentTimeMillis());
        }
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

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#configure(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void configure(Configuration conf, String driverType, String driverName) throws LensException {
    super.configure(conf, driverType, driverName);
    init();
    configured = true;
    Class<? extends QueryCostCalculator> queryCostCalculatorClass = getConf().getClass(JDBC_COST_CALCULATOR,
      StaticCostCalculator.class, QueryCostCalculator.class);
    try {
      queryCostCalculator = queryCostCalculatorClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new LensException("Can't instantiate query cost calculator of class: " + queryCostCalculatorClass, e);
    }

    //For initializing the decider class instance
    queryCostCalculator.init(this);

    log.info("JDBC Driver {} configured", getFullyQualifiedName());
  }

  /**
   * Inits the.
   *
   * @throws LensException the lens exception
   */
  public void init() throws LensException {
    final int maxPoolSize = parseInt(getConf().get(JDBC_POOL_MAX_SIZE.getConfigKey()));
    final int maxConcurrentQueries
      = parseInt(getConf().get(MaxConcurrentDriverQueriesConstraintFactory.MAX_CONCURRENT_QUERIES_KEY));
    checkState(maxPoolSize >= maxConcurrentQueries, "maxPoolSize:" + maxPoolSize + " maxConcurrentQueries:"
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

    Class<? extends ConnectionProvider> cpClass = getConf().getClass(JDBC_CONNECTION_PROVIDER,
      DataSourceConnectionProvider.class, ConnectionProvider.class);
    try {
      connectionProvider = cpClass.newInstance();
      estimateConnectionProvider = cpClass.newInstance();
    } catch (Exception e) {
      log.error("Error initializing connection provider: ", e);
      throw new LensException(e);
    }
    this.logSegregationContext = new MappedDiagnosticLogSegregationContext();
    this.isStatementCancelSupported = getConf().getBoolean(STATEMENT_CANCEL_SUPPORTED,
      DEFAULT_STATEMENT_CANCEL_SUPPORTED);
  }

  public QueryCost calculateQueryCost(AbstractQueryContext qctx) throws LensException {
    return queryCostCalculator.calculateCost(qctx, this);
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
      return connectionProvider.getConnection(getConf());
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
    Class<? extends QueryRewriter> queryRewriterClass = getConf().getClass(JDBC_QUERY_REWRITER_CLASS,
      DummyQueryRewriter.class, QueryRewriter.class);
    try {
      rewriter = queryRewriterClass.newInstance();
      log.info("{} Initialized :{}", getFullyQualifiedName(), queryRewriterClass);
    } catch (Exception e) {
      log.error("{} Unable to create rewriter object", getFullyQualifiedName(), e);
      throw new LensException(e);
    }
    rewriter.init(getConf());
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

  /**
   * Dummy JDBC query Plan class to get min cost selector working.
   */
  private static class JDBCQueryPlan extends DriverQueryPlan {

    @Getter
    private final QueryCost cost;

    JDBCQueryPlan(QueryCost cost){
      this.cost = cost;
    }
    @Override
    public String getPlan() {
      return "";
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
    return calculateQueryCost(qctx);
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
    JDBCQueryPlan jqp = new JDBCQueryPlan(calculateQueryCost(explainCtx));
    explainCtx.getDriverContext().setDriverQueryPlan(this, jqp);
    return jqp;
  }

  /**
   * Validate query using prepare
   *
   * @param pContext context to validate
   * @throws LensException
   */
  public void validate(AbstractQueryContext pContext) throws LensException {
    if (pContext.getDriverQuery(this) == null) {
      throw new NullPointerException("Null driver query for " + pContext.getUserQuery());
    }
    boolean validateThroughPrepare = pContext.getDriverConf(this).getBoolean(JDBC_VALIDATE_THROUGH_PREPARE,
      DEFAULT_JDBC_VALIDATE_THROUGH_PREPARE);
    if (validateThroughPrepare) {
      PreparedStatement stmt;
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
      Configuration tmpConf = new Configuration(getConf());
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
   * @param pContext prepare context
   * @return prepared statement of the query
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
      if (!pContext.getDriverConf(this).getBoolean(JDBC_VALIDATE_SKIP_WARNINGS,
        DEFAULT_JDBC_VALIDATE_SKIP_WARNINGS) && stmt.getWarnings() != null) {
        throw new LensException(stmt.getWarnings());
      }
    } catch (SQLException sql) {
      handleJDBCSQLException(sql);
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
   * Handle sql exception
   *
   * @param sqlex SQLException
   * @throws LensException
   */
  private LensException handleJDBCSQLException(SQLException sqlex) throws LensException {
    String cause = LensUtil.getCauseMessage(sqlex);
    if (getSqlSynataxExceptions(sqlex).contains("SyntaxError")) {
      throw new LensException(LensDriverErrorCode.SEMANTIC_ERROR.getLensErrorInfo(), sqlex, cause);
    }
    throw new LensException(LensDriverErrorCode.DRIVER_ERROR.getLensErrorInfo(), sqlex, cause);
  }

  private String getSqlSynataxExceptions(Throwable e) {
    String exp = null;
    if (e.getCause() != null) {
      exp = e.getClass() + getSqlSynataxExceptions(e.getCause());
    }
    return exp;
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
    return new JDBCQueryPlan(calculateQueryCost(pContext));
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
    JdbcQueryContext queryContext = new JdbcQueryContext(context);
    queryContext.setPrepared(false);
    queryContext.setRewrittenQuery(rewrittenQuery);
    return new QueryCallable(queryContext, logSegregationContext).call();
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
    JdbcQueryContext jdbcCtx = new JdbcQueryContext(context);
    jdbcCtx.setRewrittenQuery(rewrittenQuery);
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
   * Get status of the query, specified by the handle.
   *
   * @param context The query handle
   * @throws LensException the lens exception
   */
  @Override
  public void updateStatus(QueryContext context) throws LensException {
    checkConfigured();
    JdbcQueryContext ctx = getQueryContext(context.getQueryHandle());
    if (ctx.getLensContext().getDriverStatus().isFinished()) {
      // terminal state. No updates can be done.
      return;
    }
    if (ctx.getResultFuture().isCancelled()) {
      if (!context.getDriverStatus().isCanceled()) {
        context.getDriverStatus().setProgress(1.0);
        context.getDriverStatus().setState(DriverQueryState.CANCELED);
        context.getDriverStatus().setStatusMessage("Query Canceled");
      }
    } else if (ctx.getResultFuture().isDone()) {
      context.getDriverStatus().setProgress(1.0);
      // Since future is already done, this call should not block
      if (ctx.getQueryResult() != null && ctx.getQueryResult().error != null) {
        if (!context.getDriverStatus().isFailed()) {
          context.getDriverStatus().setState(DriverQueryState.FAILED);
          context.getDriverStatus().setStatusMessage("Query execution failed!");
          context.getDriverStatus().setErrorMessage(ctx.getQueryResult().error.getMessage());
        }
      } else {
        if (!context.getDriverStatus().isFinished()) {
          // assuming successful
          context.getDriverStatus().setState(DriverQueryState.SUCCESSFUL);
          context.getDriverStatus().setStatusMessage(context.getQueryHandle() + " successful");
          context.getDriverStatus().setResultSetAvailable(true);
        }
      }
    } else {
      if (!context.getDriverStatus().isRunning()) {
        context.getDriverStatus().setState(DriverQueryState.RUNNING);
        context.getDriverStatus().setStatusMessage(context.getQueryHandle() + " is running");
      }
    }
  }

  @Override
  protected LensResultSet createResultSet(QueryContext ctx) throws LensException {
    checkConfigured();
    return getQueryContext(ctx.getQueryHandle()).getQueryResult().getLensResultSet(true);
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
    log.info("{} cancel request on query {}", getFullyQualifiedName(), handle);
    boolean cancelResult = context.cancel();
    if (cancelResult) {
      context.getLensContext().setDriverStatus(DriverQueryState.CANCELED);
      context.closeResult();
      log.info("{} Canceled query : {}", getFullyQualifiedName(), handle);
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
      for (QueryHandle query : new ArrayList<>(queryContextMap.keySet())) {
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

  @Override
  public StatusUpdateMethod getStatusUpdateMethod() {
    return StatusUpdateMethod.PUSH;
  }
}
