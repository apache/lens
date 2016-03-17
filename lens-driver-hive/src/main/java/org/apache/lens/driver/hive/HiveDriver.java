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
package org.apache.lens.driver.hive;

import static org.apache.lens.server.api.error.LensDriverErrorCode.*;
import static org.apache.lens.server.api.util.LensUtil.getImplementations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.Priority;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.cube.query.cost.FactPartitionBasedQueryCostCalculator;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.collect.WaitingQueriesSelectionPolicy;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.query.cost.QueryCostCalculator;
import org.apache.lens.server.api.query.priority.CostRangePriorityDecider;
import org.apache.lens.server.api.query.priority.CostToPriorityRangeConf;
import org.apache.lens.server.api.query.priority.QueryPriorityDecider;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.TaskStatus;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class HiveDriver.
 */
@Slf4j
public class HiveDriver extends AbstractLensDriver {

  /** The Constant HIVE_CONNECTION_CLASS. */
  public static final String HIVE_CONNECTION_CLASS = "lens.driver.hive.connection.class";

  public static final String HIVE_QUERY_HOOK_CLASS = "lens.driver.hive.query.hook.class";

  /** The Constant HS2_CONNECTION_EXPIRY_DELAY. */
  public static final String HS2_CONNECTION_EXPIRY_DELAY = "lens.driver.hive.hs2.connection.expiry.delay";

  public static final String HS2_CALCULATE_PRIORITY = "lens.driver.hive.calculate.priority";
  public static final String HS2_COST_CALCULATOR = "lens.driver.hive.cost.calculator.class";

  /**
   * Config param for defining priority ranges.
   */
  public static final String HS2_PRIORITY_RANGES = "lens.driver.hive.priority.ranges";

  // Default values of conf params
  public static final long DEFAULT_EXPIRY_DELAY = 600 * 1000;
  public static final String HS2_PRIORITY_DEFAULT_RANGES = "VERY_HIGH,7.0,HIGH,30.0,NORMAL,90,LOW";
  public static final String SESSION_KEY_DELIMITER = ".";

  public static final String QUERY_LAUNCHING_CONSTRAINT_FACTORIES_KEY
    = "lens.driver.hive.query.launching.constraint.factories";

  private static final String WAITING_QUERIES_SELECTION_POLICY_FACTORIES_KEY
    = "lens.driver.hive.waiting.queries.selection.policy.factories";

  /** The driver conf- which will merged with query conf */
  private Configuration driverConf;

  /** The HiveConf - used for connecting to hive server and metastore */
  private HiveConf hiveConf;

  /** The hive handles. */
  @Getter
  private Map<QueryHandle, OperationHandle> hiveHandles = new ConcurrentHashMap<QueryHandle, OperationHandle>();

  /** The orphaned hive sessions. */
  private ConcurrentLinkedQueue<SessionHandle> orphanedHiveSessions;

  /** The opHandle to hive session map. */
  private Map<OperationHandle, SessionHandle> opHandleToSession;

  /** The session lock. */
  private final Lock sessionLock;

  // connections need to be separate for each user and each thread
  /** The thread connections. */
  private final Map<String, ExpirableConnection> threadConnections =
    new ConcurrentHashMap<String, ExpirableConnection>();

  /** The thrift conn expiry queue. */
  private final DelayQueue<ExpirableConnection> thriftConnExpiryQueue = new DelayQueue<ExpirableConnection>();

  /** The connection expiry thread. */
  private final Thread connectionExpiryThread = new Thread(new ConnectionExpiryRunnable());

  // assigned only in case of embedded connection
  /** The embedded connection. */
  private ThriftConnection embeddedConnection;
  // Store mapping of Lens session ID to Hive session identifier
  /** The lens to hive session. */
  private Map<String, SessionHandle> lensToHiveSession;
  /** Keep track of resources added to the Hive session */
  private Map<SessionHandle, Boolean> resourcesAddedForSession;

  /** The driver listeners. */
  private List<LensEventListener<DriverEvent>> driverListeners;

  QueryCostCalculator queryCostCalculator;
  QueryPriorityDecider queryPriorityDecider;
  // package-local. Test case can change.
  boolean whetherCalculatePriority;
  private DriverQueryHook queryHook;

  @Getter
  protected ImmutableSet<QueryLaunchingConstraint> queryConstraints;
  private ImmutableSet<WaitingQueriesSelectionPolicy> selectionPolicies;

  private String sessionDbKey(String sessionHandle, String database) {
    return sessionHandle + SESSION_KEY_DELIMITER + database;
  }

  /**
   * Return true if resources have been added to this Hive session
   * @param sessionHandle lens session identifier
   * @param database lens database
   * @return true if resources have been already added to this session + db pair
   */
  public boolean areDBResourcesAddedForSession(String sessionHandle, String database) {
    String key = sessionDbKey(sessionHandle, database);
    SessionHandle hiveSession = lensToHiveSession.get(key);
    return hiveSession != null
      && resourcesAddedForSession.containsKey(hiveSession)
      && resourcesAddedForSession.get(hiveSession);
  }

  /**
   * Tell Hive driver that resources have been added for this session and for the given database
   * @param sessionHandle lens session identifier
   * @param database lens database
   */
  public void setResourcesAddedForSession(String sessionHandle, String database) {
    SessionHandle hiveSession = lensToHiveSession.get(sessionDbKey(sessionHandle, database));
    resourcesAddedForSession.put(hiveSession, Boolean.TRUE);
  }

  /**
   * The Class ConnectionExpiryRunnable.
   */
  class ConnectionExpiryRunnable implements Runnable {

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      try {
        while (true) {
          ExpirableConnection expired = thriftConnExpiryQueue.take();
          expired.setExpired();
          ThriftConnection thConn = expired.getConnection();

          if (thConn != null) {
            try {
              log.info("Closed connection: {}", expired.getConnId());
              thConn.close();
            } catch (IOException e) {
              log.error("Error closing connection", e);
            }
          }
        }
      } catch (InterruptedException intr) {
        log.warn("Connection expiry thread interrupted", intr);
        return;
      }
    }
  }

  /** The Constant CONNECTION_COUNTER. */
  private static final AtomicInteger CONNECTION_COUNTER = new AtomicInteger();

  /**
   * The Class ExpirableConnection.
   */
  static class ExpirableConnection implements Delayed {

    /** The access time. */
    long accessTime;

    /** The conn. */
    private final ThriftConnection conn;

    /** The timeout. */
    private final long timeout;

    /** The expired. */
    private volatile boolean expired;

    /** The conn id. */
    private final int connId;

    /**
     * Instantiates a new expirable connection.
     *
     * @param conn    the conn
     * @param timeout the timeout
     */
    public ExpirableConnection(ThriftConnection conn, long timeout) {
      this.conn = conn;
      this.timeout = timeout;
      connId = CONNECTION_COUNTER.incrementAndGet();
      accessTime = System.currentTimeMillis();
    }

    private ThriftConnection getConnection() {
      accessTime = System.currentTimeMillis();
      return conn;
    }

    private boolean isExpired() {
      return expired;
    }

    /**
     * Sets the expired.
     */
    private void setExpired() {
      expired = true;
    }

    private int getConnId() {
      return connId;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Delayed other) {
      return (int) (this.getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS));
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Delayed#getDelay(java.util.concurrent.TimeUnit)
     */
    @Override
    public long getDelay(TimeUnit unit) {
      long age = System.currentTimeMillis() - accessTime;
      return unit.convert(timeout - age, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Open connections.
   *
   * @return the int
   */
  int openConnections() {
    return thriftConnExpiryQueue.size();
  }

  /** The connection class. */
  private Class<? extends ThriftConnection> connectionClass;

  /** The is embedded. */
  private boolean isEmbedded;

  /** The connection expiry timeout. */
  private long connectionExpiryTimeout;

  /**
   * Instantiates a new hive driver.
   *
   * @throws LensException the lens exception
   */
  public HiveDriver() throws LensException {
    this.sessionLock = new ReentrantLock();
    lensToHiveSession = new HashMap<String, SessionHandle>();
    opHandleToSession = new ConcurrentHashMap<OperationHandle, SessionHandle>();
    orphanedHiveSessions = new ConcurrentLinkedQueue<SessionHandle>();
    resourcesAddedForSession = new HashMap<SessionHandle, Boolean>();
    connectionExpiryThread.setDaemon(true);
    connectionExpiryThread.setName("HiveDriver-ConnectionExpiryThread");
    connectionExpiryThread.start();
    driverListeners = new ArrayList<LensEventListener<DriverEvent>>();
    log.info("Hive driver inited");
  }

  @Override
  public Configuration getConf() {
    return driverConf;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#configure(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void configure(Configuration conf, String driverType, String driverName) throws LensException {
    super.configure(conf, driverType, driverName);
    this.driverConf = new Configuration(conf);
    String driverConfPath = getDriverResourcePath("hivedriver-site.xml");
    this.driverConf.addResource("hivedriver-default.xml");
    this.driverConf.addResource(driverConfPath);

    // resources have to be added separately on hiveConf again because new HiveConf() overrides hive.* properties
    // from HiveConf
    this.hiveConf = new HiveConf(conf, HiveDriver.class);
    this.hiveConf.addResource("hivedriver-default.xml");
    this.hiveConf.addResource(driverConfPath);

    connectionClass = this.driverConf.getClass(HIVE_CONNECTION_CLASS, EmbeddedThriftConnection.class,
      ThriftConnection.class);
    isEmbedded = (connectionClass.getName().equals(EmbeddedThriftConnection.class.getName()));
    connectionExpiryTimeout = this.driverConf.getLong(HS2_CONNECTION_EXPIRY_DELAY, DEFAULT_EXPIRY_DELAY);
    whetherCalculatePriority = this.driverConf.getBoolean(HS2_CALCULATE_PRIORITY, true);
    Class<? extends QueryCostCalculator> queryCostCalculatorClass = this.driverConf.getClass(HS2_COST_CALCULATOR,
      FactPartitionBasedQueryCostCalculator.class, QueryCostCalculator.class);
    try {
      queryCostCalculator = queryCostCalculatorClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new LensException("Can't instantiate query cost calculator of class: " + queryCostCalculatorClass, e);
    }
    queryPriorityDecider = new CostRangePriorityDecider(
      new CostToPriorityRangeConf(driverConf.get(HS2_PRIORITY_RANGES, HS2_PRIORITY_DEFAULT_RANGES))
    );
    try {
      queryHook = driverConf.getClass(
        HIVE_QUERY_HOOK_CLASS, NoOpDriverQueryHook.class, DriverQueryHook.class
      ).newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new LensException("Can't instantiate driver query hook for hivedriver with given class", e);
    }
    queryConstraints = getImplementations(QUERY_LAUNCHING_CONSTRAINT_FACTORIES_KEY, driverConf);
    selectionPolicies = getImplementations(WAITING_QUERIES_SELECTION_POLICY_FACTORIES_KEY, driverConf);
    log.info("Hive driver {} configured successfully", getFullyQualifiedName());
  }

  private QueryCost calculateQueryCost(AbstractQueryContext qctx) throws LensException {
    if (qctx.isOlapQuery()) {
      QueryCost cost = queryCostCalculator.calculateCost(qctx, this);
      if (cost != null) {
        return cost;
      }
    }
    return new FactPartitionBasedQueryCost(Double.MAX_VALUE);
  }

  @Override
  public QueryCost estimate(AbstractQueryContext qctx) throws LensException {
    log.info("{} Estimate: {}", getFullyQualifiedName(), qctx.getDriverQuery(this));
    if (qctx.getDriverQuery(this) == null) {
      throw new NullPointerException("Null driver query for " + qctx.getUserQuery());
    }
    if (qctx.getDriverContext().getDriverQueryCost(this) != null) {
      // estimate called again and again
      return qctx.getDriverContext().getDriverQueryCost(this);
    }
    if (qctx.isOlapQuery()) {
      // if query is olap query and rewriting takes care of semantic validation
      // estimate is calculating cost of the query
      // the calculation is done only for cube queries
      // for all other native table queries, the cost will be maximum
      return calculateQueryCost(qctx);
    } else {
      // its native table query. validate and return cost
      return explain(qctx).getCost();
    }
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#explain(java.lang.String, org.apache.hadoop.conf.Configuration)
   */
  @Override
  public HiveQueryPlan explain(AbstractQueryContext explainCtx) throws LensException {
    if (explainCtx.getDriverQuery(this) == null) {
      throw new NullPointerException("Null driver query for " + explainCtx.getUserQuery());
    }
    if (explainCtx.getDriverContext().getDriverQueryPlan(this) != null) {
      // explain called again and again
      return (HiveQueryPlan) explainCtx.getDriverContext().getDriverQueryPlan(this);
    }
    log.info("{} Explain: {}", getFullyQualifiedName(), explainCtx.getDriverQuery(this));
    Configuration explainConf = new Configuration(explainCtx.getDriverConf(this));
    explainConf.setClassLoader(explainCtx.getConf().getClassLoader());
    explainConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    final String explainQuery = "EXPLAIN EXTENDED " + explainCtx.getDriverQuery(this);

    QueryContext explainQueryCtx = QueryContext.createContextWithSingleDriver(explainQuery,
      explainCtx.getSubmittedUser(), new LensConf(), explainConf, this, explainCtx.getLensSessionIdentifier(), false);

    // Get result set of explain
    InMemoryResultSet inMemoryResultSet = (InMemoryResultSet) execute(explainQueryCtx);
    List<String> explainOutput = new ArrayList<>();
    while (inMemoryResultSet.hasNext()) {
      explainOutput.add((String) inMemoryResultSet.next().getValues().get(0));
    }
    closeQuery(explainQueryCtx.getQueryHandle());
    try {
      hiveConf.setClassLoader(explainCtx.getConf().getClassLoader());
      HiveQueryPlan hqp = new HiveQueryPlan(explainOutput, null, hiveConf, calculateQueryCost(explainCtx));
      explainCtx.getDriverContext().setDriverQueryPlan(this, hqp);
      return hqp;
    } catch (HiveException e) {
      throw new LensException("Unable to create hive query plan", e);
    }
  }

  // this is used for tests
  int getHiveHandleSize() {
    return hiveHandles.size();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.lens.server.api.driver.LensDriver#explainAndPrepare
   * (org.apache.lens.server.api.query.PreparedQueryContext)
   */
  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
    DriverQueryPlan plan = explain(pContext);
    plan.setPrepareHandle(pContext.getPrepareHandle());
    return plan;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#prepare(org.apache.lens.server.api.query.PreparedQueryContext)
   */
  @Override
  public void prepare(PreparedQueryContext pContext) throws LensException {
    // NO OP
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#closePreparedQuery(org.apache.lens.api.query.QueryPrepareHandle)
   */
  @Override
  public void closePreparedQuery(QueryPrepareHandle handle) throws LensException {
    // NO OP

  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#execute(org.apache.lens.server.api.query.QueryContext)
   */
  // assuming this is only called for executing explain/insert/set/delete/etc... queries which don't ask to fetch data.
  public LensResultSet execute(QueryContext ctx) throws LensException {
    OperationHandle op = null;
    LensResultSet result = null;
    try {
      addPersistentPath(ctx);
      Configuration qdconf = ctx.getDriverConf(this);
      qdconf.set("mapred.job.name", ctx.getQueryHandle().toString());
      SessionHandle sessionHandle = getSession(ctx);
      op = getClient().executeStatement(sessionHandle, ctx.getSelectedDriverQuery(),
        qdconf.getValByRegex(".*"));
      log.info("The hive operation handle: {}", op);
      ctx.setDriverOpHandle(op.toString());
      hiveHandles.put(ctx.getQueryHandle(), op);
      opHandleToSession.put(op, sessionHandle);
      updateStatus(ctx);
      OperationStatus status = getClient().getOperationStatus(op);

      if (status.getState() == OperationState.ERROR) {
        throw new LensException("Unknown error while running query " + ctx.getUserQuery());
      }
      result = createResultSet(ctx, true);
      // close the query immediately if the result is not inmemory result set
      if (result == null || !(result instanceof InMemoryResultSet)) {
        closeQuery(ctx.getQueryHandle());
      }
      // remove query handle from hiveHandles even in case of inmemory result set
      hiveHandles.remove(ctx.getQueryHandle());
    } catch (IOException e) {
      throw new LensException("Error adding persistent path", e);
    } catch (HiveSQLException hiveErr) {
      handleHiveServerError(ctx, hiveErr);
      handleHiveSQLException(hiveErr);
    } finally {
      if (null != op) {
        opHandleToSession.remove(op);
      }
    }
    return result;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#executeAsync(org.apache.lens.server.api.query.QueryContext)
   */
  @Override
  public void executeAsync(QueryContext ctx) throws LensException {
    try {
      addPersistentPath(ctx);
      Configuration qdconf = ctx.getDriverConf(this);
      qdconf.set("mapred.job.name", ctx.getQueryHandle().toString());
      decidePriority(ctx);
      queryHook.preLaunch(ctx);
      SessionHandle sessionHandle = getSession(ctx);
      OperationHandle op = getClient().executeStatementAsync(sessionHandle, ctx.getSelectedDriverQuery(),
        qdconf.getValByRegex(".*"));
      ctx.setDriverOpHandle(op.toString());
      log.info("QueryHandle: {} HiveHandle:{}", ctx.getQueryHandle(), op);
      hiveHandles.put(ctx.getQueryHandle(), op);
      opHandleToSession.put(op, sessionHandle);
    } catch (IOException e) {
      throw new LensException("Error adding persistent path", e);
    } catch (HiveSQLException e) {
      handleHiveServerError(ctx, e);
      handleHiveSQLException(e);
    }
  }

  private LensException handleHiveSQLException(HiveSQLException ex) throws LensException {
    if (ex.getMessage().contains("SemanticException")) {
      throw new LensException(SEMANTIC_ERROR.getLensErrorInfo(), ex, ex.getMessage());
    }
    throw new LensException(DRIVER_ERROR.getLensErrorInfo(), ex, ex.getMessage());
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#updateStatus(org.apache.lens.server.api.query.QueryContext)
   */
  @Override
  public void updateStatus(QueryContext context) throws LensException {
    log.debug("GetStatus: {}", context.getQueryHandle());
    if (context.getDriverStatus().isFinished()) {
      return;
    }
    OperationHandle hiveHandle = getHiveHandle(context.getQueryHandle());
    ByteArrayInputStream in = null;
    try {
      // Get operation status from hive server
      log.debug("GetStatus hiveHandle: {}", hiveHandle);
      OperationStatus opStatus = getClient().getOperationStatus(hiveHandle);
      log.debug("GetStatus on hiveHandle: {} returned state:", hiveHandle, opStatus.getState().name());

      switch (opStatus.getState()) {
      case CANCELED:
        context.getDriverStatus().setState(DriverQueryState.CANCELED);
        context.getDriverStatus().setStatusMessage("Query has been cancelled!");
        break;
      case CLOSED:
        context.getDriverStatus().setState(DriverQueryState.CLOSED);
        context.getDriverStatus().setStatusMessage("Query has been closed!");
        break;
      case ERROR:
        context.getDriverStatus().setState(DriverQueryState.FAILED);
        context.getDriverStatus().setStatusMessage("Query execution failed!");
        context.getDriverStatus().setErrorMessage(
          "Query failed with errorCode:" + opStatus.getOperationException().getErrorCode() + " with errorMessage: "
            + opStatus.getOperationException().getMessage());
        break;
      case FINISHED:
        context.getDriverStatus().setState(DriverQueryState.SUCCESSFUL);
        context.getDriverStatus().setStatusMessage("Query is successful!");
        context.getDriverStatus().setResultSetAvailable(hiveHandle.hasResultSet());
        break;
      case INITIALIZED:
        context.getDriverStatus().setState(DriverQueryState.INITIALIZED);
        context.getDriverStatus().setStatusMessage("Query is initiazed in HiveServer!");
        break;
      case RUNNING:
        context.getDriverStatus().setState(DriverQueryState.RUNNING);
        context.getDriverStatus().setStatusMessage("Query is running in HiveServer!");
        break;
      case PENDING:
        context.getDriverStatus().setState(DriverQueryState.PENDING);
        context.getDriverStatus().setStatusMessage("Query is pending in HiveServer");
        break;
      case UNKNOWN:
      default:
        throw new LensException("Query is in unknown state at HiveServer");
      }

      float progress = 0f;
      String jsonTaskStatus = opStatus.getTaskStatus();
      String errorMsg = null;
      if (StringUtils.isNotBlank(jsonTaskStatus)) {
        ObjectMapper mapper = new ObjectMapper();
        in = new ByteArrayInputStream(jsonTaskStatus.getBytes("UTF-8"));
        List<TaskStatus> taskStatuses = mapper.readValue(in, new TypeReference<List<TaskStatus>>() {
        });
        int completedTasks = 0;
        StringBuilder errorMessage = new StringBuilder();
        for (TaskStatus taskStat : taskStatuses) {
          String tstate = taskStat.getTaskState();
          if ("FINISHED_STATE".equalsIgnoreCase(tstate)) {
            completedTasks++;
          }
          if ("FAILED_STATE".equalsIgnoreCase(tstate)) {
            appendTaskIds(errorMessage, taskStat);
            errorMessage.append(" has failed! ");
          }
        }
        progress = taskStatuses.size() == 0 ? 0 : (float) completedTasks / taskStatuses.size();
        errorMsg = errorMessage.toString();
      } else {
        log.warn("Empty task statuses");
      }
      String error = null;
      if (StringUtils.isNotBlank(errorMsg)) {
        error = errorMsg;
      } else if (opStatus.getState().equals(OperationState.ERROR)) {
        error = context.getDriverStatus().getStatusMessage();
      }
      context.getDriverStatus().setErrorMessage(error);
      context.getDriverStatus().setProgressMessage(jsonTaskStatus);
      context.getDriverStatus().setProgress(progress);
      context.getDriverStatus().setDriverStartTime(opStatus.getOperationStarted());
      context.getDriverStatus().setDriverFinishTime(opStatus.getOperationCompleted());
    } catch (Exception e) {
      log.error("Error getting query status", e);
      handleHiveServerError(context, e);
      throw new LensException("Error getting query status", e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          log.error("Error closing stream.", e);
        }
      }
    }
  }

  /**
   * Append task ids.
   *
   * @param message  the message
   * @param taskStat the task stat
   */
  private void appendTaskIds(StringBuilder message, TaskStatus taskStat) {
    message.append(taskStat.getTaskId()).append("(");
    message.append(taskStat.getType()).append("):");
    if (taskStat.getExternalHandle() != null) {
      message.append(taskStat.getExternalHandle()).append(":");
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#closeResultSet(org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public void closeResultSet(QueryHandle handle) throws LensException {
    // NO OP ?
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#closeQuery(org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public void closeQuery(QueryHandle handle) throws LensException {
    if (handle == null) {
      return;
    }
    log.info("CloseQuery: {}", handle);
    OperationHandle opHandle = hiveHandles.remove(handle);
    if (opHandle != null) {
      log.info("CloseQuery hiveHandle: {}", opHandle);
      try {
        getClient().closeOperation(opHandle);
      } catch (HiveSQLException e) {
        checkInvalidOperation(handle, e);
        throw new LensException("Unable to close query", e);
      } finally {
        SessionHandle hiveSession = opHandleToSession.remove(opHandle);
        if (null != hiveSession && !opHandleToSession.containsValue(hiveSession)
          && orphanedHiveSessions.contains(hiveSession)) {
          orphanedHiveSessions.remove(hiveSession);
          try {
            getClient().closeSession(hiveSession);
            log.info("Closed orphaned hive session : {}", hiveSession.getHandleIdentifier());
          } catch (HiveSQLException e) {
            log.warn("Error closing orphan hive session : {} ", hiveSession.getHandleIdentifier(), e);
          }
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#cancelQuery(org.apache.lens.api.query.QueryHandle)
   */
  @Override
  public boolean cancelQuery(QueryHandle handle) throws LensException {
    log.info("CancelQuery: {}", handle);
    OperationHandle hiveHandle = getHiveHandle(handle);
    opHandleToSession.remove(hiveHandle);
    try {
      log.info("CancelQuery hiveHandle: {}", hiveHandle);
      getClient().cancelOperation(hiveHandle);
      return true;
    } catch (HiveSQLException e) {
      checkInvalidOperation(handle, e);
      throw new LensException();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.server.api.driver.LensDriver#close()
   */
  @Override
  public void close() {
    log.info("CloseDriver {}", getFullyQualifiedName());
    // Close this driver
    sessionLock.lock();
    lensToHiveSession.clear();
    orphanedHiveSessions.clear();
    sessionLock.unlock();
  }

  /**
   * Add a listener for driver events.
   *
   * @param driverEventListener the driver event listener
   */
  @Override
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {
    driverListeners.add(driverEventListener);
  }

  @Override
  public ImmutableSet<WaitingQueriesSelectionPolicy> getWaitingQuerySelectionPolicies() {
    return selectionPolicies;
  }

  @Override
  public Priority decidePriority(AbstractQueryContext ctx) {
    if (whetherCalculatePriority && ctx.getDriverConf(this).get("mapred.job.priority") == null) {
      try {
        // Inside try since non-data fetching queries can also be executed by async method.
        Priority priority = ctx.decidePriority(this, queryPriorityDecider);
        String priorityStr = priority.toString();
        ctx.getDriverConf(this).set("mapred.job.priority", priorityStr);
        log.info("set priority to {}", priority);
        return priority;
      } catch (Exception e) {
        // not failing query launch when setting priority fails
        // priority will be set to usually NORMAL - the default in underlying system.
        log.error("could not set priority for lens session id:{} User query: {}", ctx.getLensSessionIdentifier(),
          ctx.getUserQuery(), e);
        return null;
      }
    }
    return null;
  }

  protected CLIServiceClient getClient() throws LensException {
    if (isEmbedded) {
      if (embeddedConnection == null) {
        try {
          embeddedConnection = connectionClass.newInstance();
          embeddedConnection.init(hiveConf, null);
        } catch (Exception e) {
          throw new LensException(e);
        }
        log.info("New thrift connection {}", connectionClass);
      }
      return embeddedConnection.getClient();
    } else {
      String user = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_USER);
      if (SessionState.get() != null && SessionState.get().getUserName() != null) {
        user = SessionState.get().getUserName();
      }

      String connectionKey = user.toLowerCase() + Thread.currentThread().getId();
      ExpirableConnection connection = threadConnections.get(connectionKey);
      if (connection == null || connection.isExpired()) {
        try {
          ThriftConnection tconn = connectionClass.newInstance();
          tconn.init(hiveConf, user);
          connection = new ExpirableConnection(tconn, connectionExpiryTimeout);
          thriftConnExpiryQueue.offer(connection);
          threadConnections.put(connectionKey, connection);
          log.info("New thrift connection {} for thread: {} for user: {} connection ID={} on driver:{}",
            connectionClass, Thread.currentThread().getId(), user, connection.getConnId(), getFullyQualifiedName());
        } catch (Exception e) {
          throw new LensException(e);
        }
      } else {
        synchronized (thriftConnExpiryQueue) {
          thriftConnExpiryQueue.remove(connection);
          thriftConnExpiryQueue.offer(connection);
        }
      }
      return connection.getConnection().getClient();
    }
  }

  @Override
  protected LensResultSet createResultSet(QueryContext context) throws LensException {
    return createResultSet(context, false);
  }

  /**
   * Creates the result set.
   *
   * @param context         the context
   * @param closeAfterFetch the close after fetch
   * @return the lens result set
   * @throws LensException the lens exception
   */
  private LensResultSet createResultSet(QueryContext context, boolean closeAfterFetch) throws LensException {
    OperationHandle op = getHiveHandle(context.getQueryHandle());
    log.info("Creating result set for hiveHandle:{}", op);
    try {
      if (context.isDriverPersistent()) {
        return new HivePersistentResultSet(new Path(context.getDriverResultPath()), op, getClient());
      } else if (op.hasResultSet()) {
        return new HiveInMemoryResultSet(op, getClient(), closeAfterFetch);
      } else {
        // queries that do not have result
        return null;
      }
    } catch (HiveSQLException hiveErr) {
      handleHiveServerError(context, hiveErr);
      throw new LensException("Error creating result set", hiveErr);
    }
  }

  /**
   * Adds the persistent path.
   *
   * @param context the context
   * @throws IOException Signals that an I/O exception has occurred.
   */
  void addPersistentPath(QueryContext context) throws IOException {
    String hiveQuery;
    Configuration qdconf = context.getDriverConf(this);
    boolean addInsertOverwrite = qdconf.getBoolean(
      LensConfConstants.QUERY_ADD_INSERT_OVEWRITE, LensConfConstants.DEFAULT_ADD_INSERT_OVEWRITE);
    if (context.isDriverPersistent() && addInsertOverwrite
      && (context.getSelectedDriverQuery().startsWith("SELECT")
      || context.getSelectedDriverQuery().startsWith("select"))) {
      // store persistent data into user specified location
      // If absent, take default home directory
      Path resultSetPath = context.getHDFSResultDir();
      // create query
      StringBuilder builder = new StringBuilder("INSERT OVERWRITE DIRECTORY ");
      context.setDriverResultPath(
        resultSetPath.makeQualified(resultSetPath.getFileSystem(context.getConf())).toString());
      builder.append('"').append(resultSetPath).append("\" ");
      String outputDirFormat = qdconf.get(LensConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT);
      if (outputDirFormat != null) {
        builder.append(outputDirFormat);
      }
      builder.append(' ').append(context.getSelectedDriverQuery()).append(' ');
      hiveQuery = builder.toString();
    } else {
      context.unSetDriverPersistent();
      hiveQuery = context.getSelectedDriverQuery();
    }
    log.info("Hive driver {} query:{}", getFullyQualifiedName(), hiveQuery);
    context.setSelectedDriverQuery(hiveQuery);
  }

  /**
   * Gets the session.
   *
   * @param ctx the ctx
   * @return the session
   * @throws LensException the lens exception
   */
  private SessionHandle getSession(QueryContext ctx) throws LensException {
    sessionLock.lock();
    try {
      String lensSession = ctx.getLensSessionIdentifier();
      String sessionDbKey = sessionDbKey(lensSession, ctx.getDatabase());
      if (lensSession == null && SessionState.get() != null) {
        lensSession = SessionState.get().getSessionId();
      }

      if (lensSession == null) {
        throw new IllegalStateException("Current session state does not have a Lens session id");
      }

      SessionHandle hiveSession;
      if (!lensToHiveSession.containsKey(sessionDbKey)) {
        try {
          hiveSession = getClient().openSession(ctx.getClusterUser(), "");
          lensToHiveSession.put(sessionDbKey, hiveSession);
          log.info("New hive session for user: {} , lens session: {} , hive session handle: {} , driver : {}",
            ctx.getClusterUser(), sessionDbKey, hiveSession.getHandleIdentifier(), getFullyQualifiedName());
          for (LensEventListener<DriverEvent> eventListener : driverListeners) {
            try {
              eventListener.onEvent(new DriverSessionStarted(System.currentTimeMillis(), this, lensSession, hiveSession
                .getSessionId().toString()));
            } catch (Exception exc) {
              log.error("Error sending driver {} start event to listener {}", getFullyQualifiedName(), eventListener,
                exc);
            }
          }
        } catch (Exception e) {
          throw new LensException(e);
        }
      } else {
        hiveSession = lensToHiveSession.get(sessionDbKey);
      }
      return hiveSession;
    } finally {
      sessionLock.unlock();
    }
  }

  /**
   * Gets the hive handle.
   *
   * @param handle the handle
   * @return the hive handle
   * @throws LensException the lens exception
   */
  private OperationHandle getHiveHandle(QueryHandle handle) throws LensException {
    OperationHandle opHandle = hiveHandles.get(handle);
    if (opHandle == null) {
      throw new LensException("Query not found " + handle);
    }
    return opHandle;
  }

  /**
   * The Class QueryCompletionNotifier.
   */
  private class QueryCompletionNotifier implements Runnable {

    /** The poll interval. */
    long pollInterval;

    /** The hive handle. */
    OperationHandle hiveHandle;

    /** The timeout millis. */
    long timeoutMillis;

    /** The listener. */
    QueryCompletionListener listener;

    /** The handle. */
    QueryHandle handle;

    /**
     * Instantiates a new query completion notifier.
     *
     * @param handle        the handle
     * @param timeoutMillis the timeout millis
     * @param listener      the listener
     * @throws LensException the lens exception
     */
    QueryCompletionNotifier(QueryHandle handle, long timeoutMillis, QueryCompletionListener listener)
      throws LensException {
      this.handle = handle;
      this.timeoutMillis = timeoutMillis;
      this.listener = listener;
      this.pollInterval = timeoutMillis / 10;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      // till query is complete or timeout has reached
      long timeSpent = 0;
      String error;
      try {
        while (timeSpent <= timeoutMillis) {
          try {
            hiveHandle = getHiveHandle(handle);
            if (isFinished(hiveHandle)) {
              listener.onCompletion(handle);
              return;
            }
          } catch (LensException e) {
            log.debug("query handle: {} Not yet launched on driver {}", handle, getFullyQualifiedName());
          }
          Thread.sleep(pollInterval);
          timeSpent += pollInterval;
        }
        error = "timedout";
      } catch (Exception e) {
        log.warn("Error while polling for status", e);
        error = "error polling";
      }
      listener.onError(handle, error);
    }

    /**
     * Checks if is finished.
     *
     * @param hiveHandle the hive handle
     * @return true, if is finished
     * @throws LensException the lens exception
     */
    private boolean isFinished(OperationHandle hiveHandle) throws LensException {
      OperationState state;
      try {
        state = getClient().getOperationStatus(hiveHandle).getState();
      } catch (HiveSQLException e) {
        throw new LensException("Could not get Status", e);
      }
      if (state.equals(OperationState.FINISHED) || state.equals(OperationState.CANCELED)
        || state.equals(OperationState.ERROR) || state.equals(OperationState.CLOSED)) {
        return true;
      }
      return false;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.lens.server.api.driver.LensDriver#registerForCompletionNotification
   * (org.apache.lens.api.query.QueryHandle, long, org.apache.lens.server.api.driver.QueryCompletionListener)
   */
  @Override
  public void registerForCompletionNotification(
    QueryHandle handle, long timeoutMillis, QueryCompletionListener listener)
    throws LensException {
    Thread th = new Thread(new QueryCompletionNotifier(handle, timeoutMillis, listener));
    th.start();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    synchronized (hiveHandles) {
      int numHiveHnadles = in.readInt();
      for (int i = 0; i < numHiveHnadles; i++) {
        QueryHandle qhandle = (QueryHandle) in.readObject();
        OperationHandle opHandle = new OperationHandle((TOperationHandle) in.readObject());
        hiveHandles.put(qhandle, opHandle);
        log.debug("Hive driver {} recovered {}:{}", getFullyQualifiedName(), qhandle, opHandle);
      }
      log.info("Hive driver {} recovered {} queries", getFullyQualifiedName(), hiveHandles.size());
      int numSessions = in.readInt();
      for (int i = 0; i < numSessions; i++) {
        String lensId = in.readUTF();
        SessionHandle sHandle = new SessionHandle((TSessionHandle) in.readObject(),
          TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
        lensToHiveSession.put(lensId, sHandle);
      }
      log.info("Hive driver {} recovered {} sessions", getFullyQualifiedName(), lensToHiveSession.size());
    }
    int numOpHandles = in.readInt();
    for (int i = 0; i < numOpHandles; i++) {
      OperationHandle opHandle = new OperationHandle((TOperationHandle) in.readObject());
      SessionHandle sHandle = new SessionHandle((TSessionHandle) in.readObject(),
        TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
      opHandleToSession.put(opHandle, sHandle);
    }
    log.info("Hive driver {} recovered {} operation handles", getFullyQualifiedName(), opHandleToSession.size());
    int numOrphanedSessions = in.readInt();
    for (int i = 0; i < numOrphanedSessions; i++) {
      SessionHandle sHandle = new SessionHandle((TSessionHandle) in.readObject(),
        TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
      orphanedHiveSessions.add(sHandle);
    }
    log.info("Hive driver {} recovered {} orphaned sessions", getFullyQualifiedName(), orphanedHiveSessions.size());
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // Write the query handle to hive handle map to output
    synchronized (hiveHandles) {
      out.writeInt(hiveHandles.size());
      for (Map.Entry<QueryHandle, OperationHandle> entry : hiveHandles.entrySet()) {
        out.writeObject(entry.getKey());
        out.writeObject(entry.getValue().toTOperationHandle());
        log.debug("Hive driver {} persisted {}:{}", getFullyQualifiedName(), entry.getKey(), entry.getValue());
      }
      log.info("Hive driver {} persisted {} queries ", getFullyQualifiedName(), hiveHandles.size());
      out.writeInt(lensToHiveSession.size());
      for (Map.Entry<String, SessionHandle> entry : lensToHiveSession.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeObject(entry.getValue().toTSessionHandle());
      }
      log.info("Hive driver {} persisted {} sessions", getFullyQualifiedName(), lensToHiveSession.size());
      out.writeInt(opHandleToSession.size());
      for (Map.Entry<OperationHandle, SessionHandle> entry : opHandleToSession.entrySet()) {
        out.writeObject(entry.getKey().toTOperationHandle());
        out.writeObject(entry.getValue().toTSessionHandle());
      }
      log.info("Hive driver {} persisted {} operation handles", getFullyQualifiedName(), opHandleToSession.size());
      out.writeInt(orphanedHiveSessions.size());
      for (SessionHandle sessionHandle : orphanedHiveSessions) {
        out.writeObject(sessionHandle.toTSessionHandle());
      }
      log.info("Hive driver {} persisted {} orphaned sessions", getFullyQualifiedName(), orphanedHiveSessions.size());
    }
  }

  /**
   * Checks if is session invalid.
   *
   * @param exc           the exc
   * @param sessionHandle the session handle
   * @return true, if is session invalid
   */
  protected boolean isSessionInvalid(HiveSQLException exc, SessionHandle sessionHandle) {
    if (exc.getMessage().contains("Invalid SessionHandle") && exc.getMessage().contains(sessionHandle.toString())) {
      return true;
    }

    // Check if there is underlying cause
    if (exc.getCause() instanceof HiveSQLException) {
      isSessionInvalid((HiveSQLException) exc.getCause(), sessionHandle);
    }
    return false;
  }

  /**
   * Check invalid session.
   *
   * @param e the e
   */
  protected void checkInvalidSession(Exception e) {
    if (!(e instanceof HiveSQLException)) {
      return;
    }

    HiveSQLException exc = (HiveSQLException) e;

    String lensSession = null;
    if (SessionState.get() != null) {
      lensSession = SessionState.get().getSessionId();
    }

    if (lensSession == null) {
      return;
    }

    // Get all hive sessions corresponding to the lens session and check if
    // any of those sessions have become invalid
    List<String> sessionKeys = new ArrayList<String>(lensToHiveSession.keySet());
    List<SessionHandle> hiveSessionsToCheck = new ArrayList<SessionHandle>();
    sessionLock.lock();
    try {
      for (String key : sessionKeys) {
        if (key.startsWith(lensSession)) {
          hiveSessionsToCheck.add(lensToHiveSession.get(key));
        }
      }
    } finally {
      sessionLock.unlock();
    }

    for (SessionHandle session : hiveSessionsToCheck) {
      if (isSessionInvalid(exc, session)) {
        // We have to expire previous session
        log.info("{} Hive server session {} for lens session {} has become invalid", getFullyQualifiedName(), session,
          lensSession);
        sessionLock.lock();
        try {
          // We should close all connections and clear the session map since
          // most likely all sessions are gone
          closeAllConnections();
          lensToHiveSession.clear();
          log.info("{} Cleared all sessions", getFullyQualifiedName());
        } finally {
          sessionLock.unlock();
        }
      }
    }
  }

  /**
   * Check invalid operation.
   *
   * @param queryHandle the query handle
   * @param exc         the exc
   */
  protected void checkInvalidOperation(QueryHandle queryHandle, HiveSQLException exc) {
    final OperationHandle operation = hiveHandles.get(queryHandle);
    if (operation == null) {
      log.info("No hive operation available for {}", queryHandle);
      return;
    }
    if (exc.getMessage() != null && exc.getMessage().contains("Invalid OperationHandle:")
      && exc.getMessage().contains(operation.toString())) {
      log.info("Hive operation {} for query {} has become invalid", operation, queryHandle);
      hiveHandles.remove(queryHandle);
      return;
    }

    if (exc.getCause() instanceof HiveSQLException) {
      checkInvalidOperation(queryHandle, (HiveSQLException) exc.getCause());
    }

    return;
  }

  /**
   * Handle hive server error.
   *
   * @param ctx the ctx
   * @param exc the exc
   */
  protected void handleHiveServerError(QueryContext ctx, Exception exc) {
    if (exc instanceof HiveSQLException) {
      if (ctx != null) {
        checkInvalidOperation(ctx.getQueryHandle(), (HiveSQLException) exc);
      }
      checkInvalidSession((HiveSQLException) exc);
    }
  }

  /**
   * Close session.
   *
   * @param sessionHandle the session handle
   */
  public void closeSession(LensSessionHandle sessionHandle) {
    String sessionIdentifier = sessionHandle.getPublicId().toString();
    sessionLock.lock();
    try {
      for (String sessionDbKey : new ArrayList<String>(lensToHiveSession.keySet())) {
        if (sessionDbKey.startsWith(sessionIdentifier)) {
          SessionHandle hiveSession = lensToHiveSession.remove(sessionDbKey);
          if (hiveSession != null) {
            try {
              if (isSessionClosable(hiveSession)) {
                getClient().closeSession(hiveSession);
                log.info("Closed Hive session {} for lens session {}", hiveSession.getHandleIdentifier(),
                  sessionDbKey);
              } else {
                log.info("Skipped closing hive session {} for lens session {} due to active operations",
                  hiveSession.getHandleIdentifier(), sessionDbKey);
                orphanedHiveSessions.add(hiveSession);
              }
            } catch (Exception e) {
              log.error("Error closing hive session {} for lens session {}", hiveSession.getHandleIdentifier(),
                sessionDbKey, e);
            }
            resourcesAddedForSession.remove(hiveSession);
          }
        }
      }
    } finally {
      sessionLock.unlock();
    }
  }

  private boolean isSessionClosable(SessionHandle hiveSession) {
    return !opHandleToSession.containsValue(hiveSession);
  }

  /**
   * Close all connections.
   */
  private void closeAllConnections() {
    synchronized (thriftConnExpiryQueue) {
      for (ExpirableConnection connection : threadConnections.values()) {
        try {
          connection.getConnection().close();
        } catch (Exception ce) {
          log.warn("Error closing connection to hive server");
        }
      }
      threadConnections.clear();
    }
  }

  // For test

  /**
   * Checks for lens session.
   *
   * @param session the session
   * @return true, if successful
   */
  public boolean hasLensSession(LensSessionHandle session) {
    return lensToHiveSession.containsKey(session.getPublicId().toString());
  }
}
