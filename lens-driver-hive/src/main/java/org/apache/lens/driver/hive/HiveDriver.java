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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.TaskStatus;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.driver.DriverQueryStatus.DriverQueryState;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * The Class HiveDriver.
 */
public class HiveDriver implements LensDriver {

  /** The Constant LOG. */
  public static final Logger LOG = Logger.getLogger(HiveDriver.class);

  /** The Constant HIVE_CONNECTION_CLASS. */
  public static final String HIVE_CONNECTION_CLASS = "lens.driver.hive.connection.class";

  /** The Constant HS2_CONNECTION_EXPIRY_DELAY. */
  public static final String HS2_CONNECTION_EXPIRY_DELAY = "lens.driver.hs2.connection.expiry.delay";
  // Default expiry is 10 minutes
  /** The Constant DEFAULT_EXPIRY_DELAY. */
  public static final long DEFAULT_EXPIRY_DELAY = 600 * 1000;

  /** The driver conf. */
  private HiveConf driverConf;

  /** The hive handles. */
  private Map<QueryHandle, OperationHandle> hiveHandles = new HashMap<QueryHandle, OperationHandle>();

  /** The session lock. */
  private final Lock sessionLock;

  /** The connection lock. */
  private final Lock connectionLock;

  // connections need to be separate for each user and each thread
  /** The thread connections. */
  private final Map<String, Map<Long, ExpirableConnection>> threadConnections = new HashMap<String, Map<Long, ExpirableConnection>>();

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

  /** The driver listeners. */
  private List<LensEventListener<DriverEvent>> driverListeners;

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
              LOG.info("Closed connection:" + expired.getConnId());
              thConn.close();
            } catch (IOException e) {
              LOG.error("Error closing connection", e);
            }
          }
        }
      } catch (InterruptedException intr) {
        LOG.warn("Connection expiry thread interrupted", intr);
        return;
      }
    }
  }

  /** The Constant connectionCounter. */
  private static final AtomicInteger connectionCounter = new AtomicInteger();

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
     * @param conn
     *          the conn
     * @param timeout
     *          the timeout
     */
    public ExpirableConnection(ThriftConnection conn, long timeout) {
      this.conn = conn;
      this.timeout = timeout;
      connId = connectionCounter.incrementAndGet();
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
   * @throws LensException
   *           the lens exception
   */
  public HiveDriver() throws LensException {
    this.sessionLock = new ReentrantLock();
    this.connectionLock = new ReentrantLock();
    lensToHiveSession = new HashMap<String, SessionHandle>();
    connectionExpiryThread.setDaemon(true);
    connectionExpiryThread.setName("HiveDriver-ConnectionExpiryThread");
    connectionExpiryThread.start();
    driverListeners = new ArrayList<LensEventListener<DriverEvent>>();
    LOG.info("Hive driver inited");
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
  public void configure(Configuration conf) throws LensException {
    this.driverConf = new HiveConf(conf, HiveDriver.class);
    ;
    this.driverConf.addResource("hivedriver-default.xml");
    this.driverConf.addResource("hivedriver-site.xml");
    connectionClass = this.driverConf.getClass(HIVE_CONNECTION_CLASS, EmbeddedThriftConnection.class,
        ThriftConnection.class);
    isEmbedded = (connectionClass.getName().equals(EmbeddedThriftConnection.class.getName()));
    connectionExpiryTimeout = this.driverConf.getLong(HS2_CONNECTION_EXPIRY_DELAY, DEFAULT_EXPIRY_DELAY);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#explain(java.lang.String, org.apache.hadoop.conf.Configuration)
   */
  @Override
  public DriverQueryPlan explain(final String query, final Configuration conf) throws LensException {
    LOG.info("Explain: " + query);
    Configuration explainConf = new Configuration(conf);
    explainConf.setBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, false);
    String explainQuery = "EXPLAIN EXTENDED " + query;
    QueryContext explainQueryCtx = new QueryContext(explainQuery, null, explainConf);
    // Get result set of explain
    HiveInMemoryResultSet inMemoryResultSet = (HiveInMemoryResultSet) execute(explainQueryCtx);
    List<String> explainOutput = new ArrayList<String>();
    while (inMemoryResultSet.hasNext()) {
      explainOutput.add((String) inMemoryResultSet.next().getValues().get(0));
    }
    closeQuery(explainQueryCtx.getQueryHandle());
    try {
      return new HiveQueryPlan(explainOutput, null, this.driverConf);
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
   * org.apache.lens.server.api.driver.LensDriver#explainAndPrepare(org.apache.lens.server.api.query.PreparedQueryContext
   * )
   */
  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
    DriverQueryPlan plan = explain(pContext.getDriverQuery(), pContext.getConf());
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
  public LensResultSet execute(QueryContext ctx) throws LensException {
    try {
      addPersistentPath(ctx);
      ctx.getConf().set("mapred.job.name", ctx.getQueryHandle().toString());
      OperationHandle op = getClient().executeStatement(getSession(ctx), ctx.getDriverQuery(),
          ctx.getConf().getValByRegex(".*"));
      LOG.info("The hive operation handle: " + op);
      ctx.setDriverOpHandle(op.toString());
      hiveHandles.put(ctx.getQueryHandle(), op);
      updateStatus(ctx);
      OperationStatus status = getClient().getOperationStatus(op);

      if (status.getState() == OperationState.ERROR) {
        throw new LensException("Unknown error while running query " + ctx.getUserQuery());
      }
      LensResultSet result = createResultSet(ctx, true);
      // close the query immediately if the result is not inmemory result set
      if (result == null || !(result instanceof HiveInMemoryResultSet)) {
        closeQuery(ctx.getQueryHandle());
      }
      // remove query handle from hiveHandles even in case of inmemory result set
      hiveHandles.remove(ctx.getQueryHandle());
      return result;
    } catch (IOException e) {
      throw new LensException("Error adding persistent path", e);
    } catch (HiveSQLException hiveErr) {
      handleHiveServerError(ctx, hiveErr);
      throw new LensException("Error executing query", hiveErr);
    }
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
      ctx.getConf().set("mapred.job.name", ctx.getQueryHandle().toString());
      OperationHandle op = getClient().executeStatementAsync(getSession(ctx), ctx.getDriverQuery(),
          ctx.getConf().getValByRegex(".*"));
      ctx.setDriverOpHandle(op.toString());
      LOG.info("QueryHandle: " + ctx.getQueryHandle() + " HiveHandle:" + op);
      hiveHandles.put(ctx.getQueryHandle(), op);
    } catch (IOException e) {
      throw new LensException("Error adding persistent path", e);
    } catch (HiveSQLException e) {
      handleHiveServerError(ctx, e);
      throw new LensException("Error executing async query", e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.server.api.driver.LensDriver#updateStatus(org.apache.lens.server.api.query.QueryContext)
   */
  @Override
  public synchronized void updateStatus(QueryContext context) throws LensException {
    LOG.debug("GetStatus: " + context.getQueryHandle());
    if (context.getDriverStatus().isFinished()) {
      return;
    }
    OperationHandle hiveHandle = getHiveHandle(context.getQueryHandle());
    ByteArrayInputStream in = null;
    try {
      // Get operation status from hive server
      LOG.debug("GetStatus hiveHandle: " + hiveHandle);
      OperationStatus opStatus = getClient().getOperationStatus(hiveHandle);
      LOG.debug("GetStatus on hiveHandle: " + hiveHandle + " returned state:" + opStatus);

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
        context.getDriverStatus().setStatusMessage(
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
        LOG.warn("Empty task statuses");
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
      LOG.error("Error getting query status", e);
      handleHiveServerError(context, e);
      throw new LensException("Error getting query status", e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Append task ids.
   *
   * @param message
   *          the message
   * @param taskStat
   *          the task stat
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
   * @see org.apache.lens.server.api.driver.LensDriver#fetchResultSet(org.apache.lens.server.api.query.QueryContext)
   */
  @Override
  public LensResultSet fetchResultSet(QueryContext ctx) throws LensException {
    LOG.info("FetchResultSet: " + ctx.getQueryHandle());
    // This should be applicable only for a async query
    return createResultSet(ctx, false);
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
    LOG.info("CloseQuery: " + handle);
    OperationHandle opHandle = hiveHandles.remove(handle);
    if (opHandle != null) {
      LOG.info("CloseQuery: " + opHandle);
      try {
        getClient().closeOperation(opHandle);
      } catch (HiveSQLException e) {
        checkInvalidOperation(handle, e);
        throw new LensException("Unable to close query", e);
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
    LOG.info("CancelQuery: " + handle);
    OperationHandle hiveHandle = getHiveHandle(handle);
    try {
      LOG.info("CancelQuery hiveHandle: " + hiveHandle);
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
    LOG.info("CloseDriver");
    // Close this driver and release all resources
    sessionLock.lock();
    try {
      for (String lensSession : lensToHiveSession.keySet()) {
        try {
          getClient().closeSession(lensToHiveSession.get(lensSession));
        } catch (Exception e) {
          checkInvalidSession(e);
          LOG.warn(
              "Error closing session for lens session: " + lensSession + ", hive session: "
                  + lensToHiveSession.get(lensSession), e);
        }
      }
      lensToHiveSession.clear();
    } finally {
      sessionLock.unlock();
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
    driverListeners.add(driverEventListener);
  }

  protected CLIServiceClient getClient() throws LensException {
    if (isEmbedded) {
      if (embeddedConnection == null) {
        try {
          embeddedConnection = connectionClass.newInstance();
        } catch (Exception e) {
          throw new LensException(e);
        }
        LOG.info("New thrift connection " + connectionClass);
      }
      return embeddedConnection.getClient(driverConf);
    } else {
      connectionLock.lock();
      try {
        HiveConf connectionConf = driverConf;
        if (SessionState.get() != null && SessionState.get().getUserName() != null) {
          connectionConf = new HiveConf(driverConf);
          connectionConf.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_USER.varname, SessionState.get()
              .getUserName());
        }
        String user = connectionConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_USER);
        Map<Long, ExpirableConnection> userThreads = threadConnections.get(user.toLowerCase());
        if (userThreads == null) {
          userThreads = new HashMap<Long, ExpirableConnection>();
          threadConnections.put(user.toLowerCase(), userThreads);
        }
        ExpirableConnection connection = userThreads.get(Thread.currentThread().getId());
        if (connection == null || connection.isExpired()) {
          try {
            ThriftConnection tconn = connectionClass.newInstance();
            connection = new ExpirableConnection(tconn, connectionExpiryTimeout);
            thriftConnExpiryQueue.offer(connection);
            userThreads.put(Thread.currentThread().getId(), connection);
            LOG.info("New thrift connection " + connectionClass + " for thread:" + Thread.currentThread().getId()
                + " for user:" + user + " connection ID=" + connection.getConnId());
          } catch (Exception e) {
            throw new LensException(e);
          }
        } else {
          synchronized (thriftConnExpiryQueue) {
            thriftConnExpiryQueue.remove(connection);
            thriftConnExpiryQueue.offer(connection);
          }
        }
        return connection.getConnection().getClient(connectionConf);
      } finally {
        connectionLock.unlock();
      }

    }
  }

  /**
   * Creates the result set.
   *
   * @param context
   *          the context
   * @param closeAfterFetch
   *          the close after fetch
   * @return the lens result set
   * @throws LensException
   *           the lens exception
   */
  private LensResultSet createResultSet(QueryContext context, boolean closeAfterFetch) throws LensException {
    OperationHandle op = getHiveHandle(context.getQueryHandle());
    LOG.info("Creating result set for hiveHandle:" + op);
    try {
      if (op.hasResultSet() || context.isDriverPersistent()) {
        if (context.isDriverPersistent()) {
          return new HivePersistentResultSet(new Path(context.getHdfsoutPath()), op, getClient());
        } else {
          return new HiveInMemoryResultSet(op, getClient(), closeAfterFetch);
        }
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
   * @param context
   *          the context
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  void addPersistentPath(QueryContext context) throws IOException {
    String hiveQuery;
    if (context.isDriverPersistent()
        && context.getConf().getBoolean(LensConfConstants.QUERY_ADD_INSERT_OVEWRITE,
            LensConfConstants.DEFAULT_ADD_INSERT_OVEWRITE)) {
      // store persistent data into user specified location
      // If absent, take default home directory
      Path resultSetPath = context.getHDFSResultDir();
      // create query
      StringBuilder builder = new StringBuilder("INSERT OVERWRITE DIRECTORY ");
      context.setHdfsoutPath(resultSetPath.makeQualified(resultSetPath.getFileSystem(context.getConf())).toString());
      builder.append('"').append(resultSetPath).append("\" ");
      String outputDirFormat = context.getConf().get(LensConfConstants.QUERY_OUTPUT_DIRECTORY_FORMAT);
      if (outputDirFormat != null) {
        builder.append(outputDirFormat);
      }
      builder.append(' ').append(context.getDriverQuery()).append(' ');
      hiveQuery = builder.toString();
    } else {
      hiveQuery = context.getDriverQuery();
    }
    LOG.info("Hive driver query:" + hiveQuery);
    context.setDriverQuery(hiveQuery);
  }

  /**
   * Gets the session.
   *
   * @param ctx
   *          the ctx
   * @return the session
   * @throws LensException
   *           the lens exception
   */
  private SessionHandle getSession(QueryContext ctx) throws LensException {
    sessionLock.lock();
    try {
      String lensSession = ctx.getLensSessionIdentifier();
      if (lensSession == null && SessionState.get() != null) {
        lensSession = SessionState.get().getSessionId();
      }

      if (lensSession == null) {
        throw new IllegalStateException("Current session state does not have a Lens session id");
      }

      SessionHandle hiveSession;
      if (!lensToHiveSession.containsKey(lensSession)) {
        try {
          hiveSession = getClient().openSession(ctx.getClusterUser(), "");
          lensToHiveSession.put(lensSession, hiveSession);
          LOG.info("New hive session for user: " + ctx.getClusterUser() + ", lens session: " + lensSession
              + " session handle: " + hiveSession.getHandleIdentifier());
          for (LensEventListener<DriverEvent> eventListener : driverListeners) {
            try {
              eventListener.onEvent(new DriverSessionStarted(System.currentTimeMillis(), this, lensSession, hiveSession
                  .getSessionId().toString()));
            } catch (Exception exc) {
              LOG.error("Error sending driver start event to listener " + eventListener, exc);
            }
          }
        } catch (Exception e) {
          throw new LensException(e);
        }
      } else {
        hiveSession = lensToHiveSession.get(lensSession);
      }
      return hiveSession;
    } finally {
      sessionLock.unlock();
    }
  }

  /**
   * Gets the hive handle.
   *
   * @param handle
   *          the handle
   * @return the hive handle
   * @throws LensException
   *           the lens exception
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
     * @param handle
     *          the handle
     * @param timeoutMillis
     *          the timeout millis
     * @param listener
     *          the listener
     * @throws LensException
     *           the lens exception
     */
    QueryCompletionNotifier(QueryHandle handle, long timeoutMillis, QueryCompletionListener listener)
        throws LensException {
      hiveHandle = getHiveHandle(handle);
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
      String error = null;
      try {
        while (timeSpent <= timeoutMillis) {
          if (isFinished(hiveHandle)) {
            listener.onCompletion(handle);
            return;
          }
          Thread.sleep(pollInterval);
          timeSpent += pollInterval;
        }
        error = "timedout";
      } catch (Exception e) {
        LOG.warn("Error while polling for status", e);
        error = "error polling";
      }
      listener.onError(handle, error);
    }

    /**
     * Checks if is finished.
     *
     * @param hiveHandle
     *          the hive handle
     * @return true, if is finished
     * @throws LensException
     *           the lens exception
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
   * org.apache.lens.server.api.driver.LensDriver#registerForCompletionNotification(org.apache.lens.api.query.QueryHandle
   * , long, org.apache.lens.server.api.driver.QueryCompletionListener)
   */
  @Override
  public void registerForCompletionNotification(QueryHandle handle, long timeoutMillis, QueryCompletionListener listener)
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
        LOG.debug("Hive driver recovered " + qhandle + ":" + opHandle);
      }
      LOG.info("HiveDriver recovered " + hiveHandles.size() + " queries");
      int numSessions = in.readInt();
      for (int i = 0; i < numSessions; i++) {
        String lensId = in.readUTF();
        SessionHandle sHandle = new SessionHandle((TSessionHandle) in.readObject(),
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
        lensToHiveSession.put(lensId, sHandle);
      }
      LOG.info("HiveDriver recovered " + lensToHiveSession.size() + " sessions");
    }
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
        LOG.debug("Hive driver persisted " + entry.getKey() + ":" + entry.getValue());
      }
      LOG.info("HiveDriver persisted " + hiveHandles.size() + " queries");
      out.writeInt(lensToHiveSession.size());
      for (Map.Entry<String, SessionHandle> entry : lensToHiveSession.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeObject(entry.getValue().toTSessionHandle());
      }
      LOG.info("HiveDriver persisted " + lensToHiveSession.size() + " sessions");
    }
  }

  /**
   * Checks if is session invalid.
   *
   * @param exc
   *          the exc
   * @param sessionHandle
   *          the session handle
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
   * @param e
   *          the e
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

    SessionHandle session = lensToHiveSession.get(lensSession);

    if (session == null || lensSession == null) {
      return;
    }

    if (isSessionInvalid(exc, session)) {
      // We have to expire previous session
      LOG.info("Hive server session " + session + " for lens session " + lensSession + " has become invalid");
      sessionLock.lock();
      try {
        // We should close all connections and clear the session map since
        // most likely all sessions are gone
        closeAllConnections();
        lensToHiveSession.clear();
        LOG.info("Cleared all sessions");
      } finally {
        sessionLock.unlock();
      }
    }
  }

  /**
   * Check invalid operation.
   *
   * @param queryHandle
   *          the query handle
   * @param exc
   *          the exc
   */
  protected void checkInvalidOperation(QueryHandle queryHandle, HiveSQLException exc) {
    final OperationHandle operation = hiveHandles.get(queryHandle);
    if (operation == null) {
      LOG.info("No hive operation available for " + queryHandle);
      return;
    }
    if (exc.getMessage() != null && exc.getMessage().contains("Invalid OperationHandle:")
        && exc.getMessage().contains(operation.toString())) {
      LOG.info("Hive operation " + operation + " for query " + queryHandle + " has become invalid");
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
   * @param ctx
   *          the ctx
   * @param exc
   *          the exc
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
   * @param sessionHandle
   *          the session handle
   */
  public void closeSession(LensSessionHandle sessionHandle) {
    sessionLock.lock();
    try {
      SessionHandle hiveSession = lensToHiveSession.remove(sessionHandle.getPublicId().toString());
      if (hiveSession != null) {
        try {
          getClient().closeSession(hiveSession);
          LOG.info("Closed Hive session " + hiveSession.getHandleIdentifier() + " for lens session "
              + sessionHandle.getPublicId());
        } catch (Exception e) {
          LOG.error("Error closing hive session " + hiveSession.getHandleIdentifier() + " for lens session "
              + sessionHandle.getPublicId(), e);
        }
      }
    } finally {
      sessionLock.unlock();
    }
  }

  /**
   * Close all connections.
   */
  private void closeAllConnections() {
    connectionLock.lock();
    try {
      synchronized (thriftConnExpiryQueue) {
        for (Map<Long, ExpirableConnection> connections : threadConnections.values()) {
          for (ExpirableConnection connection : connections.values()) {
            try {
              connection.getConnection().close();
            } catch (Exception ce) {
              LOG.warn("Error closing connection to hive server");
            }
          }
        }
        threadConnections.clear();
      }
    } finally {
      connectionLock.unlock();
    }
  }

  // For test
  /**
   * Checks for lens session.
   *
   * @param session
   *          the session
   * @return true, if successful
   */
  public boolean hasLensSession(LensSessionHandle session) {
    return lensToHiveSession.containsKey(session.getPublicId().toString());
  }
}
