package com.inmobi.grill.driver.hive;

/*
 * #%L
 * Grill Hive Driver
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.DriverQueryPlan;
import com.inmobi.grill.server.api.driver.GrillDriver;
import com.inmobi.grill.server.api.driver.GrillResultSet;
import com.inmobi.grill.server.api.driver.QueryCompletionListener;
import com.inmobi.grill.server.api.query.PreparedQueryContext;
import com.inmobi.grill.server.api.query.QueryContext;

public class HiveDriver implements GrillDriver {
  public static final Logger LOG = Logger.getLogger(HiveDriver.class);

  public static final String GRILL_HIVE_CONNECTION_CLASS = "grill.hive.connection.class";
  public static final String GRILL_CONNECTION_EXPIRY_DELAY = "grill.hs2.connection.expiry.delay";
  // Default expiry is 10 minutes
  public static final long DEFAULT_EXPIRY_DELAY = 600 * 1000;

  private HiveConf conf;
  private Map<QueryHandle, OperationHandle> hiveHandles =
      new HashMap<QueryHandle, OperationHandle>();
  private final Lock sessionLock;

  private final Map<Long, ExpirableConnection> threadConnections = 
      new HashMap<Long, ExpirableConnection>();
  private final DelayQueue<ExpirableConnection> thriftConnExpiryQueue = 
      new DelayQueue<ExpirableConnection>();
  private final Thread connectionExpiryThread = new Thread(new ConnectionExpiryRunnable());
  
  // assigned only in case of embedded connection
  private ThriftConnection embeddedConnection;
  // Store mapping of Grill session ID to Hive session identifier
  private Map<String, SessionHandle> grillToHiveSession;

  class ConnectionExpiryRunnable implements Runnable {
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

  private static final AtomicInteger connectionCounter = new AtomicInteger();
  static class ExpirableConnection implements Delayed {
    long accessTime;
    private final ThriftConnection conn;
    private final long timeout;
    private volatile boolean expired;
    private final int connId;

    public ExpirableConnection(ThriftConnection conn, HiveConf conf) {
      this.conn = conn;
      this.timeout = 
          conf.getLong(GRILL_CONNECTION_EXPIRY_DELAY, DEFAULT_EXPIRY_DELAY);
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

    private void setExpired() {
      expired = true;
    }

    private int getConnId() {
      return connId;
    }

    @Override
    public int compareTo(Delayed other) {
      return (int)(this.getDelay(TimeUnit.MILLISECONDS)
          - other.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public long getDelay(TimeUnit unit) {
      long age = System.currentTimeMillis() - accessTime;
      return unit.convert(timeout - age, TimeUnit.MILLISECONDS) ;
    }
  }

  int openConnections() {
    return thriftConnExpiryQueue.size();
  }

  private Class<? extends ThriftConnection> connectionClass;
  private boolean isEmbedded;
  public HiveDriver() throws GrillException {
    this.sessionLock = new ReentrantLock();
    grillToHiveSession = new HashMap<String, SessionHandle>();
    connectionExpiryThread.setDaemon(true);
    connectionExpiryThread.setName("HiveDriver-ConnectionExpiryThread");
    connectionExpiryThread.start();
    LOG.info("Hive driver inited");
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void configure(Configuration conf) throws GrillException {
    this.conf = new HiveConf(conf, HiveDriver.class);
    connectionClass = conf.getClass(
        GRILL_HIVE_CONNECTION_CLASS, 
        EmbeddedThriftConnection.class, 
        ThriftConnection.class);
    isEmbedded = (connectionClass.getName().equals(EmbeddedThriftConnection.class.getName()));
  }

  @Override
  public DriverQueryPlan explain(final String query, final Configuration conf)
      throws GrillException {
    HiveConf explainConf = new HiveConf(conf, HiveDriver.class);
    explainConf.setBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, false);
    String explainQuery = "EXPLAIN EXTENDED " + query;
    QueryContext explainQueryCtx = new QueryContext(explainQuery, null, explainConf);
    // Get result set of explain
    HiveInMemoryResultSet inMemoryResultSet = (HiveInMemoryResultSet) execute(
        explainQueryCtx);
    List<String> explainOutput = new ArrayList<String>();
    while (inMemoryResultSet.hasNext()) {
      explainOutput.add((String)inMemoryResultSet.next().getValues().get(0));
    }
    LOG.info("Explain: " + query);
    try {
      return new HiveQueryPlan(explainOutput, null,
          new HiveConf(conf, HiveDriver.class));
    } catch (HiveException e) {
      throw new GrillException("Unable to create hive query plan", e);
    }
  }

  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws GrillException {
    DriverQueryPlan plan = explain(pContext.getDriverQuery(), pContext.getConf());
    plan.setPrepareHandle(pContext.getPrepareHandle());
    return plan;
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws GrillException {
    // NO OP
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle)
      throws GrillException {
    // NO OP

  }

  public GrillResultSet execute(QueryContext ctx) throws GrillException {
    try {
      addPersistentPath(ctx);
      ctx.getConf().set("mapred.job.name", ctx.getQueryHandle().toString());
      OperationHandle op = getClient().executeStatement(getSession(ctx), ctx.getDriverQuery(),
          ctx.getConf().getValByRegex(".*"));
      LOG.info("The hive operation handle: " + op);
      ctx.setDriverOpHandle(op.toString());
      hiveHandles.put(ctx.getQueryHandle(), op);
      OperationStatus status = getClient().getOperationStatus(op);

      if (status.getState() == OperationState.ERROR) {
        throw new GrillException("Unknown error while running query " + ctx.getUserQuery());
      }
      return createResultSet(ctx);
    } catch (IOException e) {
      throw new GrillException("Error adding persistent path" , e);
    } catch (HiveSQLException hiveErr) {
      throw new GrillException("Error executing query" , hiveErr);
    }
  }

  @Override
  public void executeAsync(QueryContext ctx)
      throws GrillException {
    try {
      addPersistentPath(ctx);
      ctx.getConf().set("mapred.job.name", ctx.getQueryHandle().toString());
      OperationHandle op = getClient().executeStatementAsync(getSession(ctx),
          ctx.getDriverQuery(), 
          ctx.getConf().getValByRegex(".*"));
      LOG.info("QueryHandle: " + ctx.getQueryHandle() + " HiveHandle:" + op);
      hiveHandles.put(ctx.getQueryHandle(), op);
    } catch (IOException e) {
      throw new GrillException("Error adding persistent path" , e);
    } catch (HiveSQLException e) {
      throw new GrillException("Error executing async query", e);
    }
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle)  throws GrillException {
    LOG.debug("GetStatus: " + handle);
    OperationHandle hiveHandle = getHiveHandle(handle);
    ByteArrayInputStream in = null;
    boolean hasResult = false;
    try {
      // Get operation status from hive server
      LOG.debug("GetStatus hiveHandle: " + hiveHandle);
      OperationStatus opStatus = getClient().getOperationStatus(hiveHandle);
      LOG.debug("GetStatus on hiveHandle: " + hiveHandle + " returned state:" + opStatus);
      QueryStatus.Status stat = null;
      String statusMessage;

      switch (opStatus.getState()) {
      case CANCELED:
        stat = Status.CANCELED;
        statusMessage = "Query has been cancelled!";
        break;
      case CLOSED:
        stat = Status.CLOSED;
        statusMessage = "Query has been closed!";
        break;
      case ERROR:
        stat = Status.FAILED;
        statusMessage = "Query failed with errorCode:" +
            opStatus.getOperationException().getErrorCode() +
            " with errorMessage: " + opStatus.getOperationException().getMessage();
        break;
      case FINISHED:
        statusMessage = "Query is successful!"; 
        stat = Status.SUCCESSFUL;
        hasResult = true;
        break;
      case INITIALIZED:
        statusMessage = "Query is initiazed in HiveServer!";
        stat = Status.RUNNING;
        break;
      case RUNNING:
        statusMessage = "Query is running in HiveServer!";
        stat = Status.RUNNING;
        break;
      case PENDING:
        stat = Status.LAUNCHED;
        statusMessage = "Query is pending in HiveServer";
        break;
      case UNKNOWN:
        throw new GrillException("Query is in unknown state at HiveServer");
      default :
        statusMessage = "";
        break;
      }

      float progress = 0f;
      String jsonTaskStatus = opStatus.getTaskStatus();
      String errorMsg = null;
      if (StringUtils.isNotBlank(jsonTaskStatus)) {
        ObjectMapper mapper = new ObjectMapper();
        in = new ByteArrayInputStream(jsonTaskStatus.getBytes("UTF-8"));
        List<TaskStatus> taskStatuses = 
            mapper.readValue(in, new TypeReference<List<TaskStatus>>() {});
        int completedTasks = 0;
        StringBuilder errorMessage = new StringBuilder();
        for (TaskStatus taskStat : taskStatuses) {
          String state = taskStat.getTaskState();
          if ("FINISHED_STATE".equalsIgnoreCase(state)) {
            completedTasks++;
          }
          if ("FAILED_STATE".equalsIgnoreCase(state)) {
            appendTaskIds(errorMessage, taskStat);
            errorMessage.append(" has failed! ");
          }
        }
        progress = taskStatuses.size() == 0 ? 0 : (float)completedTasks/taskStatuses.size();
        errorMsg = errorMessage.toString();
      } else {
        LOG.warn("Empty task statuses");
      }
      String error = null;
      if (StringUtils.isNotBlank(errorMsg)) {
        error = errorMsg;
      } else if (stat.equals(Status.FAILED)) {
        error = statusMessage;
      }
      return new QueryStatus(progress, stat, statusMessage, hasResult, jsonTaskStatus, error);
    } catch (Exception e) {
      LOG.error("Error getting query status", e);
      throw new GrillException("Error getting query status", e);
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

  private void appendTaskIds(StringBuilder message, TaskStatus taskStat) {
    message.append(taskStat.getTaskId()).append("(");
    message.append(taskStat.getType()).append("):");
    if (taskStat.getExternalHandle() != null) {
      message.append(taskStat.getExternalHandle()).append(":");
    }    
  }

  @Override
  public GrillResultSet fetchResultSet(QueryContext ctx)  throws GrillException {
    LOG.info("FetchResultSet: " + ctx.getQueryHandle());
    // This should be applicable only for a async query
    return createResultSet(ctx);
  }

  @Override
  public void closeResultSet(QueryHandle handle) throws GrillException {
    // NO OP ?
  }

  @Override
  public void closeQuery(QueryHandle handle) throws GrillException {
    LOG.info("CloseQuery: " + handle);
    OperationHandle opHandle = hiveHandles.remove(handle);
    if (opHandle != null) {
      LOG.info("CloseQuery: " + opHandle);
      try {
        getClient().closeOperation(opHandle);
      } catch (HiveSQLException e) {
        throw new GrillException("Unable to close query", e);
      }
    }
  }

  @Override
  public boolean cancelQuery(QueryHandle handle)  throws GrillException {
    LOG.info("CancelQuery: " + handle);
    OperationHandle hiveHandle = getHiveHandle(handle);
    try {
      LOG.info("CancelQuery hiveHandle: " + hiveHandle);
      getClient().cancelOperation(hiveHandle);
      return true;
    } catch (HiveSQLException e) {
      throw new GrillException();
    }
  }

  @Override
  public void close() {
    LOG.info("CloseDriver");
    // Close this driver and release all resources
    for (QueryHandle query : new ArrayList<QueryHandle>(hiveHandles.keySet())) {
      try {
        closeQuery(query);
      } catch (GrillException exc) {
        LOG.warn("Could not close query: " +  query, exc);
      }
    }

    sessionLock.lock();
    try {
      for (String grillSession : grillToHiveSession.keySet()) {
        try {
          getClient().closeSession(grillToHiveSession.get(grillSession));
        } catch (Exception e) {
          LOG.warn("Error closing session for grill session: " + grillSession + ", hive session: "
              + grillToHiveSession.get(grillSession), e);
        }
      }
      grillToHiveSession.clear();
    } finally {
      sessionLock.unlock();
    }
  }

  protected CLIServiceClient getClient() throws GrillException {
    if (isEmbedded) {
      if (embeddedConnection == null) {
        try {
          embeddedConnection = connectionClass.newInstance();
        } catch (Exception e) {
          throw new GrillException(e);
        }
        LOG.info("New thrift connection " + connectionClass);
      }
      return embeddedConnection.getClient(conf);
    } else {
    ExpirableConnection connection = threadConnections.get(Thread.currentThread().getId());
    if (connection == null || connection.isExpired()) {
      try {
        ThriftConnection tconn = connectionClass.newInstance();
        connection = new ExpirableConnection(tconn, conf);
        thriftConnExpiryQueue.offer(connection);
        threadConnections.put(Thread.currentThread().getId(), connection);
        LOG.info("New thrift connection " + connectionClass + " for thread:"
        + Thread.currentThread().getId() + " connection ID=" + connection.getConnId());
      } catch (Exception e) {
        throw new GrillException(e);
      }
    } else {
      synchronized(thriftConnExpiryQueue) {
        thriftConnExpiryQueue.remove(connection);
        thriftConnExpiryQueue.offer(connection);
      }
    }

    return connection.getConnection().getClient(conf);
    }
  }

  private GrillResultSet createResultSet(QueryContext context)
      throws GrillException {
    LOG.info("Creating result set for hiveHandle:" + hiveHandles.get(context.getQueryHandle()));
    if (context.isPersistent()) {
      return new HivePersistentResultSet(new Path(context.getResultSetPath()),
          hiveHandles.get(context.getQueryHandle()), getClient(), context.getQueryHandle());
    } else {
      return new HiveInMemoryResultSet(
          hiveHandles.get(context.getQueryHandle()), getClient());
    }
  }

  void addPersistentPath(QueryContext context) throws IOException {
    String hiveQuery;
    if (context.isPersistent() &&
        context.getConf().getBoolean(GrillConfConstants.GRILL_ADD_INSERT_OVEWRITE, true)) {
      // store persistent data into user specified location
      // If absent, take default home directory
      String resultSetParentDir = context.getResultSetPersistentPath();
      StringBuilder builder;
      Path resultSetPath;
      if (StringUtils.isNotBlank(resultSetParentDir)) {
        resultSetPath = new Path(resultSetParentDir, context.getQueryHandle().toString());
        // create query
        builder = new StringBuilder("INSERT OVERWRITE DIRECTORY ");
      } else {
        // Write to /tmp/grillreports
        resultSetPath = new
            Path(GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT, context.getQueryHandle().toString());
        builder = new StringBuilder("INSERT OVERWRITE LOCAL DIRECTORY ");
      }
      context.setResultSetPath(resultSetPath.makeQualified(
          resultSetPath.getFileSystem(context.getConf())).toString());
      builder.append('"').append(resultSetPath).append("\" ");
      String outputDirFormat = context.getConf().get(GrillConfConstants.GRILL_OUTPUT_DIRECTORY_FORMAT);
      if (outputDirFormat != null) {
        builder.append(outputDirFormat);
      }
      builder.append(' ').append(context.getDriverQuery()).append(' ');
      hiveQuery =  builder.toString();
    } else {
      hiveQuery = context.getDriverQuery();
    }
    LOG.info("Hive driver query:" + hiveQuery);
    context.setDriverQuery(hiveQuery);
  }

  private SessionHandle getSession(QueryContext ctx) throws GrillException {
    sessionLock.lock();
    try {
      String grillSession = null;
      if (SessionState.get() != null) {
        grillSession = SessionState.get().getSessionId();
      }
      SessionHandle userSession;
      if (!grillToHiveSession.containsKey(grillSession)) {
        try {
          userSession = getClient().openSession(ctx.getSubmittedUser(), "");
          grillToHiveSession.put(grillSession, userSession);
          LOG.info("New session for user: " + ctx.getSubmittedUser() + " grill session: " +
              grillSession + " session handle: " + userSession.getHandleIdentifier());
        } catch (Exception e) {
          throw new GrillException(e);
        }
      } else {
        userSession = grillToHiveSession.get(grillSession);
      }
      return userSession;
    } finally {
      sessionLock.unlock();
    }
  }

  private OperationHandle getHiveHandle(QueryHandle handle) throws GrillException {
    OperationHandle opHandle = hiveHandles.get(handle);
    if (opHandle == null) {
      throw new GrillException("Query not found " + handle); 
    }
    return opHandle;
  }

  private class QueryCompletionNotifier implements Runnable {
    long pollInterval;
    OperationHandle hiveHandle;
    long timeoutMillis;
    QueryCompletionListener listener;
    QueryHandle handle;

    QueryCompletionNotifier(QueryHandle handle, long timeoutMillis,
        QueryCompletionListener listener) throws GrillException {
      hiveHandle = getHiveHandle(handle);
      this.timeoutMillis = timeoutMillis;
      this.listener = listener;
      this.pollInterval = timeoutMillis/10;
    }

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

    private boolean isFinished(OperationHandle hiveHandle) throws GrillException {
      OperationState state;
      try {
        state = getClient().getOperationStatus(hiveHandle).getState();
      } catch (HiveSQLException e) {
        throw new GrillException("Could not get Status", e);
      }
      if (state.equals(OperationState.FINISHED) ||
          state.equals(OperationState.CANCELED) ||
          state.equals(OperationState.ERROR) ||
          state.equals(OperationState.CLOSED)) {
        return true;
      }
      return false;
    }
  }

  @Override
  public void registerForCompletionNotification(QueryHandle handle,
      long timeoutMillis, QueryCompletionListener listener)
          throws GrillException {
    Thread th = new Thread(new QueryCompletionNotifier(handle, timeoutMillis, listener));
    th.start();
  }
  
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    synchronized(hiveHandles) {
      int numHiveHnadles = in.readInt();
      for (int i = 0; i < numHiveHnadles; i++) {
        QueryHandle qhandle = (QueryHandle)in.readObject();
        OperationHandle opHandle = new OperationHandle((TOperationHandle) in.readObject());
        hiveHandles.put(qhandle, opHandle);
      }
      int numSessions = in.readInt();
      for (int i = 0; i < numSessions; i++) {
        String grillId = in.readUTF();
        SessionHandle sHandle = new SessionHandle((TSessionHandle) in.readObject(),
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
        grillToHiveSession.put(grillId, sHandle);
      }
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // Write the query handle to hive handle map to output
    synchronized (hiveHandles) {
      out.writeInt(hiveHandles.size());
      for (Map.Entry<QueryHandle, OperationHandle> entry : hiveHandles.entrySet()) {
        out.writeObject(entry.getKey());
        out.writeObject(entry.getValue().toTOperationHandle());
      }
      out.writeInt(grillToHiveSession.size());
      for (Map.Entry<String, SessionHandle> entry : grillToHiveSession.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeObject(entry.getValue().toTSessionHandle());
      }
    }
  }}
