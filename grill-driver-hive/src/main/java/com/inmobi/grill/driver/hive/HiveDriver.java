package com.inmobi.grill.driver.hive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.TaskStatus;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.TStringValue;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.inmobi.grill.api.GrillConfConstants;
import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.PreparedQueryContext;
import com.inmobi.grill.api.QueryContext;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.exception.GrillException;

public class HiveDriver implements GrillDriver {
  public static final Logger LOG = Logger.getLogger(HiveDriver.class);

  public static final String GRILL_USER_NAME_KEY = "grill.hs2.user";
  public static final String GRILL_PASSWORD_KEY = "grill.hs2.password";
  public static final String GRILL_HIVE_CONNECTION_CLASS = "grill.hive.connection.class";
  public static final String GRILL_RESULT_SET_PARENT_DIR_DEFAULT = "/tmp/grillreports";
  public static final String GRILL_ADD_INSERT_OVEWRITE = "grill.add.insert.overwrite";

  private HiveConf conf;
  private SessionHandle session;
  private Map<QueryHandle, OperationHandle> hiveHandles =
      new HashMap<QueryHandle, OperationHandle>();
  private ThriftConnection connection;
  private final Lock connectionLock;
  private final Lock sessionLock;

  public HiveDriver() throws GrillException {
    this.connectionLock = new ReentrantLock();
    this.sessionLock = new ReentrantLock();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void configure(Configuration conf) throws GrillException {
    this.conf = new HiveConf(conf, HiveDriver.class);
  }

  @Override
  public QueryPlan explain(final String query, final Configuration conf)
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
      explainOutput.add(((TStringValue) inMemoryResultSet.next().get(0)).getValue());
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
  public QueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws GrillException {
    QueryPlan plan = explain(pContext.getDriverQuery(), pContext.getConf());
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
      OperationHandle op = getClient().executeStatement(getSession(), ctx.getDriverQuery(), 
          ctx.getConf().getValByRegex(".*"));
      hiveHandles.put(ctx.getQueryHandle(), op);
      OperationStatus status = getClient().getOperationStatus(op);

      if (status.getState() == OperationState.ERROR) {
        throw new GrillException("Unknown error while running query " + ctx.getUserQuery());
      }
      return createResultSet(ctx);
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
      OperationHandle op = getClient().executeStatementAsync(getSession(),
          ctx.getDriverQuery(), 
          ctx.getConf().getValByRegex(".*"));
      hiveHandles.put(ctx.getQueryHandle(), op);
    } catch (HiveSQLException e) {
      throw new GrillException("Error executing async query", e);
    }
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle)  throws GrillException {
    LOG.info("GetStatus: " + handle);
    OperationHandle hiveHandle = getHiveHandle(handle);
    ByteArrayInputStream in = null;
    try {
      // Get operation status from hive server
      OperationStatus opStatus = getClient().getOperationStatus(hiveHandle);
      QueryStatus.Status stat = null;

      switch (opStatus.getState()) {
      case CANCELED:
        stat = Status.CANCELED;
        break;
      case CLOSED:
        stat = Status.CLOSED;
        break;
      case ERROR:
        stat = Status.FAILED;
        break;
      case FINISHED:
        stat = Status.SUCCESSFUL;
        break;
      case INITIALIZED:
        stat = Status.RUNNING;
        break;
      case RUNNING:
        stat = Status.RUNNING;
        break;
      case PENDING:
        stat = Status.LAUNCHED;
        break;
      case UNKNOWN:
        stat = Status.UNKNOWN;
        break;
      }

      float progress = 0f;
      String jsonTaskStatus = opStatus.getTaskStatus();
      String msg = "";
      if (StringUtils.isNotBlank(jsonTaskStatus)) {
        ObjectMapper mapper = new ObjectMapper();
        in = new ByteArrayInputStream(jsonTaskStatus.getBytes("UTF-8"));
        List<TaskStatus> taskStatuses = 
            mapper.readValue(in, new TypeReference<List<TaskStatus>>() {});
        int completedTasks = 0;
        StringBuilder message = new StringBuilder();
        for (TaskStatus taskStat : taskStatuses) {
          String state = taskStat.getTaskState();
          if ("FINISHED_STATE".equalsIgnoreCase(state)) {
            completedTasks++;
          }
          message.append(taskStat.getExternalHandle()).append(":").append(state).append(" ");
        }
        progress = taskStatuses.size() == 0 ? 0 : (float)completedTasks/taskStatuses.size();
        msg = message.toString();
      } else {
        LOG.warn("Empty task statuses");
      }
      return new QueryStatus(progress, stat, msg, false);
    } catch (Exception e) {
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
        LOG.warn("Could not close query" +  query, exc);
      }
    }

    try {
      getClient().closeSession(getSession());
    } catch (Exception e) {
      LOG.error("Unable to close connection", e);
    }
  }

  protected ThriftCLIServiceClient getClient() throws GrillException {
    connectionLock.lock();
    try {
      if (connection == null) {
        Class<? extends ThriftConnection> clazz = conf.getClass(
            GRILL_HIVE_CONNECTION_CLASS, 
            EmbeddedThriftConnection.class, 
            ThriftConnection.class);
        try {
          this.connection = clazz.newInstance();
          LOG.info("New thrift connection " + clazz.getName());
        } catch (Exception e) {
          throw new GrillException(e);
        }
      }
    } finally {
      connectionLock.unlock();
    }
    return connection.getClient(conf);
  }

  private GrillResultSet createResultSet(QueryContext context)
      throws GrillException {
    if (context.isPersistent()) {
      return new HivePersistentResultSet(new Path(context.getResultSetPath()),
          hiveHandles.get(context.getQueryHandle()), getClient(), context.getQueryHandle());
    } else {
      return new HiveInMemoryResultSet(
          hiveHandles.get(context.getQueryHandle()), getClient());
    }
  }

  void addPersistentPath(QueryContext context) {
    String hiveQuery;
    if (context.isPersistent() &&
        context.getConf().getBoolean(GRILL_ADD_INSERT_OVEWRITE, true)) {
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
            Path(GRILL_RESULT_SET_PARENT_DIR_DEFAULT, context.getQueryHandle().toString());
        builder = new StringBuilder("INSERT OVERWRITE LOCAL DIRECTORY ");
      }
      context.setResultSetPath(resultSetPath.toString());
      builder.append('"').append(resultSetPath).append('"')
      .append(' ').append(context.getDriverQuery()).append(' ');
      hiveQuery =  builder.toString();
    } else {
      hiveQuery = context.getDriverQuery();
    }
    context.setDriverQuery(hiveQuery);
  }

  private SessionHandle getSession() throws GrillException {
    sessionLock.lock();
    try {
      if (session == null) {
        try {
          String userName = conf.getUser();
          session = getClient().openSession(userName, "");
          LOG.info("New session: " + session.getSessionId());
        } catch (Exception e) {
          throw new GrillException(e);
        }
      }
    } finally {
      sessionLock.unlock();
    }
    return session;
  }

  private OperationHandle getHiveHandle(QueryHandle handle) throws GrillException {
    OperationHandle opHandle = hiveHandles.get(handle);
    if (opHandle == null) {
      throw new GrillException("Query not found " + handle); 
    }
    return opHandle;
  }
}
