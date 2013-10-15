package com.inmobi.grill.driver.hive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

import com.inmobi.grill.api.GrillConfConstatnts;
import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.exception.GrillException;

public class HiveDriver implements GrillDriver {
  public static final Logger LOG = Logger.getLogger(HiveDriver.class);

  public static final String GRILL_USER_NAME_KEY = "grill.hs2.user";
  public static final String GRILL_PASSWORD_KEY = "grill.hs2.password";
  public static final String GRILL_PERSISTENT_RESULT_SET = "grill.persistent.resultset";
  public static final String GRILL_RESULT_SET_PARENT_DIR = "grill.result.parent.dir";
  public static final String GRILL_HIVE_CONNECTION_CLASS = "grill.hive.connection.class";
  public static final String GRILL_RESULT_SET_PARENT_DIR_DEFAULT = "/tmp/grillreports";
  public static final String GRILL_ADD_INSERT_OVEWRITE = "grill.add.insert.overwrite";

  private HiveConf conf;
  private SessionHandle session;
  private Map<QueryHandle, QueryContext> handleToContext;
  private ThriftConnection connection;
  private final Lock connectionLock;
  private final Lock sessionLock;

  /**
   * Internal class to hold query related info
   */
  class QueryContext {
    final QueryHandle queryHandle;
    OperationHandle hiveHandle;
    String userQuery;
    String hiveQuery;
    Path resultSetPath;
    boolean isPersistent;
    HiveConf conf;

    public QueryContext() {
      queryHandle = new QueryHandle(UUID.randomUUID());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof QueryContext) {
        return queryHandle.equals(((QueryContext) obj).queryHandle);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return queryHandle.hashCode();
    }

    @Override
    public String toString() {
      return queryHandle + "/" + userQuery;
    }
  }

  public HiveDriver() throws GrillException {
    this.connectionLock = new ReentrantLock();
    this.sessionLock = new ReentrantLock();
    this.handleToContext = new HashMap<QueryHandle, QueryContext>();
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
  public QueryPlan explain(String query, Configuration conf)
      throws GrillException {
    QueryContext ctx = createQueryContext(query, conf);
    // Get result set of explain
    Configuration explainConf = new Configuration(conf);
    explainConf.setBoolean(GRILL_PERSISTENT_RESULT_SET, false);
    String explainQuery = "EXPLAIN EXTENDED " + ctx.hiveQuery;
    HiveInMemoryResultSet inMemoryResultSet = (HiveInMemoryResultSet) execute(
        explainQuery, explainConf);
    List<String> explainOutput = new ArrayList<String>();
    while (inMemoryResultSet.hasNext()) {
      explainOutput.add(((TStringValue) inMemoryResultSet.next().get(0)).getValue());
    }

    QueryHandle handle = null;
    if (conf.getBoolean(GrillConfConstatnts.PREPARE_ON_EXPLAIN,
        GrillConfConstatnts.DEFAULT_PREPARE_ON_EXPLAIN)) {
      handleToContext.put(ctx.queryHandle, ctx);
      handle = ctx.queryHandle;
    }
    LOG.info("Explain: " + query);
    try {
      return new HiveQueryPlan(explainOutput, handle, ctx.conf);
    } catch (HiveException e) {
      throw new GrillException("Unable to create hive query plan", e);
    }
  }

  @Override
  public GrillResultSet execute(String query, Configuration conf) throws GrillException {
    // Get eventual Hive query based on conf
    QueryContext ctx = createQueryContext(query, conf);
    LOG.info("Execute: " + query);
    return execute(ctx);
  }

  private GrillResultSet execute(QueryContext ctx) throws GrillException {
    try {
      ctx.conf.set("mapred.job.name", ctx.queryHandle.toString());
      OperationHandle op = getClient().executeStatement(getSession(), ctx.hiveQuery, 
          ctx.conf.getValByRegex(".*"));
      ctx.hiveHandle = op;
      OperationStatus status = getClient().getOperationStatus(op);

      if (status.getState() == OperationState.ERROR) {
        throw new GrillException("Unknown error while running query " + ctx.userQuery);
      }
      return createResultSet(ctx);
    } catch (HiveSQLException hiveErr) {
      throw new GrillException("Error executing query" , hiveErr);
    }

  }
  @Override
  public QueryHandle executeAsync(String query, Configuration conf) throws GrillException {
    LOG.info("ExecuteAsync: " + query);
    QueryContext ctx = createQueryContext(query, conf);
    handleToContext.put(ctx.queryHandle, ctx);
    return executeAsync(ctx);
  }

  private QueryHandle executeAsync(QueryContext ctx)
      throws GrillException {
    try {
      ctx.conf.set("mapred.job.name", ctx.queryHandle.toString());
      ctx.hiveHandle = getClient().executeStatementAsync(getSession(), ctx.hiveQuery, 
          ctx.conf.getValByRegex(".*"));
    } catch (HiveSQLException e) {
      throw new GrillException("Error executing async query", e);
    }
    return ctx.queryHandle;
  }

  private void copyConf(QueryContext ctx, Configuration conf) {
    for (Map.Entry<String, String> entry : conf) {
      ctx.conf.set(entry.getKey(), entry.getValue());
    }
  }
  @Override
  public GrillResultSet executePrepare(QueryHandle handle, Configuration conf)
      throws GrillException {
    LOG.info("ExecutePrepared: " + handle);
    QueryContext ctx = getContext(handle);
    copyConf(ctx, conf);
    return execute(ctx);
  }

  public void executePrepareAsync(QueryHandle handle, Configuration conf)
      throws GrillException {
    QueryContext ctx = getContext(handle);
    copyConf(ctx, conf);
    executeAsync(ctx);
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle)  throws GrillException {
    LOG.info("GetStatus: " + handle);
    QueryContext ctx = getContext(handle);
    ByteArrayInputStream in = null;
    try {
      // Get operation status from hive server
      OperationStatus opStatus = getClient().getOperationStatus(ctx.hiveHandle);
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
        stat = Status.PENDING;
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
  public GrillResultSet fetchResultSet(QueryHandle handle)  throws GrillException {
    LOG.info("FetchResultSet: " + handle);
    // This should be applicable only for a async query
    QueryContext ctx = getContext(handle);
    return createResultSet(ctx);
  }

  @Override
  public void closeQuery(QueryHandle handle) throws GrillException {
    LOG.info("CloseQuery: " + handle);
    QueryContext options = handleToContext.remove(handle);
    if (options != null) {
      OperationHandle opHandle = options.hiveHandle;
      if (opHandle != null) {
        try {
          getClient().closeOperation(opHandle);
        } catch (HiveSQLException e) {
          throw new GrillException("Unable to close query", e);
        }
      }
    }
  }

  @Override
  public boolean cancelQuery(QueryHandle handle)  throws GrillException {
    LOG.info("CancelQuery: " + handle);
    QueryContext ctx = getContext(handle);
    try {
      getClient().cancelOperation(ctx.hiveHandle);
      return true;
    } catch (HiveSQLException e) {
      throw new GrillException();
    }
  }

  @Override
  public void close() {
    LOG.info("CloseDriver");
    // Close this driver and release all resources
    for (QueryHandle query : new ArrayList<QueryHandle>(handleToContext.keySet())) {
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
    if (context.isPersistent) {
      return new HivePersistentResultSet(context.resultSetPath,
          context.hiveHandle, getClient(), context.queryHandle);
    } else {
      return new HiveInMemoryResultSet(context.hiveHandle, getClient());
    }
  }

  QueryContext createQueryContext(String query, Configuration conf) {
    QueryContext ctx = new QueryContext();
    ctx.conf = new HiveConf(conf, HiveDriver.class);
    ctx.isPersistent = conf.getBoolean(GRILL_PERSISTENT_RESULT_SET, true);
    ctx.userQuery = query;

    if (ctx.isPersistent && conf.getBoolean(GRILL_ADD_INSERT_OVEWRITE, true)) {
      // store persistent data into user specified location
      // If absent, take default home directory
      String resultSetParentDir = conf.get(GRILL_RESULT_SET_PARENT_DIR);
      StringBuilder builder;
      if (StringUtils.isNotBlank(resultSetParentDir)) {
        ctx.resultSetPath = new Path(resultSetParentDir, ctx.queryHandle.toString());
        // create query
        builder = new StringBuilder("INSERT OVERWRITE DIRECTORY ");
      } else {
        // Write to /tmp/grillreports
        ctx.resultSetPath = new 
            Path(GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ctx.queryHandle.toString());
        builder = new StringBuilder("INSERT OVERWRITE LOCAL DIRECTORY ");
      }
      builder.append('"').append(ctx.resultSetPath).append('"')
      .append(' ').append(ctx.userQuery).append(' ');
      ctx.hiveQuery =  builder.toString();
    } else {
      ctx.hiveQuery = ctx.userQuery;
    }

    return ctx;
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

  private QueryContext getContext(QueryHandle handle) throws GrillException {
    QueryContext ctx = handleToContext.get(handle);
    if (ctx == null) {
      throw new GrillException("Query not found " + ctx); 
    }
    return ctx;
  }
}
