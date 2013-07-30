package con.inmobi.grill.driver.hive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.TaskStatus;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TMemoryBuffer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

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
	public static final String GRILL_RESULT_SET_TYPE_KEY = "grill.result.type";
	private static final String PERSISTENT = "persistent";
	private static final String GRILL_RESULT_SET_PARENT_DIR = "grill.result.parent.dir";
	public static final String GRILL_HIVE_CONNECTION_CLASS = "grill.hive.connection.class";
	
	private HiveConf conf;
	private SessionHandle session;
	private Map<QueryHandle, QueryContext> handleToContext;
	private ThriftConnection connection;
	
	/**
	 * Internal class to hold query related info
	 */
	private class QueryContext {
		final QueryHandle queryHandle;
		OperationHandle hiveHandle;
		String userQuery;
		String hiveQuery;
		Path resultSetPath;
		boolean isPersistent;
		
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
	public QueryPlan explain(String query, Configuration conf) throws GrillException {
		QueryContext ctx = createQueryContext(query, conf);
		
		TMemoryBuffer tmb = null;
		try {
			String planJson = 
					getClient().getQueryPlan(getSession(), ctx.hiveQuery, conf.getValByRegex(".*"));
	    return new HiveQueryPlan(planJson);
		} catch (HiveSQLException e) {
			throw new GrillException("Error getting explain on query " + ctx.userQuery, e);
		} finally {
			if (tmb != null) {
				tmb.close();
			}
		}
	}

	@Override
	public GrillResultSet execute(String query, Configuration conf) throws GrillException {
		try {
			// Get eventual Hive query based on conf
			QueryContext ctx = createQueryContext(query, conf);
			
			OperationHandle op = getClient().executeStatement(getSession(), ctx.hiveQuery, 
					conf.getValByRegex(".*"));
			ctx.hiveHandle = op;
			OperationStatus status = getClient().getOperationStatus(op);
			handleToContext.put(ctx.queryHandle, ctx);
			
			if (status.getState() == OperationState.ERROR) {
				throw new GrillException("Unknown error while running query " + query);
			}
			
			return createResultSet(ctx);
		} catch (HiveSQLException hiveErr) {
			throw new GrillException("Error executing query" , hiveErr);
		}
	}

	@Override
	public QueryHandle executeAsync(String query, Configuration conf) throws GrillException {
		try{
			QueryContext ctx = createQueryContext(query, conf);
			
			ctx.hiveHandle = getClient().executeStatementAsync(getSession(), ctx.hiveQuery, 
					conf.getValByRegex(".*"));
			handleToContext.put(ctx.queryHandle, ctx);
			return ctx.queryHandle;
		} catch (HiveSQLException hiveErr) {
			throw new GrillException("Error executing async query", hiveErr);
		}
	}
	
	@Override
	public QueryStatus getStatus(QueryHandle handle)  throws GrillException {
		QueryContext ctx = handleToContext.get(handle);
		
		if (ctx == null) {
			throw new GrillException("Could not find query " + ctx);
		}
		
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
			case UNKNOWN:
				stat = Status.UNKNOWN;
				break;
			}
			
			float progress = 0f;
			String jsonTaskStatus = opStatus.getTaskStatus();
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
			return new QueryStatus(progress, stat, message.toString(), false);
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
		// This should be applicable only for a async query
		QueryContext ctx = handleToContext.get(handle);
		if (ctx == null) {
			throw new GrillException("Query not found " + ctx); 
		}
		return createResultSet(ctx);
	}
	
	public void closeQuery(QueryHandle handle) throws GrillException {
		QueryContext options = handleToContext.remove(handle);
		if (options != null) {
			OperationHandle opHandle = options.hiveHandle;
			try {
				getClient().closeOperation(opHandle);
			} catch (HiveSQLException e) {
				throw new GrillException("Unable to close query", e);
			}
		}
	}

	@Override
	public boolean cancelQuery(QueryHandle handle)  throws GrillException {
		QueryContext ctx = handleToContext.get(handle);
		if (ctx == null) {
			throw new GrillException("Query not found " + ctx);
		}
		
		try {
			getClient().cancelOperation(ctx.hiveHandle);
			return true;
		} catch (HiveSQLException e) {
			throw new GrillException();
		}
	}
	
	public void close() {
		// Close this driver and release all resources
		for (QueryHandle query : handleToContext.keySet()) {
			try {
				closeQuery(query);
			} catch (GrillException exc) {
				LOG.warn("Could not close query" +  query, exc);
			}
		}
		
		try {
			getClient().closeSession(session);
		} catch (Exception e) {
			LOG.error("Unable to close connection", e);
		}
	}
	
	protected ThriftCLIServiceClient getClient() throws GrillException {
		synchronized (connection) {
			if (connection == null) {
				Class<? extends ThriftConnection> clazz = conf.getClass(GRILL_HIVE_CONNECTION_CLASS, 
						EmbeddedThriftConnection.class, 
						ThriftConnection.class);
				try {
					this.connection = (ThriftConnection) clazz.newInstance();
				} catch (Exception e) {
					throw new GrillException(e);
				}
			}
			return connection.getClient(conf);
		}
	}

	private GrillResultSet createResultSet(QueryContext context) throws GrillException {
		if (context.isPersistent) {
			return new HivePersistentResultSet(context.resultSetPath, context.hiveHandle, getClient());
		} else {
			return new HiveInMemoryResultSet(context.hiveHandle, getClient());
		}
	}

	private QueryContext createQueryContext(String query, Configuration conf) {
		QueryContext ctx = new QueryContext();
		
		String resultSetType = conf.get(GRILL_RESULT_SET_TYPE_KEY, PERSISTENT);
		
		ctx.isPersistent = PERSISTENT.equalsIgnoreCase(resultSetType);
		ctx.userQuery = query;
		
		if (ctx.isPersistent) {
			// store persistent data into user specified location
			String resultSetParentDir = this.conf.get(GRILL_RESULT_SET_PARENT_DIR);
			ctx.resultSetPath = new Path(resultSetParentDir, ctx.queryHandle.toString());
			// create query
			StringBuilder builder = new StringBuilder("INSERT OVERWRITE DIRECTORY ");
			builder.append(ctx.resultSetPath).append(ctx.userQuery).append(' ');
			ctx.hiveQuery =  builder.toString();
		} else {
			ctx.hiveQuery = ctx.userQuery;
		}
		
		return ctx;
	}
	
	private SessionHandle getSession() throws GrillException {
		synchronized (session) {
			if (session == null) {
				try {
					session = getClient().openSession(getUserName(), getPassword());
				} catch (Exception e) {
					throw new GrillException(e);
				}
			}
			return session;
		}
	}
	
	private String getUserName() {
		return conf.get(GRILL_USER_NAME_KEY);
	}
	
	private String getPassword() {
		return conf.get(GRILL_PASSWORD_KEY);
	}
	
}
