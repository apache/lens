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

import java.util.List;
import java.util.Map;

import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.rpc.thrift.TCLIService;

/**
 * The Class EmbeddedThriftConnection.
 */
public class EmbeddedThriftConnection implements ThriftConnection {
  public static class SessionStateContext implements AutoCloseable {
    /**
     * This is needed because we're using embedded mode. In opening a hive session, a new session state is started
     * and previous session state is lost, since it's all happening in the same jvm.
     * For all other session operations (getting status, getting result etc),
     * they are wrapped in acquire-release block in HiveSessionImpl,
     * and the release clears session state for the current thread.
     * Since it's happening in a single thread, the session is cleared for further operations too
     * and needs to be restored for tests to proceed further.
     */
    private SessionState state = SessionState.get();

    @Override
    public void close() {
      if (state != null && !state.equals(SessionState.get())) {
        SessionState.setCurrentSessionState(state);
      }
    }
  }
  //SUSPEND CHECKSTYLE CHECK InnerAssignmentCheck
  public static class EmbeddedThriftCLIServiceClient extends ThriftCLIServiceClient {

    public EmbeddedThriftCLIServiceClient(TCLIService.Iface cliService) {
      super(cliService);
    }

    public SessionHandle openSession(String username, String password,
      Map<String, String> configuration) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.openSession(username, password, configuration);
      }
    }

    public SessionHandle openSessionWithImpersonation(String username, String password,
      Map<String, String> configuration, String delegationToken) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.openSessionWithImpersonation(username, password, configuration, delegationToken);
      }
    }

    public void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        super.closeSession(sessionHandle);
      }
    }

    public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType infoType) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.getInfo(sessionHandle, infoType);
      }
    }

    public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.executeStatement(sessionHandle, statement, confOverlay);
      }
    }

    public OperationHandle executeStatementAsync(SessionHandle sessionHandle,
      String statement, Map<String, String> confOverlay) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.executeStatementAsync(sessionHandle, statement, confOverlay);
      }
    }

    public OperationHandle getTypeInfo(SessionHandle sessionHandle) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.getTypeInfo(sessionHandle);
      }
    }

    public OperationHandle getCatalogs(SessionHandle sessionHandle) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.getCatalogs(sessionHandle);
      }
    }

    public OperationHandle getSchemas(SessionHandle sessionHandle,
      String catalogName, String schemaName) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.getSchemas(sessionHandle, catalogName, schemaName);
      }
    }

    public OperationHandle getTables(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, List<String> tableTypes) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.getTables(sessionHandle, catalogName, schemaName, tableName, tableTypes);
      }
    }

    public OperationHandle getTableTypes(SessionHandle sessionHandle) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.getTableTypes(sessionHandle);
      }
    }

    public OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.getColumns(sessionHandle, catalogName, schemaName, tableName, columnName);
      }
    }

    public OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.getFunctions(sessionHandle, catalogName, schemaName, functionName);
      }
    }

    public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        super.cancelOperation(opHandle);
      }
    }

    public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        super.closeOperation(opHandle);
      }
    }

    public TableSchema getResultSetMetadata(OperationHandle opHandle) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.getResultSetMetadata(opHandle);
      }
    }

    public RowSet fetchResults(OperationHandle opHandle) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.fetchResults(opHandle);
      }
    }

    public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.fetchResults(opHandle, orientation, maxRows, fetchType);
      }
    }

    public String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String owner, String renewer) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.getDelegationToken(sessionHandle, authFactory, owner, renewer);
      }
    }

    public void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        super.cancelDelegationToken(sessionHandle, authFactory, tokenStr);
      }
    }

    public void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        super.renewDelegationToken(sessionHandle, authFactory, tokenStr);
      }
    }

    @Override
    public SessionHandle openSession(String username, String password) throws HiveSQLException {
      try (SessionStateContext ignored = new SessionStateContext()) {
        return super.openSession(username, password);
      }
    }
  }

  /** The client. */
  private EmbeddedThriftCLIServiceClient client;

  /** The connected. */
  private EmbeddedThriftBinaryCLIService service;

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.driver.hive.ThriftConnection#getClient(org.apache.hadoop.hive.conf.HiveConf)
   */
  @Override
  public ThriftCLIServiceClient getClient() throws LensException {
    if (client == null) {
      client = new EmbeddedThriftCLIServiceClient(getService());
    }
    return client;
  }

  private EmbeddedThriftBinaryCLIService getService() {
    if (service == null) {
      service = new EmbeddedThriftBinaryCLIService();
    }
    return service;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() {
    // Does nothing
  }

  @Override
  public void init(HiveConf conf, String user) {
    try (SessionStateContext ignored = new SessionStateContext()) {
      getService().init(conf);
    }
  }
  //RESUME CHECKSTYLE CHECK InnerAssignmentCheck
}
