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

import java.io.IOException;
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
  public static class EmbeddedThriftCLIServiceClient extends ThriftCLIServiceClient {
    private SessionState state;
    private void captureState() {
      state = SessionState.get();
    }
    private void restoreState() {
      if(state != null) {
        SessionState.setCurrentSessionState(state);
      }
    }
    public EmbeddedThriftCLIServiceClient(TCLIService.Iface cliService) {
      super(cliService);
    }

    public SessionHandle openSession(String username, String password,
      Map<String, String> configuration)
      throws HiveSQLException {
      try{
        captureState();
        return super.openSession(username, password, configuration);
      } finally {
        restoreState();
      }
    }

    public SessionHandle openSessionWithImpersonation(String username, String password,
      Map<String, String> configuration, String delegationToken)
      throws HiveSQLException {
      try{
        captureState();
        return super.openSessionWithImpersonation(username, password, configuration, delegationToken);
      } finally {
        restoreState();
      }
    }

    public void closeSession(SessionHandle sessionHandle)
      throws HiveSQLException {
      try{
        captureState();
        super.closeSession(sessionHandle);
      } finally {
        restoreState();
      }
    }

    public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType infoType)
      throws HiveSQLException {
      try{
        captureState();
        return super.getInfo(sessionHandle, infoType);
      } finally {
        restoreState();
      }
    }

    public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay)
      throws HiveSQLException {
      try{
        captureState();
        return super.executeStatement(sessionHandle, statement, confOverlay);
      } finally {
        restoreState();
      }
    }

    public OperationHandle executeStatementAsync(SessionHandle sessionHandle,
      String statement, Map<String, String> confOverlay)
      throws HiveSQLException {
      try{
        captureState();
        return super.executeStatementAsync(sessionHandle, statement, confOverlay);
      } finally {
        restoreState();
      }
    }

    public OperationHandle getTypeInfo(SessionHandle sessionHandle)
      throws HiveSQLException {
      try{
        captureState();
        return super.getTypeInfo(sessionHandle);
      } finally {
        restoreState();
      }
    }

    public OperationHandle getCatalogs(SessionHandle sessionHandle)
      throws HiveSQLException {
      try{
        captureState();
        return super.getCatalogs(sessionHandle);
      } finally {
        restoreState();
      }
    }

    public OperationHandle getSchemas(SessionHandle sessionHandle,
      String catalogName, String schemaName)
      throws HiveSQLException {
      try{
        captureState();
        return super.getSchemas(sessionHandle, catalogName, schemaName);
      } finally {
        restoreState();
      }
    }

    public OperationHandle getTables(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, List<String> tableTypes)
      throws HiveSQLException {
      try{
        captureState();
        return super.getTables(sessionHandle, catalogName, schemaName, tableName, tableTypes);
      } finally {
        restoreState();
      }
    }

    public OperationHandle getTableTypes(SessionHandle sessionHandle)
      throws HiveSQLException {
      try{
        captureState();
        return super.getTableTypes(sessionHandle);
      } finally {
        restoreState();
      }
    }

    public OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
      throws HiveSQLException {
      try{
        captureState();
        return super.getColumns(sessionHandle, catalogName, schemaName, tableName, columnName);
      } finally {
        restoreState();
      }
    }

    public OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName)
      throws HiveSQLException {
      try{
        captureState();
        return super.getFunctions(sessionHandle, catalogName, schemaName, functionName);
      } finally {
        restoreState();
      }
    }

    public OperationStatus getOperationStatus(OperationHandle opHandle)
      throws HiveSQLException {
      try{
        captureState();
        return super.getOperationStatus(opHandle);
      } finally {
        restoreState();
      }
    }

    public void cancelOperation(OperationHandle opHandle)
      throws HiveSQLException {
      try{
        captureState();
        super.cancelOperation(opHandle);
      } finally {
        restoreState();
      }
    }

    public void closeOperation(OperationHandle opHandle)
      throws HiveSQLException {
      try{
        captureState();
        super.closeOperation(opHandle);
      } finally {
        restoreState();
      }
    }

    public TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws HiveSQLException {
      try{
        captureState();
        return super.getResultSetMetadata(opHandle);
      } finally {
        restoreState();
      }
    }

    public RowSet fetchResults(OperationHandle opHandle)
      throws HiveSQLException {
      try{
        captureState();
        return super.fetchResults(opHandle);
      } finally {
        restoreState();
      }
    }

    public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws HiveSQLException {
      try{
        captureState();
        return super.fetchResults(opHandle, orientation, maxRows, fetchType);
      } finally {
        restoreState();
      }
    }

    public String getDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String owner, String renewer) throws HiveSQLException {
      try{
        captureState();
        return super.getDelegationToken(sessionHandle, authFactory, owner, renewer);
      } finally {
        restoreState();
      }
    }

    public void cancelDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException {
      try{
        captureState();
        super.cancelDelegationToken(sessionHandle, authFactory, tokenStr);
      } finally {
        restoreState();
      }
    }

    public void renewDelegationToken(SessionHandle sessionHandle, HiveAuthFactory authFactory,
      String tokenStr) throws HiveSQLException {
      try{
        captureState();
        super.renewDelegationToken(sessionHandle, authFactory, tokenStr);
      } finally {
        restoreState();
      }
    }

    @Override
    public SessionHandle openSession(String username, String password) throws HiveSQLException {
      try{
        captureState();
        return super.openSession(username, password);
      } finally {
        restoreState();
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
  public void close() throws IOException {
    // Does nothing
  }

  @Override
  public void init(HiveConf conf, String user) {
    SessionState state = null;
    try {
      state = SessionState.get();
      getService().init(conf);
    } finally {
      if (state != null) {
        SessionState.setCurrentSessionState(state);
      }
    }
  }
}
