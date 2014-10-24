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
package org.apache.lens.client;

import com.google.common.collect.Maps;
import org.apache.lens.api.metastore.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lens.api.APIResult;
import org.apache.lens.api.query.*;

import java.util.HashMap;
import java.util.List;


public class LensClient {
  private static final Log LOG = LogFactory.getLog(LensClient.class);
  private static final String DEFAULT_PASSWORD = "";
  private final LensClientConfig conf;
  private final LensMetadataClient mc;
  private String password;
  private LensConnection conn;
  private final HashMap<QueryHandle, LensStatement> statementMap =
      Maps.newHashMap();
  private final LensStatement statement;

  public LensClient() {
    this(new LensClientConfig());
  }

  public LensClient(LensClientConfig conf) {
    this(conf, conf.getUser(), DEFAULT_PASSWORD);
  }

  public LensClient(String username, String password) {
    this(new LensClientConfig(), username, password);
  }

  public LensClient(LensClientConfig conf, String username, String password) {
    this.conf = conf;
    conf.setUser(username);
    this.password = password;
    if(this.conf.get(LensClientConfig.SESSION_CLUSTER_USER) == null) {
      this.conf.set(LensClientConfig.SESSION_CLUSTER_USER, System.getProperty("user.name"));
    }
    connectToLensServer();
    mc = new LensMetadataClient(conn);
    statement = new LensStatement(conn);
  }

  public LensClient(Credentials cred) {
    this(cred.getUsername(), cred.getPassword());
  }

  public QueryHandle executeQueryAsynch(String sql, String queryName) {
    LOG.debug("Executing query " + sql);
    statement.execute(sql, false, queryName);
    LensQuery query = statement.getQuery();
    LOG.debug("Adding query to statementMap " + query.getQueryHandle());
    statementMap.put(query.getQueryHandle(), statement);
    return query.getQueryHandle();
  }

  public LensConnection getConnection() {
    return conn;
  }

  public static class LensClientResultSetWithStats {
    private final LensClientResultSet resultSet;
    private final LensQuery query;

    public LensClientResultSetWithStats(LensClientResultSet resultSet,
        LensQuery query) {
      this.resultSet = resultSet;
      this.query = query;
    }

    public LensClientResultSet getResultSet() {
      return resultSet;
    }

    public LensQuery getQuery() {
      return query;
    }
  }

  public LensClientResultSetWithStats getResults(String sql, String queryName) {
    LOG.debug("Executing query " + sql);
    statement.execute(sql, true, queryName);
    return getResultsFromStatement(statement);
  }

  private LensClientResultSetWithStats getResultsFromStatement(LensStatement statement) {
    QueryStatus.Status status = statement.getStatus().getStatus();
    if(status != QueryStatus.Status.SUCCESSFUL) {
      throw new IllegalStateException(statement.getStatus().getStatusMessage()
          + " cause:" + statement.getStatus().getErrorMessage());
    }
    LensClientResultSet result = null;
    if (statement.getStatus().isResultSetAvailable()) {
      result = new LensClientResultSet(statement.getResultSet(),
          statement.getResultSetMetaData());
    }
    return new LensClientResultSetWithStats(result, statement.getQuery());
  }

  private LensClientResultSetWithStats getResultsFromHandle(QueryHandle q) {
    LensQuery query = statement.getQuery(q);
    if (query.getStatus().getStatus()
        == QueryStatus.Status.FAILED) {
      throw new IllegalStateException(query.getStatus().getErrorMessage());
    }
    LensClientResultSet result = null;
    if (statement.getStatus().isResultSetAvailable()) {
      result = new LensClientResultSet(statement.getResultSet(),
          statement.getResultSetMetaData());
    }
    return new LensClientResultSetWithStats(result, statement.getQuery());
  }

  public LensClientResultSetWithStats getAsyncResults(QueryHandle q) {
    return getResultsFromHandle(q);
  }

  public LensStatement getLensStatement(QueryHandle query) {
    return this.statementMap.get(query);
  }

  public QueryStatus getQueryStatus(QueryHandle query) {
    return new LensStatement(conn).getQuery(query).getStatus();
  }

  public QueryStatus getQueryStatus(String q) {
    return getQueryStatus(QueryHandle.fromString(q));
  }

  public QueryPlan getQueryPlan(String q) {
    return new LensStatement(conn).explainQuery(q);
  }

  public boolean killQuery(QueryHandle q) {
    return statement.kill(statement.getQuery(q));
  }


  public QueryResult getResults(QueryHandle query) {
    QueryStatus status = getLensStatement(query).getStatus();
    if (!status.isResultSetAvailable()) {
      LOG.debug("Current status of the query is " + status);
      throw new IllegalStateException("Resultset for the query "
          + query + " is not available, its current status is " + status);
    }
    return getLensStatement(query).getResultSet();
  }

  public List<QueryHandle> getQueries(String state, String queryName, String user, long fromDate, long toDate) {
    return new LensStatement(conn).getAllQueries(state, queryName, user, fromDate, toDate);
  }


  private void connectToLensServer() {
    LOG.debug("Connecting to lens server " + new LensConnectionParams(conf));
    conn = new LensConnection(new LensConnectionParams(conf));
    conn.open(password);
    LOG.debug("Successfully connected to server " + conn);
  }


  public List<String> getAllDatabases() {
    LOG.debug("Getting all database");
    return mc.getAlldatabases();
  }

  public List<String> getAllNativeTables() {
    LOG.debug("Getting all native tables");
    return mc.getAllNativeTables();
  }

  public List<String> getAllFactTables() {
    LOG.debug("Getting all fact table");
    return mc.getAllFactTables();
  }


  public List<String> getAllDimensionTables() {
    LOG.debug("Getting all dimension table");
    return mc.getAllDimensionTables();
  }

  public List<String> getAllCubes() {
    LOG.debug("Getting all cubes in database");
    return mc.getAllCubes();
  }

  public List<String> getAllDimensions() {
    LOG.debug("Getting all dimensions in database");
    return mc.getAllDimensions();
  }

  public String getCurrentDatabae() {
    LOG.debug("Getting current database");
    return mc.getCurrentDatabase();
  }


  public boolean setDatabase(String database) {
    LOG.debug("Set the database to " + database);
    APIResult result = mc.setDatabase(database);
    return result.getStatus() == APIResult.Status.SUCCEEDED;
  }

  public APIResult dropDatabase(String database) {
    LOG.debug("Dropping database " + database);
    APIResult result = mc.dropDatabase(database);
    LOG.debug("Return status of dropping " + database + " result " + result);
    return result;
  }

  public APIResult createDatabase(String database, boolean ignoreIfExists) {
    LOG.debug("Creating database " + database + " ignore " + ignoreIfExists);
    APIResult result = mc.createDatabase(database, ignoreIfExists);
    LOG.debug("Create database result " + result);
    return result;
  }

  public APIResult setConnectionParam(String key, String val) {
    return this.conn.setConnectionParams(key, val);
  }

  public List<String> getConnectionParam() {
    return this.conn.getConnectionParams();
  }

  public List<String> getConnectionParam(String key) {
    return this.conn.getConnectionParams(key);
  }

  public APIResult closeConnection() {
    LOG.debug("Closing lens connection: " + new LensConnectionParams(conf));
    return this.conn.close();
  }

  public APIResult addJarResource(String path) {
    return this.conn.addResourceToConnection("jar", path);
  }

  public APIResult removeJarResource(String path) {
    return this.conn.removeResourceFromConnection("jar", path);
  }

  public APIResult addFileResource(String path) {
    return this.conn.addResourceToConnection("file", path);
  }

  public APIResult removeFileResource(String path) {
    return this.conn.removeResourceFromConnection("file", path);
  }

  public APIResult createFactTable(String factSpec,
      String storageSpecPath) {
    return mc.createFactTable(factSpec, storageSpecPath);
  }

  public APIResult createCube(String cubeSpec) {
    return mc.createCube(cubeSpec);
  }

  public APIResult createStorage(String storageSpec) {
    return mc.createNewStorage(storageSpec);
  }

  public APIResult createDimension(String dimSpec) {
    return mc.createDimension(dimSpec);
  }

  public APIResult createDimensionTable(String dimSpec, String storageSpec) {
    return mc.createDimensionTable(dimSpec, storageSpec);
  }

  public List<String> getAllStorages() {
    return mc.getAllStorages();
  }

  public APIResult dropDimensionTable(String dim, boolean cascade) {
    return mc.dropDimensionTable(dim, cascade);
  }

  public APIResult dropFactTable(String fact, boolean cascade) {
    return mc.dropFactTable(fact, cascade);
  }

  public APIResult dropCube(String cube) {
    return mc.dropCube(cube);
  }

  public APIResult dropStorage(String storage) {
    return mc.dropStorage(storage);
  }

  public APIResult dropDimension(String dimName) {
    return mc.dropDimension(dimName);
  }

  public APIResult updateFactTable(String factName, String factSpec) {
    return mc.updateFactTable(factName, factSpec);
  }

  public APIResult updateDimensionTable(String dimName, String dimSpec) {
    return mc.updateDimensionTable(dimName, dimSpec);
  }

  public APIResult updateCube(String cubeName, String cubeSpec) {
    return mc.updateCube(cubeName, cubeSpec);
  }

  public APIResult updateStorage(String storageName, String storageSpec) {
    return mc.updateStorage(storageName, storageSpec);
  }

  public APIResult updateDimension(String dimName, String dimSpec) {
    return mc.updateDimension(dimName, dimSpec);
  }

  public FactTable getFactTable(String factName) {
    return mc.getFactTable(factName);
  }

  public DimensionTable getDimensionTable(String dimName) {
    return mc.getDimensionTable(dimName);
  }

  public NativeTable getNativeTable(String tblName) {
    return mc.getNativeTable(tblName);
  }

  public XCube getCube(String cubeName) {
    return mc.getCube(cubeName);
  }

  public XDimension getDimension(String dimName) {
    return mc.getDimension(dimName);
  }

  public XStorage getStorage(String storageName) {
    return mc.getStorage(storageName);
  }

  public List<String> getFactStorages(String fact) {
    return mc.getAllStoragesOfFactTable(fact);
  }

  public List<String> getDimStorages(String dim) {
    return mc.getAllStoragesOfDimTable(dim);
  }

  public APIResult dropAllStoragesOfDim(String table) {
    return mc.dropAllStoragesOfDimension(table);
  }

  public APIResult dropAllStoragesOfFact(String table) {
    return mc.dropAllStoragesOfFactTable(table);
  }

  public APIResult addStorageToFact(String factName, String spec) {
    return mc.addStorageToFactTable(factName, spec);
  }

  public APIResult dropStorageFromFact(String factName, String storage) {
    return mc.dropStorageFromFactTable(factName, storage);
  }

  public XStorageTableElement getStorageFromFact(String fact, String storage) {
    return mc.getStorageOfFactTable(fact, storage);
  }

  public APIResult addStorageToDim(String dim, String storage) {
    return mc.addStorageToDimTable(dim, storage);
  }

  public APIResult dropStorageFromDim(String dim, String storage) {
    return mc.dropStoragesOfDimensionTable(dim, storage);
  }

  public XStorageTableElement getStorageFromDim(String dim, String storage) {
    return mc.getStorageOfDimensionTable(dim, storage);
  }

  public List<XPartition> getAllPartitionsOfFact(String fact, String storage) {
    return mc.getPartitionsOfFactTable(fact, storage);
  }

  public List<XPartition> getAllPartitionsOfFact(String fact, String storage, String list) {
    return mc.getPartitionsOfFactTable(fact, storage, list);
  }

  public List<XPartition> getAllPartitionsOfDim(String dim, String storage) {
    return mc.getAllPartitionsOfDimensionTable(dim, storage);
  }

  public List<XPartition> getAllPartitionsOfDim(String dim, String storage, String list) {
    return mc.getAllPartitionsOfDimensionTable(dim, storage);
  }

  public APIResult dropAllPartitionsOfFact(String fact, String storage) {
    return mc.dropPartitionsOfFactTable(fact, storage);
  }

  public APIResult dropAllPartitionsOfFact(String fact, String storage, String list) {
    return mc.dropPartitionsOfFactTable(fact, storage, list);
  }

  public APIResult dropAllPartitionsOfDim(String dim, String storage) {
    return mc.dropAllPartitionsOfDimensionTable(dim, storage);
  }

  public APIResult dropAllPartitionsOfDim(String dim, String storage, String list) {
    return mc.dropAllPartitionsOfDimensionTable(dim, storage, list);
  }

  public APIResult addPartitionToFact(String table, String storage, String partSpec) {
    return mc.addPartitionToFactTable(table, storage, partSpec);
  }

  public APIResult addPartitionToDim(String table, String storage, String partSpec) {
    return mc.addPartitionToDimensionTable(table, storage, partSpec);
  }

  public QueryPrepareHandle prepare(String sql, String queryName) {
    return statement.prepareQuery(sql, queryName);
  }

  public QueryPlan explainAndPrepare(String sql, String queryName) {
    return statement.explainAndPrepare(sql, queryName);
  }

  public boolean destroyPrepared(QueryPrepareHandle queryPrepareHandle) {
    return statement.destroyPrepared(queryPrepareHandle);
  }

  public List<QueryPrepareHandle> getPreparedQueries(String userName, String queryName, long fromDate, long toDate) {
    return statement.getAllPreparedQueries(userName, queryName, fromDate, toDate);
  }

  public LensPreparedQuery getPreparedQuery(QueryPrepareHandle phandle) {
    return statement.getPreparedQuery(phandle);
  }

  public LensClientResultSetWithStats getResultsFromPrepared(QueryPrepareHandle phandle, String queryName) {
    QueryHandle qh = statement.executeQuery(phandle, true, queryName);
    return getResultsFromHandle(qh);
  }

  public QueryHandle executePrepared(QueryPrepareHandle phandle, String queryName) {
    return statement.executeQuery(phandle, false, queryName);
  }

  public boolean isConnectionOpen() {
    return this.conn.isOpen();
  }

}
