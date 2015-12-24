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

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.metastore.*;
import org.apache.lens.api.query.*;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.util.PathValidator;
import org.apache.lens.client.exceptions.LensAPIException;
import org.apache.lens.client.exceptions.LensBriefErrorException;
import org.apache.lens.client.model.BriefError;
import org.apache.lens.client.model.IdBriefErrorTemplate;
import org.apache.lens.client.model.IdBriefErrorTemplateKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class LensClient {
  public static final String CLILOGGER =  "cliLogger";
  private static final String DEFAULT_PASSWORD = "";
  private final LensClientConfig conf;
  @Getter
  private final LensMetadataClient mc;
  private String password;
  @Getter
  private LensConnection connection;
  private final HashMap<QueryHandle, LensStatement> statementMap =
    Maps.newHashMap();
  @Getter
  private final LensStatement statement;

  @Getter
  private PathValidator pathValidator;

  public static Logger getCliLooger() {
    return LoggerFactory.getLogger(CLILOGGER);
  }

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
    if (this.conf.get(LensClientConfig.SESSION_CLUSTER_USER) == null) {
      this.conf.set(LensClientConfig.SESSION_CLUSTER_USER, System.getProperty("user.name"));
    }
    connectToLensServer();
    mc = new LensMetadataClient(connection);
    statement = new LensStatement(connection);
  }

  public LensClient(Credentials cred) {
    this(cred.getUsername(), cred.getPassword());
  }

  public LensMetadataClient getMetadataClient() {
    return mc;
  }

  public LensAPIResult<QueryHandle> executeQueryAsynch(String sql, String queryName) throws LensAPIException {
    log.debug("Executing query {}", sql);
    LensAPIResult<QueryHandle> lensAPIResult = statement.execute(sql, false, queryName);
    LensQuery query = statement.getQuery();
    log.debug("Adding query to statementMap {}", query.getQueryHandle());
    statementMap.put(query.getQueryHandle(), statement);
    return lensAPIResult;
  }

  public Date getLatestDateOfCube(String cubeName, String timePartition) {
    return mc.getLatestDateOfCube(cubeName, timePartition);
  }

  public List<String> getPartitionTimelines(String factName, String storageName, String updatePeriod,
    String timeDimension) {
    return mc.getPartitionTimelines(factName, storageName, updatePeriod, timeDimension);
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

  public LensClientResultSetWithStats getResults(String sql, String queryName) throws LensAPIException {
    log.debug("Executing query {}", sql);
    statement.execute(sql, true, queryName);
    return getResultsFromStatement(statement);
  }

  private LensClientResultSetWithStats getResultsFromStatement(LensStatement statement) {
    QueryStatus.Status status = statement.getStatus().getStatus();
    if (status != QueryStatus.Status.SUCCESSFUL) {
      IdBriefErrorTemplate errorResult = new IdBriefErrorTemplate(IdBriefErrorTemplateKey.QUERY_ID,
          statement.getQueryHandleString(), new BriefError(statement.getErrorCode(), statement.getErrorMessage()));
      throw new LensBriefErrorException(errorResult);
    }
    LensClientResultSet result = null;
    if (statement.getStatus().isResultSetAvailable()) {
      result = new LensClientResultSet(statement.getResultSetMetaData(), statement.getResultSet());
    }
    return new LensClientResultSetWithStats(result, statement.getQuery());
  }

  private LensClientResultSetWithStats getResultsFromHandle(QueryHandle q, boolean async) {
    if (!async) {
      statement.waitForQueryToComplete(q);
    }
    LensQuery query = statement.getQuery(q);
    if (query.getStatus().getStatus()
      == QueryStatus.Status.FAILED) {
      throw new IllegalStateException(query.getStatus().getErrorMessage());
    }
    LensClientResultSet result = null;
    if (statement.getStatus().isResultSetAvailable()) {
      result = new LensClientResultSet(statement.getResultSetMetaData(), statement.getResultSet());
    }
    return new LensClientResultSetWithStats(result, statement.getQuery());
  }

  public LensClientResultSetWithStats getAsyncResults(QueryHandle q) {
    return getResultsFromHandle(q, true);
  }

  public LensClientResultSetWithStats getSyncResults(QueryHandle q) {
    return getResultsFromHandle(q, false);
  }

  public Response getHttpResults() {
    return statement.getHttpResultSet();
  }

  public Response getHttpResults(QueryHandle q) {
    return statement.getHttpResultSet(statement.getQuery(q));
  }

  public LensStatement getLensStatement(QueryHandle query) {
    return this.statementMap.get(query);
  }

  public QueryStatus getQueryStatus(QueryHandle query) {
    return statement.getQuery(query).getStatus();
  }

  public LensQuery getQueryDetails(QueryHandle handle) {
    return statement.getQuery(handle);
  }

  public QueryStatus getQueryStatus(String q) {
    return getQueryStatus(QueryHandle.fromString(q));
  }

  public LensQuery getQueryDetails(String handle) {
    return getQueryDetails(QueryHandle.fromString(handle));
  }

  public LensAPIResult<QueryPlan> getQueryPlan(String q) throws LensAPIException {
    return statement.explainQuery(q);
  }

  public boolean killQuery(QueryHandle q) {
    return statement.kill(statement.getQuery(q));
  }


  public QueryResult getResults(QueryHandle query) {
    QueryStatus status = getLensStatement(query).getStatus();
    if (!status.isResultSetAvailable()) {
      log.debug("Current status of the query is {}", status);
      throw new IllegalStateException("Resultset for the query "
        + query + " is not available, its current status is " + status);
    }
    return getLensStatement(query).getResultSet();
  }

  public List<QueryHandle> getQueries(String state, String queryName, String user, String driver, long fromDate,
    long toDate) {
    return statement.getAllQueries(state, queryName, user, driver, fromDate, toDate);
  }

  private void connectToLensServer() {
    log.debug("Connecting to lens server {}", new LensConnectionParams(conf));
    connection = new LensConnection(new LensConnectionParams(conf));
    connection.open(password);
    log.debug("Successfully connected to server {}", connection);
    pathValidator = new PathValidator(connection.getLensConnectionParams().getSessionConf());
    Preconditions.checkNotNull(pathValidator, "Error in initializing Path Validator.");
  }


  public List<String> getAllDatabases() {
    log.debug("Getting all database");
    return mc.getAlldatabases();
  }

  public List<String> getAllNativeTables() {
    log.debug("Getting all native tables");
    return mc.getAllNativeTables();
  }

  public List<String> getAllFactTables() {
    log.debug("Getting all fact table");
    return mc.getAllFactTables();
  }

  public List<String> getAllFactTables(String cubeName) {
    log.debug("Getting all fact table");
    return mc.getAllFactTables(cubeName);
  }

  public List<String> getAllDimensionTables() {
    log.debug("Getting all dimension table");
    return mc.getAllDimensionTables();
  }

  public List<String> getAllDimensionTables(String dimensionName) {
    log.debug("Getting all dimension table");
    return mc.getAllDimensionTables(dimensionName);
  }

  public List<String> getAllCubes() {
    log.debug("Getting all cubes in database");
    return mc.getAllCubes();
  }

  public List<String> getAllDimensions() {
    log.debug("Getting all dimensions in database");
    return mc.getAllDimensions();
  }

  public String getCurrentDatabae() {
    log.debug("Getting current database");
    return mc.getCurrentDatabase();
  }


  public boolean setDatabase(String database) {
    log.debug("Set the database to {}", database);
    APIResult result = mc.setDatabase(database);
    return result.getStatus() == APIResult.Status.SUCCEEDED;
  }

  public APIResult dropDatabase(String database, boolean cascade) {
    log.debug("Dropping database {}, cascade: {}", database, cascade);
    APIResult result = mc.dropDatabase(database, cascade);
    log.debug("Return status of dropping {} result {}", database, result);
    return result;
  }

  public APIResult createDatabase(String database, boolean ignoreIfExists) {
    log.debug("Creating database {} ignore {}", database, ignoreIfExists);
    APIResult result = mc.createDatabase(database, ignoreIfExists);
    log.debug("Create database result {}", result);
    return result;
  }

  public APIResult setConnectionParam(String key, String val) {
    return this.connection.setConnectionParams(key, val);
  }

  public List<String> getConnectionParam() {
    return this.connection.getConnectionParams();
  }

  public List<String> getConnectionParam(String key) {
    return this.connection.getConnectionParams(key);
  }

  public APIResult closeConnection() {
    log.debug("Closing lens connection: {}", new LensConnectionParams(conf));
    return this.connection.close();
  }

  public APIResult addJarResource(String path) {
    return this.connection.addResourceToConnection("jar", path);
  }

  public APIResult removeJarResource(String path) {
    return this.connection.removeResourceFromConnection("jar", path);
  }

  public APIResult addFileResource(String path) {
    return this.connection.addResourceToConnection("file", path);
  }

  public APIResult removeFileResource(String path) {
    return this.connection.removeResourceFromConnection("file", path);
  }

  public APIResult createFactTable(String factSpec) {
    return mc.createFactTable(factSpec);
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

  public APIResult createDimensionTable(String dimSpec) {
    return mc.createDimensionTable(dimSpec);
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

  public XFactTable getFactTable(String factName) {
    return mc.getFactTable(factName);
  }

  public XDimensionTable getDimensionTable(String dimName) {
    return mc.getDimensionTable(dimName);
  }

  public XNativeTable getNativeTable(String tblName) {
    return mc.getNativeTable(tblName);
  }

  public XCube getCube(String cubeName) {
    return mc.getCube(cubeName);
  }

  public XFlattenedColumns getQueryableFields(String table, boolean flattened) {
    return mc.getQueryableFields(table, flattened);
  }
  public XJoinChains getJoinChains(String table) {
    return mc.getJoinChains(table);
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

  public APIResult addPartitionsToFact(String table, String storage, String partsSpec) {
    return mc.addPartitionsToFactTable(table, storage, partsSpec);
  }

  public APIResult addPartitionToFact(String table, String storage, XPartition xp) {
    return mc.addPartitionToFactTable(table, storage, xp);
  }

  public APIResult addPartitionsToFact(String table, String storage, XPartitionList xpList) {
    return mc.addPartitionsToFactTable(table, storage, xpList);
  }

  public APIResult addPartitionToDim(String table, String storage, String partSpec) {
    return mc.addPartitionToDimensionTable(table, storage, partSpec);
  }

  public APIResult addPartitionToDim(String table, String storage, XPartition xp) {
    return mc.addPartitionToDimensionTable(table, storage, xp);
  }

  public APIResult addPartitionsToDim(String table, String storage, XPartitionList xpList) {
    return mc.addPartitionsToDimensionTable(table, storage, xpList);
  }

  public APIResult addPartitionsToDim(String table, String storage, String partsSpec) {
    return mc.addPartitionsToDimensionTable(table, storage, partsSpec);
  }
  public APIResult updatePartitionOfFact(String table, String storage, String partSpec) {
    return mc.updatePartitionOfFactTable(table, storage, partSpec);
  }

  public APIResult updatePartitionsOfFact(String table, String storage, String partsSpec) {
    return mc.updatePartitionsOfFactTable(table, storage, partsSpec);
  }

  public APIResult updatePartitionOfFact(String table, String storage, XPartition xp) {
    return mc.updatePartitionOfFactTable(table, storage, xp);
  }

  public APIResult updatePartitionsOfFact(String table, String storage, XPartitionList xpList) {
    return mc.updatePartitionsOfFactTable(table, storage, xpList);
  }

  public APIResult updatePartitionOfDim(String table, String storage, String partSpec) {
    return mc.updatePartitionOfDimensionTable(table, storage, partSpec);
  }

  public APIResult updatePartitionOfDim(String table, String storage, XPartition xp) {
    return mc.updatePartitionOfDimensionTable(table, storage, xp);
  }

  public APIResult updatePartitionsOfDim(String table, String storage, XPartitionList xpList) {
    return mc.updatePartitionsOfDimensionTable(table, storage, xpList);
  }

  public APIResult updatePartitionsOfDim(String table, String storage, String partsSpec) {
    return mc.updatePartitionsOfDimensionTable(table, storage, partsSpec);
  }

  public LensAPIResult<QueryPrepareHandle> prepare(String sql, String queryName) throws LensAPIException {
    return statement.prepareQuery(sql, queryName);
  }

  public LensAPIResult<QueryPlan> explainAndPrepare(String sql, String queryName) throws LensAPIException {
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
    return getResultsFromHandle(qh, true);
  }

  public QueryHandle executePrepared(QueryPrepareHandle phandle, String queryName) {
    return statement.executeQuery(phandle, false, queryName);
  }

  public boolean isConnectionOpen() {
    return this.connection.isOpen();
  }

  public List<String> listResources(String type) {
    return this.connection.listResourcesFromConnection(type);
  }

  public Response getLogs(String logFile) {
    return this.connection.getLogs(logFile);
  }
}
