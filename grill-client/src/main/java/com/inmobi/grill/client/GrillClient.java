package com.inmobi.grill.client;

import com.google.common.collect.Maps;
import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.metastore.*;
import com.inmobi.grill.api.query.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;


public class GrillClient {
  private static final Log LOG = LogFactory.getLog(GrillClient.class);
  private final GrillClientConfig conf;
  private GrillConnection conn;
  private final HashMap<QueryHandle, GrillStatement> statementMap =
      Maps.newHashMap();

  public GrillClient() {
    this.conf = new GrillClientConfig();
    connectToGrillServer();
  }

  public GrillClient(GrillClientConfig conf) {
    this.conf = conf;
    connectToGrillServer();
  }

  public QueryHandle executeQueryAsynch(String sql) {
    GrillStatement statement = new GrillStatement(conn);
    LOG.debug("Executing query " + sql);
    statement.execute(sql, false);
    GrillQuery query = statement.getQuery();
    LOG.debug("Adding query to statementMap " + query.getQueryHandle());
    statementMap.put(query.getQueryHandle(), statement);
    return query.getQueryHandle();
  }

  public GrillClientResultSet getResults(String sql) {
    GrillStatement statement = new GrillStatement(conn);
    LOG.debug("Executing query " + sql);
    statement.execute(sql, true);
    return new GrillClientResultSet(statement.getResultSet(),
        statement.getResultSetMetaData());
  }

  public GrillClientResultSet getAsyncResults(QueryHandle q) {
    GrillStatement statement = new GrillStatement(conn);
    GrillQuery query = statement.getQuery(q);

    return new GrillClientResultSet(statement.getResultSet(query),
        statement.getResultSetMetaData(query));
  }

  private GrillStatement getGrillStatement(QueryHandle query) {
    return this.statementMap.get(query);
  }

  public QueryStatus getQueryStatus(QueryHandle query) {
    return new GrillStatement(conn).getQuery(query).getStatus();
  }

  public QueryStatus getQueryStatus(String q) {
    QueryHandle qh = QueryHandle.fromString(q);
    return getQueryStatus(q);
  }

  public QueryPlan getQueryPlan(String q) {
    return new GrillStatement(conn).explainQuery(q);
  }

  public boolean killQuery(QueryHandle q) {
    GrillStatement statement = new GrillStatement(conn);

    return statement.kill(statement.getQuery(q));
  }


  public QueryResult getResults(QueryHandle query) {
    QueryStatus status = getGrillStatement(query).getStatus();
    if (!status.isResultSetAvailable()) {
      LOG.debug("Current status of the query is " + status);
      throw new IllegalStateException("Resultset for the query "
          + query + " is not available, its current status is " + status);
    }
    return getGrillStatement(query).getResultSet();
  }

  public List<QueryHandle> getQueries() {
    return new GrillStatement(conn).getAllQueries();
  }


  private void connectToGrillServer() {
    LOG.debug("Connecting to grill server " + new GrillConnectionParams(conf));
    conn = new GrillConnection(new GrillConnectionParams(conf));
    conn.open();
    LOG.debug("Successfully connected to server " + conn);
  }


  public List<String> getAllDatabases() {
    LOG.debug("Getting all database");
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    return mc.getAlldatabases();
  }

  public List<String> getAllFactTables() {
    LOG.debug("Getting all fact table");
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    return mc.getAllFactTables();
  }


  public List<String> getAllDimensionTables() {
    LOG.debug("Getting all dimension table");
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    return mc.getAllDimensionTables();
  }

  public List<String> getAllCubes() {
    LOG.debug("Getting all cubes in database");
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    return mc.getAllCubes();
  }

  public String getCurrentDatabae() {
    LOG.debug("Getting current database");
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    return mc.getCurrentDatabase();
  }


  public boolean setDatabase(String database) {
    LOG.debug("Set the database to " + database);
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    APIResult result = mc.setDatabase(database);
    return result.getStatus() == APIResult.Status.SUCCEEDED;
  }

  public APIResult dropDatabase(String database) {
    LOG.debug("Dropping database " + database);
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    APIResult result = mc.dropDatabase(database);
    LOG.debug("Return status of dropping " + database + " result " + result);
    return result;
  }

  public APIResult createDatabase(String database, boolean ignoreIfExists) {
    LOG.debug("Creating database " + database + " ignore " + ignoreIfExists);
    GrillMetadataClient mc = new GrillMetadataClient(conn);
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
    GrillMetadataClient mc = new GrillMetadataClient(conn);

    return mc.createFactTable(factSpec, storageSpecPath);
  }

  public APIResult createCube(String cubeSpec) {
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    return mc.createCube(cubeSpec);
  }

  public APIResult createStorage(String storageSpec) {
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    return mc.createNewStorage(storageSpec);
  }

  public APIResult createDimension(String dimSpec, String storageSpec) {
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    return mc.createDimensionTable(dimSpec, storageSpec);
  }

  public List<String> getAllStorages() {
    GrillMetadataClient mc = new GrillMetadataClient(conn);
    return mc.getAllStorages();
  }

  public APIResult dropDimensionTable(String dim, boolean cascade) {
    return new GrillMetadataClient(conn).dropDimensionTable(dim, cascade);
  }

  public APIResult dropFactTable(String fact, boolean cascade) {
    return new GrillMetadataClient(conn).dropFactTable(fact, cascade);
  }

  public APIResult dropCube(String cube) {
    return new GrillMetadataClient(conn).dropCube(cube);
  }

  public APIResult dropStorage(String storage) {
    return new GrillMetadataClient(conn).dropStorage(storage);
  }

  public APIResult updateFactTable(String factName, String factSpec) {
    return new GrillMetadataClient(conn).updateFactTable(factName, factSpec);
  }

  public APIResult updateDimensionTable(String dimName, String dimSpec) {
    return new GrillMetadataClient(conn).updateDimensionTable(dimName, dimSpec);
  }

  public APIResult updateCube(String cubeName, String cubeSpec) {
    return new GrillMetadataClient(conn).updateCube(cubeName, cubeSpec);
  }

  public APIResult updateStorage(String storageName, String storageSpec) {
    return new GrillMetadataClient(conn).updateStorage(storageName, storageSpec);
  }

  public FactTable getFactTable(String factName) {
    return new GrillMetadataClient(conn).getFactTable(factName);
  }

  public DimensionTable getDimensionTable(String dimName) {
    return new GrillMetadataClient(conn).getDimensionTable(dimName);
  }

  public XCube getCube(String cubeName) {
    return new GrillMetadataClient(conn).getCube(cubeName);
  }

  public XStorage getStorage(String storageName) {
    return new GrillMetadataClient(conn).getStorage(storageName);
  }

  public List<String> getFactStorages(String fact) {
    return new GrillMetadataClient(conn).getAllStoragesOfFactTable(fact);
  }

  public List<String> getDimStorages(String dim) {
    return new GrillMetadataClient(conn).getDimensionStorage(dim);
  }

  public APIResult dropAllStoragesOfDim(String table) {
    return new GrillMetadataClient(conn).dropAllStoragesOfDimension(table);
  }

  public APIResult dropAllStoragesOfFact(String table) {
    return new GrillMetadataClient(conn).dropAllStoragesOfFactTable(table);
  }

  public APIResult addStorageToFact(String factName, String spec) {
    return new GrillMetadataClient(conn).addStorageToFactTable(factName, spec);
  }

  public APIResult dropStorageFromFact(String factName, String storage) {
    return new GrillMetadataClient(conn).dropStorageFromFactTable(factName, storage);
  }

  public XStorageTableElement getStorageFromFact(String fact, String storage) {
    return new GrillMetadataClient(conn).getStorageOfFactTable(fact, storage);
  }

  public APIResult addStorageToDim(String dim, String storage) {
    return new GrillMetadataClient(conn).addStorageToDimension(dim, storage);
  }

  public APIResult dropStorageFromDim(String dim, String storage) {
    return new GrillMetadataClient(conn).dropStoragesOfDimension(dim, storage);
  }

  public XStorageTableElement getStorageFromDim(String dim, String storage) {
    return new GrillMetadataClient(conn).getStorageOfDimension(dim, storage);
  }

  public List<XPartition> getAllPartitionsOfFact(String fact, String storage) {
    return new GrillMetadataClient(conn).getPartitionsOfFactTable(fact, storage);
  }

  public List<XPartition> getAllPartitionsOfFact(String fact, String storage, String list) {
    return new GrillMetadataClient(conn).getPartitionsOfFactTable(fact, storage, list);
  }

  public List<XPartition> getAllPartitionsOfDim(String dim, String storage) {
    return new GrillMetadataClient(conn).getAllPartitionsOfDimension(dim, storage);
  }

  public List<XPartition> getAllPartitionsOfDim(String dim, String storage, String list) {
    return new GrillMetadataClient(conn).getAllPartitionsOfDimension(dim, storage);
  }

  public APIResult dropAllPartitionsOfFact(String fact, String storage) {
    return new GrillMetadataClient(conn).dropPartitionsOfFactTable(fact, storage);
  }

  public APIResult dropAllPartitionsOfFact(String fact, String storage, String list) {
    return new GrillMetadataClient(conn).dropPartitionsOfFactTable(fact, storage, list);
  }

  public APIResult dropAllPartitionsOfDim(String dim, String storage) {
    return new GrillMetadataClient(conn).dropAllPartitionsOfDimension(dim, storage);
  }

  public APIResult dropAllPartitionsOfDim(String dim, String storage, String list) {
    return new GrillMetadataClient(conn).dropAllPartitionsOfDimension(dim, storage, list);
  }

  public APIResult addPartitionToFact(String table, String storage, String partSpec) {
    return new GrillMetadataClient(conn).addPartitionToFact(table, storage, partSpec);
  }

  public APIResult addPartitionToDim(String table, String storage, String partSpec) {
    return new GrillMetadataClient(conn).addPartitionToDimension(table, storage, partSpec);
  }


}
