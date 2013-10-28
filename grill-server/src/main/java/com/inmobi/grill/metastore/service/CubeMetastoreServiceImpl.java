package com.inmobi.grill.metastore.service;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.metastore.model.*;
import com.inmobi.grill.server.api.CubeMetastoreService;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;

public class CubeMetastoreServiceImpl implements CubeMetastoreService, Configurable {
  public static final Logger LOG = LogManager.getLogger(CubeMetastoreServiceImpl.class);

  private String user;
  private CubeMetastoreClient client;
  private SessionState sessionState;
  private HiveConf userConf;
  private Configuration conf;

  private static final Map<String, CubeMetastoreServiceImpl> instances =
    new HashMap<String, CubeMetastoreServiceImpl>();

  public synchronized static CubeMetastoreService getInstance(String user) {
    if (!instances.containsKey(user)) {
      CubeMetastoreServiceImpl instance = new CubeMetastoreServiceImpl();
      instance.user = user;
      instances.put(user, instance);
    }
    return instances.get(user);
  }

  public CubeMetastoreServiceImpl() {
    userConf = new HiveConf(CubeMetastoreServiceImpl.class);
    sessionState = new SessionState(userConf); 
  }

  @Override
  public String getName() {
    return "metastore";
  }

  @Override
  public void start() throws GrillException {
    LOG.info("Starting cube metastore service");
  }

  @Override
  public void stop() throws GrillException {
    LOG.info("Stopping cube metastore service");
  }

  private HiveConf getUserConf() {
    return new HiveConf(CubeMetastoreServiceImpl.class);
  }

  private synchronized CubeMetastoreClient getClient() throws GrillException {
    if (client == null) {
      try {
        LOG.info("Create new CubeMetastoreClient");
        client = CubeMetastoreClient.getInstance(userConf);
      } catch (HiveException e) {
        throw new GrillException(e);
      }
    }

    // Start session state for the current thread
    SessionState.start(sessionState);
    return client;
  }


  /**
   * Get current database used by the CubeMetastoreClient
   *
   * @return
   */
  @Override
  public String getCurrentDatabase() throws GrillException {
    return getClient().getCurrentDatabase();
  }

  /**
   * Change the current database used by the CubeMetastoreClient
   *
   * @param database
   */
  @Override
  public void setCurrentDatabase(String database) throws GrillException {
    getClient().setCurrentDatabase(database);
    LOG.info("Set database " + database);
  }

  /**
   * Drop a database from cube metastore
   *
   * @param database database name
   * @param cascade  flag indicating if the tables in the database should be dropped as well
   */
  @Override
  public void dropDatabase(String database, boolean cascade) throws GrillException {
    try {
      Hive.get(getUserConf()).dropDatabase(database, true, true, cascade);
      LOG.info("Database dropped " + database + " cascade? " + true);
    } catch (HiveException e) {
      throw new GrillException(e);
    } catch (NoSuchObjectException e) {
      throw new GrillException(e);
    }
  }

  /**
   * Create a database in Hive metastore
   * @param database database name
   * @param ignore ignore if database already exists
   * @throws GrillException
   */
  @Override
  public void createDatabase(String database, boolean ignore) throws GrillException {
    try {
      Database db = new Database();
      db.setName(database);
      Hive.get(userConf).createDatabase(db, ignore);
    } catch (AlreadyExistsException e) {
      throw new GrillException(e);
    } catch (HiveException e) {
      throw new GrillException(e);
    }
    LOG.info("Database created " + database);
  }

  /**
   * @return get all database names
   */
  @Override
  public List<String> getAllDatabases() throws GrillException{
    try {
      return Hive.get(userConf).getAllDatabases();
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  /**
   * Get list of all cubes names in the current database
   * @return
   * @throws GrillException
   */
  @Override
  public List<String> getAllCubeNames() throws GrillException {
    try {
      List<Cube> cubes = getClient().getAllCubes();
      if (cubes != null && !cubes.isEmpty()) {
        List<String> names = new ArrayList<String>(cubes.size());
        for (Cube cube : cubes) {
          names.add(cube.getName());
        }
        return names;
      }
    } catch (HiveException e) {
      throw new GrillException(e);
    }
    return null;
  }

  /**
   * Create cube based on the JAXB cube object
   * @param cube
   * @throws GrillException
   */
  @Override
  public void createCube(XCube cube) throws GrillException {
    try {
      getClient().createCube(JAXBUtils.hiveCubeFromXCube(cube));
      LOG.info("Created cube " + cube.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  /**
   * Get a cube from the metastore
   * @param cubeName
   * @return
   * @throws GrillException
   */
  @Override
  public XCube getCube(String cubeName) throws GrillException {
    try {
      Cube c = getClient().getCube(cubeName);
      if (c != null) {
        return JAXBUtils.xCubeFromHiveCube(c);
      }
    } catch (HiveException e) {
      throw new GrillException(e);
    }
    return null;
  }

  /**
   * Drop a cube from the metastore in the currently deleted database
   * @param cubeName
   * @param cascade
   */
  public void dropCube(String cubeName, boolean cascade) throws GrillException {
    try {
      getClient().dropCube(cubeName, cascade);
      LOG.info("Dropped cube " + cubeName + " cascade? " + cascade);
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  /**
   * Update cube
   * @param cube JAXB Cube object
   * @throws GrillException
   */
  @Override
  public void updateCube(XCube cube) throws GrillException {
    try {
      getClient().alterCube(cube.getName(), JAXBUtils.hiveCubeFromXCube(cube));
      LOG.info("Cube updated " + cube.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  @Override
  public void init() throws GrillException {
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Create a cube dimension table based on JAXB object
   * @param xDimTable
   * @throws GrillException
   */
  @Override
  public void createCubeDimensionTable(DimensionTable xDimTable) throws GrillException {
    String dimName = xDimTable.getName();
    List<FieldSchema> columns = JAXBUtils.fieldSchemaListFromColumns(xDimTable.getColumns());
    Map<String, List<TableReference>> references =
      JAXBUtils.mapFromDimensionReferences(xDimTable.getDimensionsReferences());
    Map<Storage, UpdatePeriod> updatePeriodMap =
      JAXBUtils.dumpPeriodsFromUpdatePeriods(xDimTable.getUpdatePeriods());
    Map<String, String> properties = JAXBUtils.mapFromXProperties(xDimTable.getProperties());

    try {
      System.out.println("# Columns: "+ columns);
      getClient().createCubeDimensionTable(dimName,
        columns,
        xDimTable.getWeight(),
        references,
        updatePeriodMap,
        properties);
      LOG.info("Dimension Table created " + xDimTable.getName());
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }

  @Override
  public void dropDimensionTable(String dimension, boolean cascade) throws GrillException {
    try {
      getClient().dropDimension(dimension, cascade);
      LOG.info("Dropped dimension table " + dimension + " cascade? " + cascade);
    } catch (HiveException e) {
      throw new GrillException(e);
    }
  }
  
  @Override
  public DimensionTable getDimensionTable(String dimName) throws GrillException {
  	try {
  		CubeDimensionTable cubeDimTable = getClient().getDimensionTable(dimName);
  		return JAXBUtils.dimTableFromCubeDimTable(cubeDimTable);
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
  	}
  }
  
  @Override
  public void updateDimensionTable(DimensionTable dimensionTable) throws GrillException {
  	try {
  		getClient().alterCubeDimensionTable(dimensionTable.getName(), 
  				JAXBUtils.cubeDimTableFromDimTable(dimensionTable));
  		LOG.info("Updated dimension table " + dimensionTable.getName());
  	} catch (HiveException exc) {
  		throw new GrillException(exc);
  	}
  }
}
