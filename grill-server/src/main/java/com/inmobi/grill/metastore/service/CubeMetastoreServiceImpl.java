package com.inmobi.grill.metastore.service;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.CubeMetastoreService;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CubeMetastoreServiceImpl implements CubeMetastoreService {
  public static final Logger LOG = LogManager.getLogger(CubeMetastoreServiceImpl.class);

  private String user;
  private CubeMetastoreClient client;
  private SessionState sessionState;
  private HiveConf userConf;

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
    return "CubeMetastoreService";
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Get database");
    }
    return getClient().getCurrentDatabase();
  }

  /**
   * Change the current database used by the CubeMetastoreClient
   *
   * @param database
   */
  @Override
  public void setCurrentDatabase(String database) throws GrillException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Set database " + database);
    }
    getClient().setCurrentDatabase(database);
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
}
