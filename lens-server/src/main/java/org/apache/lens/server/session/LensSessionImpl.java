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
package org.apache.lens.server.session;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.NotFoundException;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.session.SessionService;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSessionImpl;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class LensSessionImpl.
 */
@Slf4j
public class LensSessionImpl extends HiveSessionImpl {

  /** The persist info. */
  private LensSessionPersistInfo persistInfo = new LensSessionPersistInfo();

  /** The last access time. */
  private long lastAccessTime = System.currentTimeMillis();

  /** The session timeout. */
  private long sessionTimeout;

  /** The conf. */
  private Configuration conf = createDefaultConf();

  /**
   * Keep track of DB static resources which failed to be added to this session
   */
  private final Map<String, List<ResourceEntry>> failedDBResources = new HashMap<String, List<ResourceEntry>>();



  /**
   * Cache of database specific class loaders for this session
   * This is updated lazily on add/remove resource calls and switch database calls.
   */
  private final Map<String, ClassLoader> sessionDbClassLoaders = new HashMap<String, ClassLoader>();

  @Setter(AccessLevel.PROTECTED)
  private DatabaseResourceService dbResService;


  /**
   * Inits the persist info.
   *
   * @param sessionHandle the session handle
   * @param username      the username
   * @param password      the password
   * @param sessionConf   the session conf
   */
  private void initPersistInfo(SessionHandle sessionHandle, String username, String password,
    Map<String, String> sessionConf) {
    persistInfo.setSessionHandle(new LensSessionHandle(sessionHandle.getHandleIdentifier().getPublicId(), sessionHandle
      .getHandleIdentifier().getSecretId()));
    persistInfo.setUsername(username);
    persistInfo.setPassword(password);
    persistInfo.setLastAccessTime(lastAccessTime);
    persistInfo.setSessionConf(sessionConf);
  }

  private static Configuration sessionDefaultConfig;
  /**
   * Creates the default conf.
   *
   * @return the configuration
   */
  public static synchronized Configuration createDefaultConf() {
    if (sessionDefaultConfig == null) {
      Configuration conf = new Configuration(false);
      conf.addResource("lenssession-default.xml");
      conf.addResource("lens-site.xml");
      sessionDefaultConfig = new Configuration(false);
      Iterator<Map.Entry<String, String>> confItr = conf.iterator();
      while (confItr.hasNext()) {
        Map.Entry<String, String> prop = confItr.next();
        if (!prop.getKey().startsWith(LensConfConstants.SERVER_PFX)) {
          sessionDefaultConfig.set(prop.getKey(), prop.getValue());
        }
      }
    }
    //Not exposing sessionDefaultConfig directly to insulate it form modifications
    return new Configuration(sessionDefaultConfig);
  }

  /** The default hive session conf. */
  public static final Map<String, String> DEFAULT_HIVE_SESSION_CONF = getHiveSessionConf();

  public static Map<String, String> getHiveSessionConf() {
    Configuration defaultConf = createDefaultConf();
    return defaultConf.getValByRegex("hive.*");
  }

  /**
   * Instantiates a new lens session impl.
   *
   * @param protocol    the protocol
   * @param username    the username
   * @param password    the password
   * @param serverConf  the server conf
   * @param sessionConf the session conf
   * @param ipAddress   the ip address
   */
  public LensSessionImpl(TProtocolVersion protocol, String username, String password, HiveConf serverConf,
    Map<String, String> sessionConf, String ipAddress) {
    super(protocol, username, password, serverConf, sessionConf, ipAddress);
    initPersistInfo(getSessionHandle(), username, password, sessionConf);
    sessionTimeout = 1000 * serverConf.getLong(LensConfConstants.SESSION_TIMEOUT_SECONDS,
      LensConfConstants.SESSION_TIMEOUT_SECONDS_DEFAULT);
    if (sessionConf != null) {
      for (Map.Entry<String, String> entry : sessionConf.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
  }

  public Configuration getSessionConf() {
    return conf;
  }

  /**
   * Constructor used when restoring session.
   *
   * @param sessionHandle the session handle
   * @param protocol      the protocol
   * @param username      the username
   * @param password      the password
   * @param serverConf    the server conf
   * @param sessionConf   the session conf
   * @param ipAddress     the ip address
   */
  public LensSessionImpl(SessionHandle sessionHandle, TProtocolVersion protocol, String username, String password,
    HiveConf serverConf, Map<String, String> sessionConf, String ipAddress) {
    super(sessionHandle, protocol, username, password, serverConf, sessionConf, ipAddress);
    initPersistInfo(getSessionHandle(), username, password, sessionConf);
    sessionTimeout = 1000 * serverConf.getLong(LensConfConstants.SESSION_TIMEOUT_SECONDS,
      LensConfConstants.SESSION_TIMEOUT_SECONDS_DEFAULT);
  }

  @Override
  public void close() throws HiveSQLException {
    super.close();

    // Release class loader resources
    synchronized (sessionDbClassLoaders) {
      for (Map.Entry<String, ClassLoader> entry : sessionDbClassLoaders.entrySet()) {
        try {
          // Close the class loader only if its not a class loader maintained by the DB service
          if (entry.getValue() != getDbResService().getClassLoader(entry.getKey())) {
            // This is a utility in hive-common
            JavaUtils.closeClassLoader(entry.getValue());
          }
        } catch (Exception e) {
          log.error("Error closing session classloader for session: {}", getSessionHandle().getSessionId(), e);
        }
      }
      sessionDbClassLoaders.clear();
    }
  }

  public CubeMetastoreClient getCubeMetastoreClient() throws LensException {
    try {
      CubeMetastoreClient cubeClient = CubeMetastoreClient.getInstance(getHiveConf());
      // since cube client's configuration is a copy of the session conf passed, setting classloader in cube client's
      // configuration does not modify session conf's classloader.
      // We are doing this on the cubeClient instance than doing a copy of conf and setting classloader and pass it to
      // cube metastore client because CubeMetastoreClient would have been cached and refreshing the classloader
      // should not result in invalidating the CubeMetastoreClient cache.
      cubeClient.getConf().setClassLoader(getClassLoader());
      return cubeClient;
    } catch (HiveException e) {
      throw new LensException(e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.cli.session.HiveSessionImpl#acquire()
   */
  public synchronized void acquire() {
    try {
      super.acquire();
      // Update thread's class loader with current DBs class loader
      Thread.currentThread().setContextClassLoader(getClassLoader(getCurrentDatabase()));
    } catch (HiveSQLException e) {
      throw new NotFoundException("Could not acquire the session", e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.cli.session.HiveSessionImpl#release()
   */
  public synchronized void release() {
    lastAccessTime = System.currentTimeMillis();
    super.release();
  }

  public boolean isActive() {
    long inactiveAge = System.currentTimeMillis() - lastAccessTime;
    return inactiveAge < sessionTimeout;
  }

  /**
   * Sets the config.
   *
   * @param config   the config to overlay
   */
  public void setConfig(Map<String, String> config) {
    persistInfo.getConfig().putAll(config);
  }

  /**
   * Removes the resource.
   *
   * @param type the type
   * @param path the path
   */
  public void removeResource(String type, String path) {
    Iterator<ResourceEntry> itr = persistInfo.getResources().iterator();
    while (itr.hasNext()) {
      ResourceEntry res = itr.next();
      if (res.getType().equals(type) && res.getLocation().equals(path)) {
        itr.remove();
      }
    }
    updateSessionDbClassLoader(getSessionState().getCurrentDatabase());
  }

  /**
   * Adds the resource.
   *
   * @param type the type
   * @param path the path
   */
  public void addResource(String type, String path) {
    ResourceEntry resource = new ResourceEntry(type, path);
    persistInfo.getResources().add(resource);
    synchronized (sessionDbClassLoaders) {
      // Update all DB class loaders
      updateSessionDbClassLoader(getSessionState().getCurrentDatabase());
    }
  }

  protected List<ResourceEntry> getResources() {
    return persistInfo.getResources();
  }

  protected Map<String, String> getConfig() {
    return persistInfo.getConfig();
  }

  public void setCurrentDatabase(String currentDatabase) {
    persistInfo.setDatabase(currentDatabase);
    getSessionState().setCurrentDatabase(currentDatabase);
    // Merge if resources are added
    synchronized (sessionDbClassLoaders) {
      updateSessionDbClassLoader(currentDatabase);
    }
  }

  private void updateSessionDbClassLoader(String database) {
    ClassLoader updatedClassLoader = getDbResService().loadDBJars(database, persistInfo.getResources());
    if (updatedClassLoader != null) {
      sessionDbClassLoaders.put(database, updatedClassLoader);
    }
  }

  private boolean areResourcesAdded() {
    return persistInfo.getResources() != null && !persistInfo.getResources().isEmpty();
  }

  private DatabaseResourceService getDbResService() {
    if (dbResService == null) {
      HiveSessionService sessionService = LensServices.get().getService(SessionService.NAME);
      return sessionService.getDatabaseResourceService();
    } else {
      return dbResService;
    }
  }

  protected ClassLoader getClassLoader(String database) {
    synchronized (sessionDbClassLoaders) {
      if (sessionDbClassLoaders.containsKey(database)) {
        return sessionDbClassLoaders.get(database);
      } else {
        try {
          ClassLoader classLoader = getDbResService().getClassLoader(database);
          if (classLoader == null) {
            log.debug("DB resource service gave null class loader for {}", database);
          } else {
            if (areResourcesAdded()) {
              // We need to update DB specific classloader with added resources
              updateSessionDbClassLoader(database);
              classLoader = sessionDbClassLoaders.get(database);
            }
          }

          return classLoader == null ? getSessionState().getConf().getClassLoader() : classLoader;
        } catch (LensException e) {
          log.error("Error getting classloader for database {} for session {} "
            + " defaulting to session state class loader", database, getSessionHandle().getSessionId(), e);
          return getSessionState().getConf().getClassLoader();
        }
      }
    }
  }

  public String getCurrentDatabase() {
    return getSessionState().getCurrentDatabase();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return getSessionHandle().getHandleIdentifier().toString();
  }

  public String getLoggedInUser() {
    return getHiveConf().get(LensConfConstants.SESSION_LOGGEDIN_USER);
  }

  public String getClusterUser() {
    return getUserName();
  }

  public LensSessionPersistInfo getLensSessionPersistInfo() {
    return persistInfo;
  }

  void setLastAccessTime(long lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  /**
   * Return resources which are added statically to the database
   * @return
   */
  public Collection<ResourceEntry> getDBResources(String database) {
    synchronized (failedDBResources) {
      List<ResourceEntry> failed = failedDBResources.get(database);
      if (failed == null && getDbResService().getResourcesForDatabase(database) != null) {
        failed = new ArrayList<ResourceEntry>(getDbResService().getResourcesForDatabase(database));
        failedDBResources.put(database, failed);
      }
      return failed;
    }
  }


  /**
   * Get session's resources which have to be added for the given database
   */
  public Collection<ResourceEntry> getPendingSessionResourcesForDatabase(String database) {
    List<ResourceEntry> pendingResources = new ArrayList<ResourceEntry>();
    for (ResourceEntry res : persistInfo.getResources()) {
      if (!res.isAddedToDatabase(database)) {
        pendingResources.add(res);
      }
    }
    return pendingResources;
  }

  /**
   * Get effective class loader for this session
   * @return
   */
  public ClassLoader getClassLoader() {
    return getClassLoader(getCurrentDatabase());
  }

  /**
   * The Class ResourceEntry.
   */
  public static class ResourceEntry {

    /** The type. */
    @Getter
    final String type;

    /** The location. */
    @Getter
    final String location;
    // For tests
    /** The restore count. */
    transient AtomicInteger restoreCount = new AtomicInteger();

    /** Set of databases for which this resource has been added */
    final transient Set<String> databases = new HashSet<String>();

    /**
     * Instantiates a new resource entry.
     *
     * @param type     the type
     * @param location the location
     */
    public ResourceEntry(String type, String location) {
      if (type == null || location == null) {
        throw new NullPointerException("ResourceEntry type or location cannot be null");
      }
      this.type = type;
      this.location = location;
    }

    public boolean isAddedToDatabase(String database) {
      return databases.contains(database);
    }

    public void addToDatabase(String database) {
      databases.add(database);
    }

    /**
     * Restored resource.
     */
    public void restoredResource() {
      restoreCount.incrementAndGet();
    }

    /**
     * Returns the value of restoreCount for the resource
     * @return
     */
    public int getRestoreCount(){
      return restoreCount.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return "type=" + type + " path=" + location;
    }

    @Override
    public int hashCode() {
      return type.hashCode() + 31 * location.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ResourceEntry) {
        ResourceEntry other = (ResourceEntry) obj;
        return type.equals(other.type) && location.equals(other.location);
      }
      return false;
    }
  }

  /**
   * The Class LensSessionPersistInfo.
   */
  @Data
  public static class LensSessionPersistInfo implements Externalizable {

    /** The resources. */
    private List<ResourceEntry> resources = new ArrayList<ResourceEntry>();

    /** The config. */
    private Map<String, String> config = new HashMap<String, String>();

    /** The session handle. */
    private LensSessionHandle sessionHandle;

    /** The database. */
    private String database;

    /** The username. */
    private String username;

    /** The password. */
    private String password;

    /** The last access time. */
    private long lastAccessTime;

    public void setSessionConf(Map<String, String> sessionConf) {
      UtilityMethods.mergeMaps(config, sessionConf, true);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeUTF(sessionHandle.toString());
      out.writeUTF(database == null ? "default" : database);
      out.writeUTF(username == null ? "" : username);
      out.writeUTF(password == null ? "" : password);

      out.writeInt(resources.size());
      for (ResourceEntry resource : resources) {
        out.writeUTF(resource.getType());
        out.writeUTF(resource.getLocation());
      }

      out.writeInt(config.size());
      for (String key : config.keySet()) {
        out.writeUTF(key);
        out.writeUTF(config.get(key));
      }
      out.writeLong(lastAccessTime);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      sessionHandle = LensSessionHandle.valueOf(in.readUTF());
      database = in.readUTF();
      username = in.readUTF();
      password = in.readUTF();

      int resSize = in.readInt();
      resources.clear();
      for (int i = 0; i < resSize; i++) {
        String type = in.readUTF();
        String location = in.readUTF();
        resources.add(new ResourceEntry(type, location));
      }

      config.clear();
      int cfgSize = in.readInt();
      for (int i = 0; i < cfgSize; i++) {
        String key = in.readUTF();
        String val = in.readUTF();
        config.put(key, val);
      }
      lastAccessTime = in.readLong();
    }
  }
}
