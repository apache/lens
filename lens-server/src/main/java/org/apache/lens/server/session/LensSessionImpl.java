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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
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
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSessionImpl;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class LensSessionImpl.
 */
@Slf4j
public class LensSessionImpl extends HiveSessionImpl implements AutoCloseable {

  /** The persist info. */
  private LensSessionPersistInfo persistInfo = new LensSessionPersistInfo();

  /** The last access time. */
  private long lastAccessTime = System.currentTimeMillis();

  /** The session timeout. */
  private long sessionTimeout;
  private static class IntegerThreadLocal extends ThreadLocal<Integer> {
    @Override
    protected Integer initialValue() {
      return 0;
    }
    public Integer incrementAndGet() {
      set(get() + 1);
      return get();
    }
    public Integer decrementAndGet() {
      set(get() - 1);
      return get();
    }
  }
  private IntegerThreadLocal acquireCount = new IntegerThreadLocal();

  /** The conf. */
  private Configuration conf = createDefaultConf();
  /**
   * List of queries which are submitted in this session.
   */
  @Getter
  private final List<QueryHandle> activeQueries = new ArrayList<>();

  /**
   * Keep track of DB static resources which failed to be added to this session
   */
  private final Map<String, List<ResourceEntry>> failedDBResources = new HashMap<>();


  /**
   * Cache of database specific class loaders for this session
   * This is updated lazily on add/remove resource calls and switch database calls.
   */
  private final Map<String, SessionClassLoader> sessionDbClassLoaders = new HashMap<>();

  @Setter(AccessLevel.PROTECTED)
  private DatabaseResourceService dbResService;


  /**
   * Inits the persist info.
   * @param sessionConf   the session conf
   */
  private void initPersistInfo(Map<String, String> sessionConf) {
    persistInfo.setSessionHandle(new LensSessionHandle(getSessionHandle().getHandleIdentifier().getPublicId(),
      getSessionHandle().getHandleIdentifier().getSecretId()));
    persistInfo.setUsername(getUserName());
    persistInfo.setPassword(getPassword());
    persistInfo.setLastAccessTime(lastAccessTime);
    persistInfo.setSessionConf(sessionConf);
    if (sessionConf != null) {
      for (Map.Entry<String, String> entry : sessionConf.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
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
      for (Map.Entry<String, String> prop : conf) {
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
   * @param ipAddress   the ip address
   */
  public LensSessionImpl(TProtocolVersion protocol, String username, String password, HiveConf serverConf,
    String ipAddress) {
    super(protocol, username, password, serverConf, ipAddress);
    sessionTimeout = 1000 * serverConf.getLong(LensConfConstants.SESSION_TIMEOUT_SECONDS,
      LensConfConstants.SESSION_TIMEOUT_SECONDS_DEFAULT);
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
   * @param ipAddress     the ip address
   */
  public LensSessionImpl(SessionHandle sessionHandle, TProtocolVersion protocol, String username, String password,
    HiveConf serverConf, String ipAddress) {
    super(sessionHandle, protocol, username, password, serverConf, ipAddress);
    sessionTimeout = 1000 * serverConf.getLong(LensConfConstants.SESSION_TIMEOUT_SECONDS,
      LensConfConstants.SESSION_TIMEOUT_SECONDS_DEFAULT);
  }

  @Override
  public void open(Map<String, String> sessionConfMap) throws HiveSQLException {
    super.open(sessionConfMap);
    initPersistInfo(sessionConfMap);
  }

  @Override
  public void close() throws HiveSQLException {
    ClassLoader nonDBClassLoader = getSessionState().getConf().getClassLoader();
    super.close();
    // Release class loader resources
    JavaUtils.closeClassLoadersTo(nonDBClassLoader, getClass().getClassLoader());
    synchronized (sessionDbClassLoaders) {
      for (Map.Entry<String, SessionClassLoader> entry : sessionDbClassLoaders.entrySet()) {
        try {
          // Closing session level classloaders up untill the db class loader if present, or null.
          // When db class loader is null, the class loader in the session is a single class loader
          // which stays as it is on database switch -- provided the new db doesn't have db jars.
          // The following line will close class loaders made on top of db class loaders and will close
          // only one classloader without closing the parents. In case of no db class loader, the session
          // classloader will already have been closed by either super.close() or before this for loop.
          JavaUtils.closeClassLoadersTo(entry.getValue(), getDbResService().getClassLoader(entry.getKey()));
        } catch (Exception e) {
          log.error("Error closing session classloader for session: {}", getSessionHandle().getSessionId(), e);
        }
      }
      sessionDbClassLoaders.clear();
    }
    // reset classloader in close
    Thread.currentThread().setContextClassLoader(LensSessionImpl.class.getClassLoader());
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
  public void acquire() {
    this.acquire(true);
  }
  @Override
  public void acquire(boolean userAccess) {
    super.acquire(userAccess);
    if (acquireCount.incrementAndGet() == 1) { // first acquire
      // Update thread's class loader with current DBs class loader
      ClassLoader classLoader = getClassLoader(getCurrentDatabase());
      Thread.currentThread().setContextClassLoader(classLoader);
      SessionState.getSessionConf().setClassLoader(classLoader);
    }
    setActive();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hive.service.cli.session.HiveSessionImpl#release()
   */
  public void release() {
    this.release(true);
  }

  @Override
  public synchronized void release(boolean userAccess) {
    setActive();
    if (acquireCount.decrementAndGet() == 0) {
      super.release(userAccess);
      // reset classloader in release
      Thread.currentThread().setContextClassLoader(LensSessionImpl.class.getClassLoader());
    }
  }

  public boolean isActive() {
    return System.currentTimeMillis() - lastAccessTime < sessionTimeout
      && (!persistInfo.markedForClose|| activeOperationsPresent());
  }
  public boolean isMarkedForClose() {
    return persistInfo.isMarkedForClose();
  }
  public synchronized void setActive() {
    setLastAccessTime(System.currentTimeMillis());
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
      if (res.getType().equalsIgnoreCase(type) && res.getUri().equals(path)) {
        itr.remove();
      }
    }
    // New classloaders will be created. Remove resource is expensive, add resource is cheap.
    updateAllSessionClassLoaders();
  }

  /**
   * Adds the resource.
   *
   * @param type the type
   * @param path the path
   * @param finalLocation The final location where resources is downloaded
   */
  public void addResource(String type, String path, String finalLocation) {
    ResourceEntry resource = new ResourceEntry(type, path, finalLocation);
    persistInfo.getResources().add(resource);
    // The following call updates the existing classloaders without creating new instances.
    // Add resource is cheap :)
    addResourceToAllSessionClassLoaders(resource);
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
    // Make sure entry is there in classloader cache
    synchronized (sessionDbClassLoaders) {
      updateSessionDbClassLoader(currentDatabase);
    }
  }

  private SessionClassLoader getUpdatedSessionClassLoader(String database) {
    ClassLoader dbClassLoader = getDbResService().getClassLoader(database);
    if (dbClassLoader == null) {
      return null;
    }
    URL[] urls = new URL[0];
    if (persistInfo.getResources() != null) {
      int i = 0;
      urls = new URL[persistInfo.getResources().size()];
      for (LensSessionImpl.ResourceEntry res : persistInfo.getResources()) {
        try {
          urls[i++] = new URL(res.getUri());
        } catch (MalformedURLException e) {
          log.error("Invalid URL {} with location: {} adding to db {}", res.getUri(), res.getLocation(), database, e);
        }
      }
    }
    if (sessionDbClassLoaders.containsKey(database)
      && Arrays.equals(sessionDbClassLoaders.get(database).getURLs(), urls)) {
      return sessionDbClassLoaders.get(database);
    }
    return new SessionClassLoader(urls, dbClassLoader);
  }

  private void updateSessionDbClassLoader(String database) {
    SessionClassLoader updatedClassLoader = getUpdatedSessionClassLoader(database);
    if (updatedClassLoader != null) {
      sessionDbClassLoaders.put(database, updatedClassLoader);
    }
  }

  private void updateAllSessionClassLoaders() {
    synchronized (sessionDbClassLoaders) {
      // Update all DB class loaders
      for (String database: sessionDbClassLoaders.keySet()) {
        updateSessionDbClassLoader(database);
      }
    }
  }

  private void addResourceToClassLoader(String database, ResourceEntry res) {
    if (sessionDbClassLoaders.containsKey(database)) {
      SessionClassLoader sessionClassLoader = sessionDbClassLoaders.get(database);
      try {
        sessionClassLoader.addURL(new URL(res.getLocation()));
      } catch (MalformedURLException e) {
        log.error("Invalid URL {} with location: {} adding to db {}", res.getUri(), res.getLocation(), database, e);
      }
    }
  }
  private void addResourceToAllSessionClassLoaders(ResourceEntry res) {
    synchronized (sessionDbClassLoaders) {
      // Update all DB class loaders
      for (String database: sessionDbClassLoaders.keySet()) {
        addResourceToClassLoader(database, res);
      }
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
        ClassLoader classLoader = getDbResService().getClassLoader(database);
        if (classLoader == null) {
          log.debug("DB resource service gave null class loader for {}", database);
        } else {
          if (areResourcesAdded()) {
            log.debug("adding resources for {}", database);
            // We need to update DB specific classloader with added resources
            updateSessionDbClassLoader(database);
            classLoader = sessionDbClassLoaders.get(database);
          }
        }
        return classLoader == null ? getSessionState().getConf().getClassLoader() : classLoader;
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
   * @return db resources
   */
  public Collection<ResourceEntry> getDBResources(String database) {
    synchronized (failedDBResources) {
      List<ResourceEntry> failed = failedDBResources.get(database);
      if (failed == null && getDbResService().getResourcesForDatabase(database) != null) {
        failed = new ArrayList<>(getDbResService().getResourcesForDatabase(database));
        failedDBResources.put(database, failed);
      }
      return failed;
    }
  }


  /**
   * Get session's resources which have to be added for the given database
   */
  public Collection<ResourceEntry> getPendingSessionResourcesForDatabase(String database) {
    List<ResourceEntry> pendingResources = new ArrayList<>();
    for (ResourceEntry res : persistInfo.getResources()) {
      if (!res.isAddedToDatabase(database)) {
        pendingResources.add(res);
      }
    }
    return pendingResources;
  }

  /**
   * @return effective class loader for this session
   */
  public ClassLoader getClassLoader() {
    return getClassLoader(getCurrentDatabase());
  }

  public void markForClose() {
    log.info("Marking session {} for close. Operations on this session will be rejected", this);
    persistInfo.markedForClose = true;
  }

  /**
   * The Class ResourceEntry.
   */
  public static class ResourceEntry {

    /** The type. */
    @Getter
    final String type;

    @Getter
    final String uri;

    /** The final location. */
    @Getter
    String location;
    // For tests
    /** The restore count. */
    transient AtomicInteger restoreCount = new AtomicInteger();

    /** Set of databases for which this resource has been added */
    final transient Set<String> databases = new HashSet<>();

    /**
     * Instantiates a new resource entry.
     *
     * @param type     the type
     * @param uri the uri of resource
     */
    public ResourceEntry(String type, String uri) {
      this(type, uri, uri);
    }

    public ResourceEntry(String type, String uri, String location) {
      if (type == null || uri == null || location == null) {
        throw new NullPointerException("ResourceEntry type or uri or location cannot be null");
      }
      this.type = type.toUpperCase();
      this.uri = uri;
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
     * @return the value of restoreCount for the resource
     */
    public int getRestoreCount() {
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
    private List<ResourceEntry> resources = new ArrayList<>();

    /** The config. */
    private Map<String, String> config = new HashMap<>();

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

    /** Whether it's marked for close */
    private boolean markedForClose;

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
        out.writeUTF(resource.getUri());
      }

      out.writeInt(config.size());
      for (String key : config.keySet()) {
        out.writeUTF(key);
        out.writeUTF(config.get(key));
      }
      out.writeLong(lastAccessTime);
      out.writeBoolean(markedForClose);
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
        String uri = in.readUTF();
        resources.add(new ResourceEntry(type, uri));
      }

      config.clear();
      int cfgSize = in.readInt();
      for (int i = 0; i < cfgSize; i++) {
        String key = in.readUTF();
        String val = in.readUTF();
        config.put(key, val);
      }
      lastAccessTime = in.readLong();
      markedForClose = in.readBoolean();
    }
  }

  public void addToActiveQueries(QueryHandle queryHandle) {
    log.info("Adding {} to active queries for session {}", queryHandle, this);
    synchronized (this.activeQueries) {
      activeQueries.add(queryHandle);
    }
  }

  public void removeFromActiveQueries(QueryHandle queryHandle) {
    log.info("Removing {} from active queries for session {}", queryHandle, this);
    synchronized (this.activeQueries) {
      activeQueries.remove(queryHandle);
    }
  }

  public boolean activeOperationsPresent() {
    synchronized (this.activeQueries) {
      return !activeQueries.isEmpty();
    }
  }
}
