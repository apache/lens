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

import javax.ws.rs.NotFoundException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSessionImpl;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.lens.api.LensException;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.util.UtilityMethods;

/**
 * The Class LensSessionImpl.
 */
public class LensSessionImpl extends HiveSessionImpl {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(LensSessionImpl.class);

  /** The cube client. */
  private CubeMetastoreClient cubeClient;

  /** The persist info. */
  private LensSessionPersistInfo persistInfo = new LensSessionPersistInfo();

  /** The last access time. */
  private long lastAccessTime = System.currentTimeMillis();

  /** The session timeout. */
  private long sessionTimeout;

  /** The conf. */
  private Configuration conf = createDefaultConf();

  /**
   * Inits the persist info.
   *
   * @param sessionHandle
   *          the session handle
   * @param username
   *          the username
   * @param password
   *          the password
   * @param sessionConf
   *          the session conf
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

  /**
   * Creates the default conf.
   *
   * @return the configuration
   */
  public static Configuration createDefaultConf() {
    Configuration conf = new Configuration(false);
    conf.addResource("lenssession-default.xml");
    conf.addResource("lens-site.xml");
    return conf;
  }

  /** The default hive session conf. */
  public static Map<String, String> DEFAULT_HIVE_SESSION_CONF = getHiveSessionConf();

  public static Map<String, String> getHiveSessionConf() {
    Configuration defaultConf = createDefaultConf();
    return defaultConf.getValByRegex("hive.*");
  }

  /**
   * Instantiates a new lens session impl.
   *
   * @param protocol
   *          the protocol
   * @param username
   *          the username
   * @param password
   *          the password
   * @param serverConf
   *          the server conf
   * @param sessionConf
   *          the session conf
   * @param ipAddress
   *          the ip address
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
   * @param sessionHandle
   *          the session handle
   * @param protocol
   *          the protocol
   * @param username
   *          the username
   * @param password
   *          the password
   * @param serverConf
   *          the server conf
   * @param sessionConf
   *          the session conf
   * @param ipAddress
   *          the ip address
   */
  public LensSessionImpl(SessionHandle sessionHandle, TProtocolVersion protocol, String username, String password,
      HiveConf serverConf, Map<String, String> sessionConf, String ipAddress) {
    super(sessionHandle, protocol, username, password, serverConf, sessionConf, ipAddress);
    initPersistInfo(getSessionHandle(), username, password, sessionConf);
    sessionTimeout = 1000 * serverConf.getLong(LensConfConstants.SESSION_TIMEOUT_SECONDS,
        LensConfConstants.SESSION_TIMEOUT_SECONDS_DEFAULT);
  }

  public CubeMetastoreClient getCubeMetastoreClient() throws LensException {
    if (cubeClient == null) {
      try {
        cubeClient = CubeMetastoreClient.getInstance(getHiveConf());
      } catch (HiveException e) {
        throw new LensException(e);
      }
    }
    return cubeClient;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hive.service.cli.session.HiveSessionImpl#acquire()
   */
  public synchronized void acquire() {
    try {
      super.acquire();
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
   * @param key
   *          the key
   * @param value
   *          the value
   */
  public void setConfig(String key, String value) {
    persistInfo.getConfig().put(key, value);
  }

  /**
   * Removes the resource.
   *
   * @param type
   *          the type
   * @param path
   *          the path
   */
  public void removeResource(String type, String path) {
    Iterator<ResourceEntry> itr = persistInfo.getResources().iterator();
    while (itr.hasNext()) {
      ResourceEntry res = itr.next();
      if (res.getType().equals(type) && res.getLocation().equals(path)) {
        itr.remove();
      }
    }
  }

  /**
   * Adds the resource.
   *
   * @param type
   *          the type
   * @param path
   *          the path
   */
  public void addResource(String type, String path) {
    persistInfo.getResources().add(new ResourceEntry(type, path));
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
   * The Class ResourceEntry.
   */
  public static class ResourceEntry {

    /** The type. */
    final String type;

    /** The location. */
    final String location;
    // For tests
    /** The restore count. */
    transient int restoreCount;

    /**
     * Instantiates a new resource entry.
     *
     * @param type
     *          the type
     * @param location
     *          the location
     */
    public ResourceEntry(String type, String location) {
      if (type == null || location == null) {
        throw new NullPointerException("ResourceEntry type or location cannot be null");
      }
      this.type = type;
      this.location = location;
    }

    public String getLocation() {
      return location;
    }

    public String getType() {
      return type;
    }

    /**
     * Restored resource.
     */
    public void restoredResource() {
      restoreCount++;
    }

    public int getRestoreCount() {
      return restoreCount;
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
  }

  /**
   * The Class LensSessionPersistInfo.
   */
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

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public List<ResourceEntry> getResources() {
      return resources;
    }

    public void setResources(List<ResourceEntry> resources) {
      this.resources = resources;
    }

    public Map<String, String> getConfig() {
      return config;
    }

    public void setConfig(Map<String, String> config) {
      this.config = config;
    }

    public String getDatabase() {
      return database;
    }

    public void setDatabase(String database) {
      this.database = database;
    }

    public LensSessionHandle getSessionHandle() {
      return sessionHandle;
    }

    public void setSessionHandle(LensSessionHandle sessionHandle) {
      this.sessionHandle = sessionHandle;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public void setLastAccessTime(long accessTime) {
      lastAccessTime = accessTime;
    }

    public long getLastAccessTime() {
      return lastAccessTime;
    }

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
