package com.inmobi.grill.server.session;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import javax.ws.rs.NotFoundException;

import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.server.api.GrillConfConstants;

import com.inmobi.grill.server.util.UtilityMethods;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSessionImpl;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

import com.inmobi.grill.api.GrillException;

public class GrillSessionImpl extends HiveSessionImpl {
  public static final Log LOG = LogFactory.getLog(GrillSessionImpl.class);
  private CubeMetastoreClient cubeClient;
  private GrillSessionPersistInfo persistInfo = new GrillSessionPersistInfo();
  private long lastAccessTime = System.currentTimeMillis();
  private long sessionTimeout;
  private Configuration conf = createDefaultConf();

  private void initPersistInfo(SessionHandle sessionHandle, String username, String password, Map<String, String> sessionConf) {
    persistInfo.setSessionHandle(new GrillSessionHandle(sessionHandle.getHandleIdentifier().getPublicId(),
      sessionHandle.getHandleIdentifier().getSecretId()));
    persistInfo.setUsername(username);
    persistInfo.setPassword(password);
    persistInfo.setLastAccessTime(lastAccessTime);
    persistInfo.setSessionConf(sessionConf);
  }

  public static Configuration createDefaultConf() {
    Configuration conf = new Configuration(false);
    conf.addResource("grillsession-default.xml");
    conf.addResource("grill-site.xml");
    return conf;
  }

  public static Map<String, String> DEFAULT_HIVE_SESSION_CONF = getHiveSessionConf();

  public static Map<String, String> getHiveSessionConf() {
    Configuration defaultConf = createDefaultConf();
    return defaultConf.getValByRegex("hive.*");
  }

  public GrillSessionImpl(TProtocolVersion protocol, String username, String password,
      HiveConf serverConf, Map<String, String> sessionConf, String ipAddress) {
    super(protocol, username, password, serverConf, sessionConf, ipAddress);
    initPersistInfo(getSessionHandle(), username, password, sessionConf);
    sessionTimeout = 1000 * serverConf.getLong(GrillConfConstants.GRILL_SESSION_TIMEOUT_SECONDS,
      GrillConfConstants.GRILL_SESSION_TIMEOUT_SECONDS_DEFAULT);
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
   * Constructor used when restoring session
   */
  public GrillSessionImpl(SessionHandle sessionHandle, TProtocolVersion protocol, String username, String password,
                          HiveConf serverConf, Map<String, String> sessionConf, String ipAddress) {
    super(sessionHandle, protocol, username, password, serverConf, sessionConf, ipAddress);
    initPersistInfo(getSessionHandle(), username, password, sessionConf);
    sessionTimeout = 1000 * serverConf.getLong(GrillConfConstants.GRILL_SESSION_TIMEOUT_SECONDS,
      GrillConfConstants.GRILL_SESSION_TIMEOUT_SECONDS_DEFAULT);
  }


  public CubeMetastoreClient getCubeMetastoreClient() throws GrillException {
    if (cubeClient == null) {
      try {
        cubeClient = CubeMetastoreClient.getInstance(getHiveConf());
      } catch (HiveException e) {
        throw new GrillException(e);
      }
    }
    return cubeClient;
  }

  public synchronized void acquire() {
    try {
      super.acquire();
    } catch (HiveSQLException e) {
      throw new NotFoundException("Could not acquire the session", e);
    }
  }

  public synchronized void release() {
    lastAccessTime = System.currentTimeMillis();
    super.release();
  }

  public boolean isActive() {
    long inactiveAge = System.currentTimeMillis() - lastAccessTime;
    return inactiveAge < sessionTimeout;
  }

  public void setConfig(String key, String value) {
    persistInfo.getConfig().put(key, value);
  }

  public void removeResource(String type, String path) {
    Iterator<ResourceEntry> itr = persistInfo.getResources().iterator();
    while (itr.hasNext()) {
      ResourceEntry res = itr.next();
      if (res.getType().equals(type) && res.getLocation().equals(path)) {
        itr.remove();
      }
    }
  }

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

  @Override
  public String toString() {
    return getSessionHandle().getHandleIdentifier().toString();
  }

  public String getLoggedInUser() {
    return getHiveConf().get(GrillConfConstants.GRILL_SESSION_LOGGEDIN_USER);
  }

  public String getClusterUser() {
    return getUserName();
  }

  public GrillSessionPersistInfo getGrillSessionPersistInfo() {
    return persistInfo;
  }

  void setLastAccessTime(long lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public static class ResourceEntry {
    final String type;
    final String location;
    // For tests
    transient int restoreCount;

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

    public void restoredResource() {
      restoreCount++;
    }

    public int getRestoreCount() {
      return restoreCount;
    }

    @Override
    public String toString() {
      return "type=" + type + " path=" + location;
    }
  }

  public static class GrillSessionPersistInfo implements Externalizable {
    private List<ResourceEntry> resources = new ArrayList<ResourceEntry>();
    private Map<String, String> config = new HashMap<String, String>();
    private GrillSessionHandle sessionHandle;
    private String database;
    private String username;
    private String password;
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

    public GrillSessionHandle getSessionHandle() {
      return sessionHandle;
    }

    public void setSessionHandle(GrillSessionHandle sessionHandle) {
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

    public void setSessionConf(Map<String,String> sessionConf) {
      UtilityMethods.mergeMaps(config, sessionConf, true);
    }

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

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      sessionHandle = GrillSessionHandle.valueOf(in.readUTF());
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
