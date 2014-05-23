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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSessionImpl;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

import com.inmobi.grill.api.GrillException;

public class GrillSessionImpl extends HiveSessionImpl implements Externalizable {
  
  private CubeMetastoreClient cubeClient;
  List<ResourceEntry> resources = new ArrayList<ResourceEntry>();
  private Map<String, String> config = new HashMap<String, String>();

  public GrillSessionImpl(TProtocolVersion protocol, String username, String password,
      HiveConf serverConf, Map<String, String> sessionConf, String ipAddress) {
    super(protocol, username, password, serverConf, sessionConf, ipAddress);
    config.putAll(sessionConf);
  }

  /**
   * Constructor used when restoring session
   */
  public GrillSessionImpl(SessionHandle sessionHandle, TProtocolVersion protocol, String username, String password,
                          HiveConf serverConf, Map<String, String> sessionConf, String ipAddress) {
    super(sessionHandle, protocol, username, password, serverConf, sessionConf, ipAddress);
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
    super.release();
  }

  @Override
  public void writeExternal(ObjectOutput objectOutput) throws IOException {
    // Write resources
    objectOutput.writeInt(resources.size());
    for (ResourceEntry res : resources) {
      objectOutput.writeUTF(res.getType());
      objectOutput.writeUTF(res.getLocation());
    }

    // Write config
    objectOutput.writeInt(config.size());
    for (Map.Entry<String, String> entry : config.entrySet()) {
      objectOutput.writeUTF(entry.getKey());
      objectOutput.writeUTF(entry.getValue());
    }
  }

  @Override
  public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
    int numRes = objectInput.readInt();
    for (int i = 0; i < numRes; i++) {
      String type = objectInput.readUTF();
      String path = objectInput.readUTF();
      resources.add(new ResourceEntry(type, path));
    }

    int numConfig = objectInput.readInt();
    for (int i = 0; i < numConfig; i++) {
      String key = objectInput.readUTF();
      String value = objectInput.readUTF();
      config.put(key, value);
    }
  }

  public void setConfig(String key, String value) {
    config.put(key, value);
  }

  public void removeResource(String type, String path) {
    Iterator<ResourceEntry> itr = resources.iterator();
    while (itr.hasNext()) {
      ResourceEntry res = itr.next();
      if (res.getType().equals(type) && res.getLocation().equals(path)) {
        itr.remove();
      }
    }
  }

  public void addResource(String type, String path) {
    resources.add(new ResourceEntry(type, path));
  }

  public class ResourceEntry {
    final String type;
    final String location;

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
  }
}
