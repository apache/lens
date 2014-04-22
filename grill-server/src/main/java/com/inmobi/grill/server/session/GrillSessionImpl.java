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

import java.util.Map;

import javax.ws.rs.NotFoundException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionImpl;
import org.apache.hive.service.cli.thrift.TProtocolVersion;

import com.inmobi.grill.api.GrillException;

public class GrillSessionImpl extends HiveSessionImpl {
  
  private CubeMetastoreClient cubeClient;

  public GrillSessionImpl(TProtocolVersion protocol, String username, String password,
      HiveConf serverConf, Map<String, String> sessionConf, String ipAddress) {
    super(protocol, username, password, serverConf, sessionConf, ipAddress);
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
}
