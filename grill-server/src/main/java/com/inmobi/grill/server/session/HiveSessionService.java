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

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.GrillSessionHandle;
import com.inmobi.grill.server.GrillService;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.thrift.THandleIdentifier;
import org.apache.hive.service.cli.thrift.TSessionHandle;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.UUID;

public class HiveSessionService extends GrillService {
  public static final Log LOG = LogFactory.getLog(HiveSessionService.class);
  public static final String NAME = "session";

  public HiveSessionService(CLIService cliService) {
    super(NAME, cliService);
  }

  public void addResource(GrillSessionHandle sessionid, String type, String path) {
    String command = "add " + type.toLowerCase() + " " + path;
    try {
      acquire(sessionid);
      getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
      getSession(sessionid).addResource(type, path);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    } finally {
      try {
        release(sessionid);
      } catch (GrillException e) {
        throw new WebApplicationException(e);
      }
    }
  }


  public void deleteResource(GrillSessionHandle sessionid, String type, String path) {
    String command = "delete " + type.toLowerCase() + " " + path;
    try {
      acquire(sessionid);
      getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
      getSession(sessionid).removeResource(type, path);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    } finally {
      try {
        release(sessionid);
      } catch (GrillException e) {
        throw new WebApplicationException(e);
      }
    }
  }

  public OperationHandle getAllSessionParameters(GrillSessionHandle sessionid,
      boolean verbose, String key) throws GrillException, HiveSQLException {
    String command = "set";
    if (verbose) {
      command += " -v ";
    }
    if (!StringUtils.isBlank(key)) {
      command += " " + key;
    }
    OperationHandle handle;
    try {
      acquire(sessionid);
      handle = getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
    } finally {
      try {
        release(sessionid);
      } catch (GrillException e) {
        throw new WebApplicationException(e);
      }
    }
    return handle;
  }

  public void setSessionParameter(GrillSessionHandle sessionid, String key, String value) {
    String command = "set" + " " + key + "= " + value;
    try {
      acquire(sessionid);
      getCliService().executeStatement(getHiveSessionHandle(sessionid), command, null);
      getSession(sessionid).setConfig(key, value);
    } catch (HiveSQLException e) {
      throw new WebApplicationException(e);
    } catch (GrillException e) {
      throw new WebApplicationException(e);
    } finally {
      try {
        release(sessionid);
      } catch (GrillException e) {
        throw new WebApplicationException(e);
      }
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // Write out all the sessions
    out.writeInt(sessionMap.size());
    for (GrillSessionHandle sessionHandle : sessionMap.values()) {
      out.writeUTF(sessionHandle.toString());
      GrillSessionImpl session = null;
      try {
        session = getSession(sessionHandle);
      } catch (GrillException e) {
        throw new IOException(e);
      }
      out.writeUTF(session.getUsername());
      out.writeUTF(session.getPassword());
      session.writeExternal(out);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int numSessions = in.readInt();
    for (int i = 0; i < numSessions; i++) {
      GrillSessionHandle sessionHandle = GrillSessionHandle.valueOf(in.readUTF());
      String userName = in.readUTF();
      String password = in.readUTF();
      try {
        restoreSession(sessionHandle, userName, password);
        GrillSessionImpl session = getSession(sessionHandle);
        session.readExternal(in);
      } catch (GrillException e) {
        throw new IOException(e);
      }
    }
  }

}
