package com.inmobi.grill.server.session;

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
      HiveConf serverConf, Map<String, String> sessionConf) {
    super(protocol, username, password, serverConf, sessionConf);
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
