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
package org.apache.lens.driver.hive;

import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.RetryingThriftCLIServiceClient;

import lombok.extern.slf4j.Slf4j;

/**
 * Connect to a remote Hive Server 2 service to run driver queries.
 */
@Slf4j
public class RemoteThriftConnection implements ThriftConnection {

  /** The connected. */
  private boolean connected;

  /** The hs2 client. */
  private RetryingThriftCLIServiceClient.CLIServiceClientWrapper hs2Client;

  private HiveConf conf;

  /**
   * Instantiates a new remote thrift connection.
   */
  public RemoteThriftConnection() {

  }

  public void init(HiveConf conf, String user) {
    // new HiveConf() is getting created because connection will be different for each user
    this.conf = new HiveConf(conf);
    this.conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_USER, user);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.driver.hive.ThriftConnection#getClient(org.apache.hadoop.hive.conf.HiveConf)
   */
  @Override
  public CLIServiceClient getClient() throws LensException {
    if (!connected) {
      try {
        log.info("HiveDriver connecting to HiveServer @ {}:{}",
          conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST),
          conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT));
        hs2Client = RetryingThriftCLIServiceClient.newRetryingCLIServiceClient(conf);
        log.info("HiveDriver connected to HiveServer @ {}:{}",
          conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST),
          conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT));

      } catch (HiveSQLException e) {
        throw new LensException(e);
      }
      connected = true;
    }
    return hs2Client;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() {
    connected = false;
    if (hs2Client != null) {
      hs2Client.closeTransport();
    }
  }
}
