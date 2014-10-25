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

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.lens.api.LensException;

/**
 * The Class EmbeddedThriftConnection.
 */
public class EmbeddedThriftConnection implements ThriftConnection {

  /** The client. */
  private ThriftCLIServiceClient client;

  /** The connected. */
  private boolean connected;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.driver.hive.ThriftConnection#getClient(org.apache.hadoop.hive.conf.HiveConf)
   */
  @Override
  public ThriftCLIServiceClient getClient(HiveConf conf) throws LensException {
    if (!connected) {
      client = new ThriftCLIServiceClient(new EmbeddedThriftBinaryCLIService());
      connected = true;
    }
    return client;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    // Does nothing
  }
}
