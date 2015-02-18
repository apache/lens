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
package org.apache.lens.server;

import java.io.IOException;

import org.apache.lens.server.api.LensConfConstants;

import org.apache.hadoop.hive.conf.HiveConf;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestLensServer {

  /**
   * Test UI server
   */
  @Test
  public void testUIServer() throws IOException {
    HiveConf conf = new HiveConf(LensServerConf.get());
    LensServer thisServer = LensServer.createLensServer(conf);
    Assert.assertEquals(thisServer.getServerList().size(), 2);

    conf.set(LensConfConstants.SERVER_UI_ENABLE, "false");
    thisServer = LensServer.createLensServer(conf);
    Assert.assertEquals(thisServer.getServerList().size(), 1);
  }
}
