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

package org.apache.lens.regression.core.helpers;

import java.io.IOException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.error.LensException;

import com.jcraft.jsch.JSchException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LensServerHelper extends ServiceManagerHelper {

  public LensServerHelper() {
  }

  public LensServerHelper(String envFileName) {
    super(envFileName);
  }

  /**
   * Restart Lens server
   */

  public void restart() throws JSchException, IOException, InterruptedException, LensException {
    int counter = 0;
    Util.runRemoteCommand("bash /usr/local/lens/server/bin/lens-ctl stop");
    Util.runRemoteCommand("bash /usr/local/lens/server/bin/lens-ctl start");

    Response response = this.exec("get", "", servLens, null, null, MediaType.TEXT_PLAIN_TYPE, MediaType.TEXT_PLAIN);
    while (response == null && counter < 40) {
      log.info("Waiting for Lens server to come up ");
      Thread.sleep(1000);
      response = this.exec("get", "", servLens, null, null, MediaType.TEXT_PLAIN_TYPE, MediaType.TEXT_PLAIN);
      counter++;
    }

    AssertUtil.assertSucceededResponse(response);
  }

  public void stop() throws JSchException, IOException, InterruptedException, LensException {
    int counter = 0;
    Util.runRemoteCommand("bash /usr/local/lens/server/bin/lens-ctl stop");
  }

  public void start() throws JSchException, IOException, InterruptedException, LensException {
    int counter = 0;
    Util.runRemoteCommand("bash /usr/local/lens/server/bin/lens-ctl start");

    Response response = this.exec("get", "", servLens, null, null, MediaType.TEXT_PLAIN_TYPE, MediaType.TEXT_PLAIN);
    while (response == null && counter < 40) {
      log.info("Waiting for Lens server to come up ");
      Thread.sleep(1000);
      response = this.exec("get", "", servLens, null, null, MediaType.TEXT_PLAIN_TYPE, MediaType.TEXT_PLAIN);
      counter++;
    }

    AssertUtil.assertSucceededResponse(response);
  }
}

