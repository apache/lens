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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.session.LensSessionImpl;
import org.apache.lens.server.session.HiveSessionService;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * The Class TestSessionExpiry.
 */
@Test(groups = "unit-test")
public class TestSessionExpiry {

  /**
   * Test session expiry.
   *
   * @throws Exception
   *           the exception
   */
  public void testSessionExpiry() throws Exception {
    HiveConf conf = LensServerConf.get();
    conf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME, LensSessionImpl.class.getName());
    conf.setLong(LensConfConstants.SESSION_TIMEOUT_SECONDS, 1L);
    CLIService cliService = new CLIService();
    cliService.init(conf);
    HiveSessionService lensService = new HiveSessionService(cliService);
    lensService.init(conf);
    lensService.start();

    try {
      LensSessionHandle sessionHandle = lensService.openSession("foo", "bar", new HashMap<String, String>());
      LensSessionImpl session = lensService.getSession(sessionHandle);
      assertTrue(session.isActive());
      session.setLastAccessTime(session.getLastAccessTime() - 2000
          * conf.getLong(LensConfConstants.SESSION_TIMEOUT_SECONDS, LensConfConstants.SESSION_TIMEOUT_SECONDS_DEFAULT));
      assertFalse(session.isActive());

      // run the expiry thread
      lensService.getSessionExpiryRunnable().run();
      try {
        lensService.getSession(sessionHandle);
        // should throw exception since session should be expired by now
        fail("Expected get session to fail for session " + sessionHandle.getPublicId());
      } catch (Exception e) {
        // pass
      }
    } finally {
      lensService.stop();
    }
  }
}
