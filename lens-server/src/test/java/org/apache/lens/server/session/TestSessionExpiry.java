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

import static org.testng.Assert.*;

import java.util.HashMap;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.metrics.MetricsService;

import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hive.service.cli.CLIService;

import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestSessionExpiry.
 */
@Test(groups = "unit-test")
@Slf4j
public class TestSessionExpiry {

  /**
   * Test session expiry.
   *
   * @throws Exception the exception
   */
  public void testSessionExpiry() throws Exception {
    HiveConf conf = LensServerConf.createHiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME, LensSessionImpl.class.getName());
    conf.setLong(LensConfConstants.SESSION_TIMEOUT_SECONDS, 1L);
    CLIService cliService = new CLIService(null);
    cliService.init(conf);
    HiveSessionService lensService = new HiveSessionService(cliService);
    lensService.init(conf);
    lensService.start();
    MetricsService metricSvc = LensServices.get().getService(MetricsService.NAME);

    try {
      LensSessionHandle sessionHandle = lensService.openSession("foo", "bar", new HashMap<String, String>());
      LensSessionImpl session = lensService.getSession(sessionHandle);
      assertTrue(session.isActive());
      session.setLastAccessTime(session.getLastAccessTime() - 2000
        * conf.getLong(LensConfConstants.SESSION_TIMEOUT_SECONDS, LensConfConstants.SESSION_TIMEOUT_SECONDS_DEFAULT));
      assertFalse(session.isActive());
      // run the expiry thread
      lensService.getSessionExpiryRunnable().run();
      log.info("Keeping a sleep of 3 seconds to make sure SessionExpiryService gets enough time to close"
          + " inactive sessions");
      Thread.sleep(3000);
      assertTrue(metricSvc.getTotalExpiredSessions() >= 1);
      assertTrue(metricSvc.getTotalClosedSessions() >= 1);

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

  public void testSessionExpiryInterval() throws Exception {
    HiveConf conf = LensServerConf.createHiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME, LensSessionImpl.class.getName());
    conf.setLong(LensConfConstants.SESSION_TIMEOUT_SECONDS, 1L);
    conf.setInt(LensConfConstants.SESSION_EXPIRY_SERVICE_INTERVAL_IN_SECS, 1);
    CLIService cliService = new CLIService(null);
    cliService.init(conf);
    HiveSessionService lensService = new HiveSessionService(cliService);
    lensService.init(conf);
    lensService.start();
    MetricsService metricSvc = LensServices.get().getService(MetricsService.NAME);
    try {
      LensSessionHandle sessionHandle = lensService.openSession("foo", "bar", new HashMap<String, String>());
      LensSessionImpl session = lensService.getSession(sessionHandle);
      assertTrue(session.isActive());
      session.setLastAccessTime(session.getLastAccessTime() - 2000
        * conf.getLong(LensConfConstants.SESSION_TIMEOUT_SECONDS, LensConfConstants.SESSION_TIMEOUT_SECONDS_DEFAULT));
      assertFalse(session.isActive());
      log.info("Keeping a sleep of 3 seconds to make sure SessionExpiryService is running and gets enough time to"
          + "close inactive sessions");
      Thread.sleep(3000);
      assertTrue(metricSvc.getTotalExpiredSessions() >= 1);
      assertTrue(metricSvc.getTotalClosedSessions() >= 1);

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
