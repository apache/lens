/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server;

import java.util.HashMap;

import javax.ws.rs.core.Application;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.error.LensServerErrorCode;
import org.apache.lens.server.query.TestQueryService;
import org.apache.lens.server.session.HiveSessionService;

import org.testng.Assert;
import org.testng.annotations.Test;


import lombok.extern.slf4j.Slf4j;

/**
 * The Class TestBaseLensService.
 */
@Test(groups = "unit-test")
@Slf4j
public class TestBaseLensService extends LensJerseyTest {

  @Override
  protected Application configure() {
    return new TestQueryService.QueryServiceTestApp();
  }


  /**
   * Test lens server session validator.
   *
   * @throws Exception the exception
   */
  @Test
  public void testLensServerValidateSession() throws Exception {

    HiveSessionService sessionService = LensServices.get().getService(HiveSessionService.NAME);

    LensSessionHandle validSession = sessionService.openSession("foo@localhost", "bar",
            new HashMap<String, String>());

    LensSessionHandle invalidSession = sessionService.openSession("foo@localhost", "bar",
            new HashMap<String, String>());
    sessionService.closeSession(invalidSession);

    LensSessionHandle notInsertedSession = sessionService.openSession("foo@localhost", "bar",
            new HashMap<String, String>());


    sessionService.validateSession(validSession);
    sessionService.validateSession(notInsertedSession);
    try {
      sessionService.validateSession(invalidSession);
    } catch (LensException exp) {
      Assert.assertEquals(exp.getErrorCode(), LensServerErrorCode.SESSION_CLOSED.getLensErrorInfo().getErrorCode());
    }

    sessionService.closeSession(validSession);
    sessionService.closeSession(notInsertedSession);
  }
}
