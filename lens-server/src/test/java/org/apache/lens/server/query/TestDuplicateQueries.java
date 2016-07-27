/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.query;

import static org.testng.Assert.*;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.driver.hive.HiveDriver;
import org.apache.lens.server.LensJerseyTest;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.apache.lens.server.common.RestAPITestUtil;
import org.apache.lens.server.common.TestResourceFile;
import org.apache.lens.server.query.TestQueryService.QueryServiceTestApp;

import org.apache.hadoop.hive.conf.HiveConf;

import org.glassfish.jersey.test.TestProperties;

import org.testng.annotations.*;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

@Test(groups = "duplicate-query", dependsOnGroups = "two-working-drivers")
@Slf4j
public class TestDuplicateQueries extends LensJerseyTest {
  /**
   * The test table.
   */
  private static final String TEST_TABLE = "TEST_TABLE_DUPLICATE_QUERIES";
  /**
   * The query service.
   */
  private QueryExecutionServiceImpl queryService;

  /**
   * The lens session id.
   */
  private LensSessionHandle lensSessionId;

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  private HiveConf getServerConfLocal() {
    HiveConf serverConf = new HiveConf(super.getServerConf());
    serverConf.setBoolean(LensConfConstants.SERVER_DUPLICATE_QUERY_ALLOWED, false);
    return serverConf;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.glassfish.jersey.test.JerseyTest#tearDown()
   */
  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected Application configure() {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    return new QueryServiceTestApp();
  }

  /**
   * Checks duplicate query handle. In the starting of test, the lens-server is
   * started with the new configuration.
   *
   * @throws Exception
   */
  @Test
  public void testExecuteAsyncDuplicate() throws Exception {
    try {
      MediaType mt = defaultMT;
      log.info("Restarting lens server!");
      //restart with overridden conf
      restartLensServer(getServerConfLocal(), false);
      log.info("Restarted lens server!");

      queryService = LensServices.get().getService(QueryExecutionService.NAME);
      Map<String, String> sessionconf = new HashMap<>();
      sessionconf.put("test.session.key", "svalue");
      lensSessionId = queryService.openSession("foo", "bar", sessionconf);
      LensServerTestUtil.createTable(TEST_TABLE, target(), lensSessionId, defaultMT);
      LensServerTestUtil
          .loadDataFromClasspath(TEST_TABLE, TestResourceFile.TEST_DATA2_FILE.getValue(), target(), lensSessionId,
              defaultMT);
      queryService.pauseQuerySubmitter(true);
      String query = "select ID, IDSTR from " + TEST_TABLE;

      final QueryHandle handle1 = RestAPITestUtil
          .executeAndGetHandle(target(), Optional.of(lensSessionId), Optional.of(query), Optional.of(new LensConf()),
              mt);
      final QueryHandle handle2 = RestAPITestUtil
          .executeAndGetHandle(target(), Optional.of(lensSessionId), Optional.of(query), Optional.of(new LensConf()),
              mt);
      assertNotNull(handle1);
      assertNotNull(handle2);
      assertEquals(handle1, handle2);
      // Cancel the query
      queryService.cancelQuery(lensSessionId, handle1);

      // Create a different query
      final QueryHandle handle3 = RestAPITestUtil
          .executeAndGetHandle(target(), Optional.of(lensSessionId), Optional.of(query), Optional.of(new LensConf()),
              mt);
      assertNotNull(handle3);
      queryService.cancelQuery(lensSessionId, handle3);
      // After completion If we launch the same query it should return a new
      // handle.
      final QueryHandle handle4 = RestAPITestUtil
          .executeAndGetHandle(target(), Optional.of(lensSessionId), Optional.of(query), Optional.of(new LensConf()),
              mt);
      assertNotEquals(handle4, handle3);
      queryService.cancelQuery(lensSessionId, handle4);

      // Launch the query in different session should result in the different
      // handle.

      final QueryHandle handle5 = RestAPITestUtil
          .executeAndGetHandle(target(), Optional.of(lensSessionId), Optional.of(query), Optional.of(new LensConf()),
              mt);
      sessionconf = new HashMap<>();
      sessionconf.put("test.session.key", "svalue");

      LensSessionHandle lensSessionId1 = queryService.openSession("foo@localhost", "bar", sessionconf);

      final QueryHandle handle6 = RestAPITestUtil
          .executeAndGetHandle(target(), Optional.of(lensSessionId1), Optional.of(query), Optional.of(new LensConf()),
              mt);
      assertNotNull(handle5);
      assertNotNull(handle6);
      assertNotEquals(handle5, handle6);
      queryService.cancelQuery(lensSessionId, handle5);
      queryService.cancelQuery(lensSessionId1, handle6);
      queryService.closeSession(lensSessionId1);

      // Different conf should different handle
      LensConf conf = new LensConf();
      final QueryHandle handle7 = RestAPITestUtil
          .executeAndGetHandle(target(), Optional.of(lensSessionId), Optional.of(query), Optional.of(conf), mt);
      // Add a property
      conf.addProperty("test", "test");
      final QueryHandle handle8 = RestAPITestUtil
          .executeAndGetHandle(target(), Optional.of(lensSessionId), Optional.of(query), Optional.of(conf), mt);
      assertNotNull(handle7);
      assertNotNull(handle8);
      assertNotEquals(handle7, handle8);
      queryService.cancelQuery(lensSessionId, handle7);
      queryService.cancelQuery(lensSessionId, handle8);
    } finally {
      // restart server with correct configuration
      queryService.pauseQuerySubmitter(false);
      LensServerTestUtil.dropTable(TEST_TABLE, target(), lensSessionId, defaultMT);
      queryService.closeSession(lensSessionId);
      for (LensDriver driver : queryService.getDrivers()) {
        if (driver instanceof HiveDriver) {
          assertFalse(((HiveDriver) driver).hasLensSession(lensSessionId));
        }
      }
      log.info("Restarting lens server!");
      // restart without overwrites
      restartLensServer();
      log.info("Restarted lens server!");
    }
  }
}
