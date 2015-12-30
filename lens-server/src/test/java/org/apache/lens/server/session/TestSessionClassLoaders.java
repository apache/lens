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


import java.io.File;
import java.net.URLClassLoader;
import java.util.HashMap;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.user.UserConfigLoaderFactory;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hive.service.cli.CLIService;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestSessionClassLoaders {

  private final HiveConf conf = LensServerConf.createHiveConf();
  private HiveSessionService sessionService;

  private static final String DB1 = TestSessionClassLoaders.class.getSimpleName() + "_db1";

  @BeforeClass
  public void setup() throws Exception {
    /**
     * Test Setup -
     * Static test.jar containing ClassLoaderTestClass.class attached to DB1
     * No static jar attached to 'default' DB
     * test2.jar containing ClassLoaderTestClass2.class added to session via addResource
     */
    // Create test databases and tables
    LensServerTestUtil.createTestDatabaseResources(new String[]{DB1}, conf);

    conf.setVar(HiveConf.ConfVars.HIVE_SESSION_IMPL_CLASSNAME, LensSessionImpl.class.getName());
    conf.set(LensConfConstants.DATABASE_RESOURCE_DIR, "target/resources");

    CLIService cliService = new CLIService();
    cliService.init(conf);

    sessionService = new HiveSessionService(cliService);
    sessionService.init(conf);

    UserConfigLoaderFactory.init(conf);

    cliService.start();
    sessionService.start();
  }


  @AfterClass
  public void tearDown() throws Exception {
    Hive hive = Hive.get(conf);
    hive.dropDatabase(DB1, true, true);
  }


  /**
   * Check that DB specific classlaoders are available
   * @throws Exception
   */
  @Test
  public void testSessionClassLoader() throws Exception {
    LensSessionHandle sessionHandle = sessionService.openSession("foo", "bar", new HashMap<String, String>());
    LensSessionImpl session = sessionService.getSession(sessionHandle);
    session.setDbResService(sessionService.getDatabaseResourceService());
    // Loading test class should fail for default database.
    session.setCurrentDatabase("default");
    try {
      // Acquire should set classloader in current thread
      sessionService.acquire(sessionHandle);
      Class clz = Class.forName(TestDatabaseResourceService.TEST_CLASS, true,
        Thread.currentThread().getContextClassLoader());
      // Expected to fail
      Assert.fail("Should not reach here as default db doesn't have jar loaded");
    } catch (ClassNotFoundException cnf) {
      // Pass
    } finally {
      sessionService.release(sessionHandle);
    }

    // test cube metastore client's configuration's classloader
    try {
      // Acquire should set classloader in current thread
      sessionService.acquire(sessionHandle);
      Class clz = session.getCubeMetastoreClient().getConf().getClassByName(TestDatabaseResourceService.TEST_CLASS);
      // Expected to fail
      Assert.fail("Should not reach here as default db doesn't have jar loaded");
    } catch (ClassNotFoundException cnf) {
      // Pass
    } finally {
      sessionService.release(sessionHandle);
    }

    log.info("@@ Teting DB2");

    // Loading test class should pass for DB1
    session.setCurrentDatabase(DB1);
    try {
      sessionService.acquire(sessionHandle);

      ClassLoader thClassLoader = Thread.currentThread().getContextClassLoader();
      Assert.assertTrue(thClassLoader == session.getClassLoader(DB1));

      Class clz = Class.forName(TestDatabaseResourceService.TEST_CLASS, true, thClassLoader);
      Assert.assertNotNull(clz);
      // test cube metastore client's configuration's classloader
      clz = null;
      clz = session.getCubeMetastoreClient().getConf().getClassByName(TestDatabaseResourceService.TEST_CLASS);
      Assert.assertNotNull(clz);
    } catch (ClassNotFoundException cnf) {
      log.error(cnf.getMessage(), cnf);
      Assert.fail("Should not have thrown class not found exception: " + cnf.getMessage());
    } finally {
      sessionService.release(sessionHandle);
    }
    sessionService.closeSession(sessionHandle);
  }

  /**
   * Check that any added resources to the session are available after database is switched
   * @throws Exception
   */
  @Test
  public void testClassLoaderMergeAfterAddResources() throws Exception {
    LensSessionHandle sessionHandle = sessionService.openSession("foo", "bar", new HashMap<String, String>());
    LensSessionImpl session = sessionService.getSession(sessionHandle);
    session.setDbResService(sessionService.getDatabaseResourceService());

    File sessionJar = new File("target/testjars/test2.jar");

    String sessionJarLocation = "file://" + sessionJar.getAbsolutePath();

    session.setCurrentDatabase("default");
    sessionService.addResource(sessionHandle, "jar", sessionJarLocation);
    session.addResource("jar", sessionJarLocation);
    session.setCurrentDatabase("default");

    boolean loadedSessionClass = false;
    boolean loadedDBClass = false;
    try {
      log.info("@@@ TEST 1");
      sessionService.acquire(sessionHandle);

      ClassLoader dbClassLoader = session.getClassLoader("default");
      Assert.assertTrue(Thread.currentThread().getContextClassLoader() == dbClassLoader);

      // testClass2 should be loaded since test2.jar is added to the session
      Class testClass2 = dbClassLoader.loadClass("ClassLoaderTestClass2");
      //Class testClass2 = Class.forName("ClassLoaderTestClass2", true, dbClassLoader);
      loadedSessionClass = true;

      // class inside 'test.jar' should fail to load since its not added to default DB.
      Class clz = Class.forName("ClassLoaderTestClass", true, Thread.currentThread().getContextClassLoader());
      loadedDBClass = true;
    } catch (ClassNotFoundException cnf) {
      log.error(cnf.getMessage(), cnf);
      Assert.assertTrue(loadedSessionClass);
      Assert.assertFalse(loadedDBClass);
    } finally {
      sessionService.release(sessionHandle);
    }

    // check loading on cube metastore client
    loadedSessionClass = false;
    loadedDBClass = false;
    try {
      log.info("@@@ TEST 1 - cube client");
      sessionService.acquire(sessionHandle);

      // testClass2 should be loaded since test2.jar is added to the session
      Class testClass2 = session.getCubeMetastoreClient().getConf().getClassByName("ClassLoaderTestClass2");
      //Class testClass2 = Class.forName("ClassLoaderTestClass2", true, dbClassLoader);
      loadedSessionClass = true;

      // class inside 'test.jar' should fail to load since its not added to default DB.
      Class clz = session.getCubeMetastoreClient().getConf().getClassByName("ClassLoaderTestClass");
      loadedDBClass = true;
    } catch (ClassNotFoundException cnf) {
      log.error(cnf.getMessage(), cnf);
      Assert.assertTrue(loadedSessionClass);
      Assert.assertFalse(loadedDBClass);
    } finally {
      sessionService.release(sessionHandle);
    }

    log.info("@@@ TEST 2");
    session.setCurrentDatabase(DB1);
    loadedSessionClass = false;
    loadedDBClass = false;
    try {
      sessionService.acquire(sessionHandle);
      // testClass2 should be loaded since test2.jar is added to the session
      URLClassLoader urlClassLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
      Class testClass2 = Class.forName("ClassLoaderTestClass2", true, Thread.currentThread().getContextClassLoader());
      // class inside 'test.jar' should also load since its added to DB1
      loadedSessionClass = true;
      Class clz = Class.forName("ClassLoaderTestClass", true, Thread.currentThread().getContextClassLoader());
      loadedDBClass = true;
    } finally {
      sessionService.release(sessionHandle);
    }
    Assert.assertTrue(loadedSessionClass);
    Assert.assertTrue(loadedDBClass);

    log.info("@@@ TEST 2 - cube client");
    loadedSessionClass = false;
    loadedDBClass = false;
    try {
      sessionService.acquire(sessionHandle);

      Class testClass2 = session.getCubeMetastoreClient().getConf().getClassByName("ClassLoaderTestClass2");
      // class inside 'test.jar' should also load since its added to DB1
      loadedSessionClass = true;
      Class clz = session.getCubeMetastoreClient().getConf().getClassByName("ClassLoaderTestClass");
      loadedDBClass = true;
    } finally {
      sessionService.release(sessionHandle);
    }
    Assert.assertTrue(loadedSessionClass);
    Assert.assertTrue(loadedDBClass);

    // Switch back to default DB, again the test2.jar should be available, test.jar should not be available
    log.info("@@@ TEST 3");
    session.setCurrentDatabase("default");
    loadedSessionClass = false;
    loadedDBClass = false;
    try {
      sessionService.acquire(sessionHandle);
      // testClass2 should be loaded since test2.jar is added to the session
      Class testClass2 = Class.forName("ClassLoaderTestClass2", true, Thread.currentThread().getContextClassLoader());
      // class inside 'test.jar' should fail to load since its not added to default DB.
      loadedSessionClass = true;
      Class clz = Class.forName("ClassLoaderTestClass", true, Thread.currentThread().getContextClassLoader());
      loadedDBClass = true;
    } catch (ClassNotFoundException cnf) {
      Assert.assertTrue(loadedSessionClass);
      Assert.assertFalse(loadedDBClass);
    } finally {
      sessionService.release(sessionHandle);
    }

    log.info("@@@ TEST 3 -- cube client");
    session.setCurrentDatabase("default");
    loadedSessionClass = false;
    loadedDBClass = false;
    try {
      sessionService.acquire(sessionHandle);
      // testClass2 should be loaded since test2.jar is added to the session
      Class testClass2 = session.getCubeMetastoreClient().getConf().getClassByName("ClassLoaderTestClass2");
      //Class testClass2 = Class.forName("ClassLoaderTestClass2", true, dbClassLoader);
      loadedSessionClass = true;

      // class inside 'test.jar' should fail to load since its not added to default DB.
      Class clz = session.getCubeMetastoreClient().getConf().getClassByName("ClassLoaderTestClass");
      loadedDBClass = true;
    } catch (ClassNotFoundException cnf) {
      log.error(cnf.getMessage(), cnf);
      Assert.assertTrue(loadedSessionClass);
      Assert.assertFalse(loadedDBClass);
    } finally {
      sessionService.release(sessionHandle);
    }

    sessionService.closeSession(sessionHandle);
  }

}
