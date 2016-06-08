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

import java.io.File;
import java.net.URI;
import java.net.URLClassLoader;
import java.util.HashMap;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.user.UserConfigLoaderFactory;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFClassLoader;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;

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

    CLIService cliService = new CLIService(null);
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
    sessionService.stop();
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
      fail("Should not reach here as default db doesn't have jar loaded");
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
      fail("Should not reach here as default db doesn't have jar loaded");
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
      assertTrue(thClassLoader == session.getClassLoader(DB1));

      Class clz = Class.forName(TestDatabaseResourceService.TEST_CLASS, true, thClassLoader);
      assertNotNull(clz);
      // test cube metastore client's configuration's classloader
      clz = null;
      clz = session.getCubeMetastoreClient().getConf().getClassByName(TestDatabaseResourceService.TEST_CLASS);
      assertNotNull(clz);
    } catch (ClassNotFoundException cnf) {
      log.error(cnf.getMessage(), cnf);
      fail("Should not have thrown class not found exception: " + cnf.getMessage());
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
    session.setCurrentDatabase("default");

    boolean loadedSessionClass = false;
    boolean loadedDBClass = false;
    try {
      log.info("@@@ TEST 1");
      sessionService.acquire(sessionHandle);

      ClassLoader dbClassLoader = session.getClassLoader("default");
      assertTrue(Thread.currentThread().getContextClassLoader() == dbClassLoader);

      // testClass2 should be loaded since test2.jar is added to the session
      Class testClass2 = dbClassLoader.loadClass("ClassLoaderTestClass2");
      //Class testClass2 = Class.forName("ClassLoaderTestClass2", true, dbClassLoader);
      loadedSessionClass = true;

      // class inside 'test.jar' should fail to load since its not added to default DB.
      Class clz = Class.forName("ClassLoaderTestClass", true, Thread.currentThread().getContextClassLoader());
      loadedDBClass = true;
    } catch (ClassNotFoundException cnf) {
      log.error(cnf.getMessage(), cnf);
      assertTrue(loadedSessionClass);
      assertFalse(loadedDBClass);
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
      assertTrue(loadedSessionClass);
      assertFalse(loadedDBClass);
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
      URLClassLoader urlClassLoader = (SessionClassLoader) Thread.currentThread().getContextClassLoader();
      Class testClass2 = Class.forName("ClassLoaderTestClass2", true, Thread.currentThread().getContextClassLoader());
      // class inside 'test.jar' should also load since its added to DB1
      loadedSessionClass = true;
      Class clz = Class.forName("ClassLoaderTestClass", true, Thread.currentThread().getContextClassLoader());
      loadedDBClass = true;
    } finally {
      sessionService.release(sessionHandle);
    }
    assertTrue(loadedSessionClass);
    assertTrue(loadedDBClass);

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
    assertTrue(loadedSessionClass);
    assertTrue(loadedDBClass);

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
      assertTrue(loadedSessionClass);
      assertFalse(loadedDBClass);
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
      assertTrue(loadedSessionClass);
      assertFalse(loadedDBClass);
    } finally {
      sessionService.release(sessionHandle);
    }

    sessionService.closeSession(sessionHandle);
  }

  private void assertDBClassNotLoading(ClassLoader classLoader) {
    try {
      classLoader.loadClass("DatabaseJarSerde");
      fail("Shouldn't be able to load DatabaseJarSerde.class");
    } catch (ClassNotFoundException e) {
      log.debug("no issues");
    }
  }

  private void assertDBClassLoading(ClassLoader classLoader) throws ClassNotFoundException {
    classLoader.loadClass("DatabaseJarSerde");
  }

  @Test
  public void testClassloaderClose() throws LensException, HiveSQLException, ClassNotFoundException {
    String parentResource = "org/apache/hadoop/conf/Configuration.class";
    String db1Resource = "DatabaseJarSerde.class";
    LensSessionHandle sessionHandle1 = sessionService.openSession("foo", "bar", new HashMap<String, String>());
    LensSessionImpl session1 = sessionService.getSession(sessionHandle1);
    session1.setDbResService(sessionService.getDatabaseResourceService());
    session1.setCurrentDatabase(DB1);
    LensSessionHandle sessionHandle2 = sessionService.openSession("foo", "bar", new HashMap<String, String>());
    LensSessionImpl session2 = sessionService.getSession(sessionHandle2);
    session2.setDbResService(sessionService.getDatabaseResourceService());
    session2.setCurrentDatabase(DB1);
    SessionClassLoader session1ClassLoader1 = (SessionClassLoader) session1.getClassLoader();
    SessionClassLoader classLoader2 = (SessionClassLoader) session2.getClassLoader();
    // classloader logically same, but instance different
    assertEquals(session1ClassLoader1, classLoader2);
    assertFalse(session1ClassLoader1 == classLoader2);
    assertEquals(session1ClassLoader1.getParent(), classLoader2.getParent());
    assertDBClassLoading(session1ClassLoader1);
    session1.setCurrentDatabase("default");
    // Default has no parent class loader, so
    UDFClassLoader session1ClassLoader2 = (UDFClassLoader) session1.getClassLoader();
    assertNotEquals(session1ClassLoader2, session1ClassLoader1); // obviously, since even types are different
    assertNotEquals(session1ClassLoader1.getParent(), session1ClassLoader2.getParent());
    assertFalse(session1ClassLoader1.isClosed());
    assertFalse(session1ClassLoader2.isClosed());
    assertDBClassNotLoading(session1ClassLoader2);
    sessionService.closeSession(sessionHandle1);
    // both classloaders got closed, but parent classloader not closed.
    assertTrue(session1ClassLoader1.isClosed());
    assertTrue(session1ClassLoader2.isClosed());
    assertFalse(((UncloseableClassLoader) session1ClassLoader1.getParent()).isClosed());
    assertNotNull(session1ClassLoader2.getResource(parentResource));
    // session 1 classloader still able to load db1 jar resources
    assertNotNull(session1ClassLoader1.getResource(db1Resource));
    // didn't affect classloaders of another session.
    assertNotNull(classLoader2.getResource(parentResource));
    sessionService.closeSession(sessionHandle2);
    assertTrue(classLoader2.isClosed());
    LensSessionHandle sessionHandle3 = sessionService.openSession("foo", "bar", new HashMap<String, String>());
    LensSessionImpl session3 = sessionService.getSession(sessionHandle3);
    session3.setDbResService(sessionService.getDatabaseResourceService());
    session3.setCurrentDatabase("default");
    UDFClassLoader session3ClassLoader1 = (UDFClassLoader) session3.getClassLoader();
    assertFalse(session3ClassLoader1.isClosed());
    assertDBClassNotLoading(session3ClassLoader1);
    session3.setCurrentDatabase(DB1);
    SessionClassLoader session3ClassLoader2 = (SessionClassLoader) session3.getClassLoader();
    assertFalse(session3ClassLoader2.isClosed());
    assertDBClassLoading(session3ClassLoader2);
    // session classloader is different in case of different sessions.
    assertNotEquals(session3ClassLoader1, session1ClassLoader2);
    // both are instances of UDFClassLoader, with same number of urls
    assertEquals(session3ClassLoader1.getClass(), session1ClassLoader2.getClass());
    assertEquals(session1ClassLoader2.getURLs().length, session3ClassLoader1.getURLs().length);
    // without adding any jars, classloaders for different sessions using same database have same
    // parent and no extra urls added
    assertEquals(session3ClassLoader2.getParent(), session1ClassLoader1.getParent());
    assertEquals(session3ClassLoader2.getURLs().length, session1ClassLoader1.getURLs().length);

    session3.setCurrentDatabase("dummy1");
    UDFClassLoader session3ClassLoader3 = (UDFClassLoader) session3.getClassLoader();
    session3.setCurrentDatabase("dummy2");
    UDFClassLoader session3ClassLoader4 = (UDFClassLoader) session3.getClassLoader();
    assertDBClassNotLoading(session3ClassLoader3);
    assertDBClassNotLoading(session3ClassLoader4);
    assertEquals(session3ClassLoader3, session3ClassLoader4);
    sessionService.closeSession(sessionHandle3);
    assertTrue(session3ClassLoader1.isClosed());
    assertTrue(session3ClassLoader2.isClosed());
    assertTrue(session3ClassLoader3.isClosed());
    assertTrue(session3ClassLoader4.isClosed());
  }

  private void assertExtraClassNotLoading(ClassLoader classLoader) {
    try {
      classLoader.loadClass("ClassLoaderTestClass2");
      // Class may be already loaded due to cache.
      assertNull(classLoader.getResource("ClassLoaderTestClass2.class"));
    } catch (ClassNotFoundException e) {
      log.debug("no issues");
    }
  }

  private void assertExtraClassLoading(ClassLoader classLoader) throws ClassNotFoundException {
    classLoader.loadClass("ClassLoaderTestClass2");
    assertNotNull("ClassLoaderTestClass2.class");
  }

  @Test
  public void testSessionClassLoaderCloseWhenExtraJars() throws Exception {
    URI resource = new File("target/testjars/test2.jar").toURI();
    LensSessionHandle sessionHandle1 = sessionService.openSession("foo", "bar", new HashMap<String, String>());
    LensSessionImpl session1 = sessionService.getSession(sessionHandle1);
    session1.setDbResService(sessionService.getDatabaseResourceService());
    session1.setCurrentDatabase(DB1);
    sessionService.addResource(sessionHandle1, "jar", resource.toString());
    SessionClassLoader loader1 = (SessionClassLoader) session1.getClassLoader();
    assertExtraClassLoading(loader1);
    assertExtraClassNotLoading(loader1.getParent());
    assertEquals(loader1.getURLs()[0].toURI(), resource);
    // change db to a db which doesn't have db jars. and add resources there.

    session1.setCurrentDatabase("default");
    // Extra jar is still there.
    assertExtraClassLoading(session1.getClassLoader());
    UDFClassLoader loader2 = (UDFClassLoader) session1.getClassLoader();

    // Close and assert on all class loaders.
    session1.setCurrentDatabase(DB1);
    assertEquals(session1.getClassLoader(), loader1);
    sessionService.closeSession(sessionHandle1);
    assertTrue(loader1.isClosed());
    assertTrue(loader2.isClosed());
    assertExtraClassNotLoading(loader1);
    assertExtraClassNotLoading(loader2);
    assertFalse(((UncloseableClassLoader) loader1.getParent()).isClosed());
    assertExtraClassNotLoading(loader1.getParent());
  }
}
