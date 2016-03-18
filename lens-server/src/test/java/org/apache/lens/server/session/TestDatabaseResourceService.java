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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.api.LensConfConstants;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestDatabaseResourceService {
  private static final String DB_PFX = TestDatabaseResourceService.class.getSimpleName();
  public static final String TEST_CLASS = "ClassLoaderTestClass";


  private static final String DB1 = DB_PFX + "db1";
  private static final String DB2 = DB_PFX + "db2";

  private final String[] testDatabases = {DB1, DB2};

  private static final String JAR_ORDER_AND_FILES = DB_PFX + "jar_order_files_in_db";
  private static final String NO_JAR_ORDER_AND_VERSION_FILES = DB_PFX + "no_jar_order_and_version_files";
  private static final String NO_JAR_ORDER_NO_FILES = DB_PFX + "no_jar_order_no_files";

  private final String[] testDatabases1 = {JAR_ORDER_AND_FILES, NO_JAR_ORDER_AND_VERSION_FILES,NO_JAR_ORDER_NO_FILES};

  private final HiveConf conf = new HiveConf(TestDatabaseResourceService.class);
  private DatabaseResourceService dbResService;

  private final HiveConf conf1 = new HiveConf(TestDatabaseResourceService.class);
  private DatabaseResourceService dbResService1;

  private final HiveConf conf2 = new HiveConf(TestDatabaseResourceService.class);
  private DatabaseResourceService dbResService2;

  @BeforeClass
  public void setup() throws Exception {

    String prefix = "file://"+System.getProperty("user.dir");

    conf.set(LensConfConstants.DATABASE_RESOURCE_DIR, prefix+"/target/resources");
    conf1.set(LensConfConstants.DATABASE_RESOURCE_DIR, prefix+"/target/resources_without_common_jars");
    conf2.set(LensConfConstants.DATABASE_RESOURCE_DIR, prefix+"/target/resources_with_common_jars");

    LensServerTestUtil.createTestDatabaseResources(testDatabases, conf);
    LensServerTestUtil.createTestDbWithoutCommonJars(testDatabases1, conf1);
    LensServerTestUtil.createTestDbWithCommonJars(testDatabases1, conf2);

    dbResService = new DatabaseResourceService(DatabaseResourceService.NAME);
    dbResService.init(conf);
    dbResService.start();

    dbResService1 = new DatabaseResourceService(DatabaseResourceService.NAME);
    dbResService1.init(conf1);
    dbResService1.start();

    dbResService2 = new DatabaseResourceService(DatabaseResourceService.NAME);
    dbResService2.init(conf2);
    dbResService2.start();


  }

  @AfterClass
  public void tearDown() throws Exception {
    Hive hive0 = Hive.get(conf);
    for (String db : testDatabases) {
      hive0.dropDatabase(db, true, true);
    }

    Hive hive1 = Hive.get(conf1);
    for (String db : testDatabases1) {
      hive1.dropDatabase(db, true, true);
    }

    Hive hive2 = Hive.get(conf2);
    for (String db : testDatabases1) {
      hive2.dropDatabase(db, true, true);
    }

  }

  @Test
  public void testClassLoaderCreated() throws Exception {
    ClassLoader db1Loader = dbResService.getClassLoader(DB1);
    ClassLoader db2Loader = dbResService.getClassLoader(DB2);

    Assert.assertNotNull(db1Loader);
    Assert.assertNotNull(db2Loader);
    Assert.assertTrue(db1Loader != db2Loader);
  }

  private boolean isJarLoaded(ClassLoader loader, String db) throws Exception {
    URLClassLoader db1Loader = (URLClassLoader) loader;

    for (URL url : db1Loader.getURLs()) {
      String jarFile = url.getPath();
      if (jarFile.endsWith(db + ".jar")) {
        log.info("Found jar url " + url.toString());
        return true;
      }
    }
    return false;
  }

  @Test
  public void testJarsLoaded() throws Exception {
    // Verify that each db's classloader contains corresponding jar
    Assert.assertTrue(isJarLoaded(dbResService.getClassLoader(DB1), DB1), DB1 + " jar should be loaded");
    Assert.assertTrue(isJarLoaded(dbResService.getClassLoader(DB2), DB2), DB2 + " jar should be loaded");

    // Verify DB1 loader does not contain DB2's jar and vice versa
    Assert.assertFalse(isJarLoaded(dbResService.getClassLoader(DB2), DB1));
    Assert.assertFalse(isJarLoaded(dbResService.getClassLoader(DB1), DB2));
  }

  @Test
  public void testJarOrder() throws Exception {
    Collection<LensSessionImpl.ResourceEntry> actualOrder = dbResService.getResourcesForDatabase(DB1);
    List<String> actualOrderList = new ArrayList<String>();

    for (LensSessionImpl.ResourceEntry res : actualOrder) {
      actualOrderList.add(res.getLocation());
    }

    String[] expectedOrderArr = {
      "z_" + DB1 + ".jar",
      "y_" + DB1 + ".jar",
      "x_" + DB1 + ".jar",
    };

    // Verify order
    for (int i = 0; i < expectedOrderArr.length; i++) {
      Assert.assertTrue(actualOrderList.get(i).contains(expectedOrderArr[i]),
        actualOrderList.get(i) + " > " + expectedOrderArr[i]);
    }
  }

  @Test
  public void verifyClassLoader() throws Exception {
    // Should fail now since current classloader doesn't have jar loaded
    try {
      Class clz = Class.forName("ClassLoaderTestClass", true, getClass().getClassLoader());
      Assert.fail("Expected class loading to fail");
    } catch (Throwable th) {
      log.error("Expected error " + th + " msg = " + th.getMessage(), th);
    }

    // Should pass now
    Class clz = Class.forName(TEST_CLASS, true, dbResService.getClassLoader(DB1));
    Assert.assertNotNull(clz);
  }


  /**************************************************
   * Test cases without common jars
   **************************************************/
  @Test
  public void testDbWithoutCommonJarsAndWithJarOrderAndFiles()throws Exception {
    String db = testDatabases1[0];
    Collection<LensSessionImpl.ResourceEntry> actualOrder = dbResService1.getResourcesForDatabase(db);
    List<String> actualOrderList = new ArrayList<String>();

    for (LensSessionImpl.ResourceEntry res : actualOrder) {
      actualOrderList.add(res.getLocation());
    }

    String[] jarFilesOrder = {
      "z_" + db + ".jar",
      "y_" + db + ".jar",
      "x_" + db + ".jar",
    };

    // Verify order
    for (int i = 0; i < jarFilesOrder.length; i++) {
      Assert.assertTrue(actualOrderList.get(i).contains(jarFilesOrder[i]),
        actualOrderList.get(i) + " > " + jarFilesOrder[i]);
    }
  }

  @Test
  public void testDbWithoutCommonJarsAndWithNoJarOrderAndVersionFiles()throws Exception {
    String db = testDatabases1[1];
    Collection<LensSessionImpl.ResourceEntry> actualOrder = dbResService1.getResourcesForDatabase(db);
    List<String> actualOrderList = new ArrayList<String>();

    for (LensSessionImpl.ResourceEntry res : actualOrder) {
      actualOrderList.add(res.getLocation());
    }

    // Should pick the latest one
    String[] jarFilesOrder = {
      db + "_3.jar",
    };

    // Verify order
    for (int i = 0; i < jarFilesOrder.length; i++) {
      Assert.assertTrue(actualOrderList.get(i).contains(jarFilesOrder[i]),
        actualOrderList.get(i) + " > " + jarFilesOrder[i]);
    }
  }

  @Test
  public void testDbWithoutCommonJarsAndNoJarOrderAndNoFiles()throws Exception {
    String db = testDatabases1[2];
    Collection<LensSessionImpl.ResourceEntry> actualOrder = dbResService1.getResourcesForDatabase(db);
    Assert.assertNull(actualOrder);

  }

  /**************************************************
   * Test cases without common jars
   **************************************************/

  @Test
  public void testDbWithCommonJarsAndWithJarOrderAndFiles()throws Exception {
    String db = testDatabases1[0];
    Collection<LensSessionImpl.ResourceEntry> actualOrder = dbResService2.getResourcesForDatabase(db);
    List<String> actualOrderList = new ArrayList<String>();

    for (LensSessionImpl.ResourceEntry res : actualOrder) {
      actualOrderList.add(res.getLocation());
    }

    String[] jarFilesOrder = {
      "z_" + db + ".jar",
      "y_" + db + ".jar",
      "x_" + db + ".jar",
    };

    // Verify order
    for (int i = 0; i < jarFilesOrder.length; i++) {
      Assert.assertTrue(actualOrderList.get(i).contains(jarFilesOrder[i]),
        actualOrderList.get(i) + " > " + jarFilesOrder[i]);
    }
  }

  @Test
  public void testDbWithCommonJarsAndWithNoJarOrderAndVersionFiles()throws Exception {
    String db = testDatabases1[1];
    Collection<LensSessionImpl.ResourceEntry> actualOrder = dbResService2.getResourcesForDatabase(db);
    List<String> actualOrderList = new ArrayList<String>();

    for (LensSessionImpl.ResourceEntry res : actualOrder) {
      actualOrderList.add(res.getLocation());
    }

    // Should pick the latest one
    String[] jarFilesOrder = {
      db + "_3.jar",
      "lens-ship.jar",
      "meta.jar",
    };

    // Verify order
    for (int i = 0; i < jarFilesOrder.length; i++) {
      Assert.assertTrue(actualOrderList.get(i).contains(jarFilesOrder[i]),
        actualOrderList.get(i) + " > " + jarFilesOrder[i]);
    }
  }

  @Test
  public void testDbWithCommonJarsAndNoJarOrderAndNoFiles()throws Exception {
    String db = testDatabases1[2];
    Collection<LensSessionImpl.ResourceEntry> actualOrder = dbResService2.getResourcesForDatabase(db);
    List<String> actualOrderList = new ArrayList<String>();

    for (LensSessionImpl.ResourceEntry res : actualOrder) {
      actualOrderList.add(res.getLocation());
    }

    // Should pick the latest one
    String[] jarFilesOrder = {
      "lens-ship.jar",
      "meta.jar",
    };

    // Verify order
    for (int i = 0; i < jarFilesOrder.length; i++) {
      Assert.assertTrue(actualOrderList.get(i).contains(jarFilesOrder[i]),
        actualOrderList.get(i) + " > " + jarFilesOrder[i]);
    }
  }


}
