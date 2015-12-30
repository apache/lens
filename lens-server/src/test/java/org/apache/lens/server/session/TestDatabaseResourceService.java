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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lens.server.LensServerTestUtil;
import org.apache.lens.server.api.LensConfConstants;

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

  private final HiveConf conf = new HiveConf(TestDatabaseResourceService.class);
  private DatabaseResourceService dbResService;

  @BeforeClass
  public void setup() throws Exception {
    LensServerTestUtil.createTestDatabaseResources(testDatabases, conf);
    // Start resource service.
    conf.set(LensConfConstants.DATABASE_RESOURCE_DIR, "target/resources");
    dbResService = new DatabaseResourceService(DatabaseResourceService.NAME);
    dbResService.init(conf);
    dbResService.start();
  }

  @AfterClass
  public void tearDown() throws Exception {
    Hive hive = Hive.get(conf);
    for (String db : testDatabases) {
      hive.dropDatabase(db, true, true);
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

    for (URL url :  db1Loader.getURLs()) {
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
      log.error("Expected error " + th + " msg = "+th.getMessage(), th);
    }

    // Should pass now
    Class clz = Class.forName(TEST_CLASS, true, dbResService.getClassLoader(DB1));
    Assert.assertNotNull(clz);
  }
}
