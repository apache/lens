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
package org.apache.lens.server.user;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.user.UserConfigLoader;
import org.apache.lens.server.util.UtilityMethods;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.hive.conf.HiveConf;

import org.hsqldb.server.Server;
import org.testng.Assert;
import org.testng.annotations.*;

import liquibase.Liquibase;
import liquibase.database.jvm.HsqlConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.FileSystemResourceAccessor;

/**
 * The Class TestUserConfigLoader.
 */
@Test(groups = "unit-test")
public class TestUserConfigLoader {

  /** The conf. */
  private HiveConf conf;

  /**
   * Inits the.
   */
  @BeforeClass(alwaysRun = true)
  public void init() {
    conf = new HiveConf(LensServerConf.getHiveConf());
  }

  /**
   * Reset factory.
   */
  @AfterClass
  public void resetFactory() {
    init();
    UserConfigLoaderFactory.init(conf);
  }

  /**
   * Test fixed.
   *
   * @throws LensException the lens exception
   */
  @Test
  public void testFixed() throws LensException {
    conf.addResource(TestUserConfigLoader.class.getResourceAsStream("/user/fixed.xml"));
    UserConfigLoaderFactory.init(conf);
    HashMap<String, String> expected = new HashMap<String, String>() {
      {
        put(LensConfConstants.SESSION_CLUSTER_USER, "lensuser");
      }
    };
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user1"), expected);
  }

  /**
   * Test property based.
   *
   * @throws LensException the lens exception
   */
  @Test
  public void testPropertyBased() throws LensException {
    conf.addResource(TestUserConfigLoader.class.getResourceAsStream("/user/propertybased.xml"));
    conf.set(LensConfConstants.USER_RESOLVER_PROPERTYBASED_FILENAME,
      TestUserConfigLoader.class.getResource("/user/propertybased.data").getPath());
    UserConfigLoaderFactory.init(conf);
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user1"), new HashMap<String, String>() {
      {
        put(LensConfConstants.SESSION_CLUSTER_USER, "clusteruser1");
        put(LensConfConstants.MAPRED_JOB_QUEUE_NAME, "queue1");
      }
    });
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user2"), new HashMap<String, String>() {
      {
        put(LensConfConstants.SESSION_CLUSTER_USER, "clusteruser2");
        put(LensConfConstants.MAPRED_JOB_QUEUE_NAME, "queue2");
      }
    });
  }

  /**
   * Setup hsql db.
   *
   * @param dbName        the db name
   * @param path          the path
   * @param changeLogPath the change log path
   * @throws SQLException       the SQL exception
   * @throws LiquibaseException the liquibase exception
   */
  private void setupHsqlDb(String dbName, String path, String changeLogPath) throws SQLException, LiquibaseException {
    Server server = new Server();
    server.setLogWriter(new PrintWriter(System.out));
    server.setErrWriter(new PrintWriter(System.out));
    server.setSilent(true);
    server.setDatabaseName(0, dbName);
    server.setDatabasePath(0, "file:" + path);
    server.start();
    BasicDataSource ds = UtilityMethods.getDataSourceFromConf(conf);

    Liquibase liquibase = new Liquibase(UserConfigLoader.class.getResource(changeLogPath).getFile(),
      new FileSystemResourceAccessor(), new HsqlConnection(ds.getConnection()));
    liquibase.update("");
  }

  /**
   * Test database.
   *
   * @throws LensException      the lens exception
   * @throws SQLException       the SQL exception
   * @throws LiquibaseException the liquibase exception
   */
  @Test
  public void testDatabase() throws LensException, SQLException, LiquibaseException {
    String path = "target/queries.db";
    String dbName = "main";
    conf.addResource(TestUserConfigLoader.class.getResourceAsStream("/user/database.xml"));
    UserConfigLoaderFactory.init(conf);
    setupHsqlDb(dbName, path, "/user/db_changelog.xml");
    String[][] valuesToVerify = new String[][]{
      {"user1", "clusteruser1", "queue12"},
      {"user2", "clusteruser2", "queue12"},
      {"user3", "clusteruser3", "queue34"},
      {"user4", "clusteruser4", "queue34"},
    };
    for (final String[] sa : valuesToVerify) {
      Assert.assertEquals(UserConfigLoaderFactory.getUserConfig(sa[0]), new HashMap<String, String>() {
        {
          put(LensConfConstants.SESSION_CLUSTER_USER, sa[1]);
          put(LensConfConstants.MAPRED_JOB_QUEUE_NAME, sa[2]);
        }
      });
    }
  }

  /**
   * Test custom.
   *
   * @throws LensException the lens exception
   */
  @Test
  public void testCustom() throws LensException {
    conf.addResource(TestUserConfigLoader.class.getResourceAsStream("/user/custom.xml"));
    UserConfigLoaderFactory.init(conf);
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user1"), FooBarConfigLoader.CONST_HASH_MAP);
  }
}
