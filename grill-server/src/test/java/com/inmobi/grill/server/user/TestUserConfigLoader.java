package com.inmobi.grill.server.user;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.server.GrillServerConf;
import com.inmobi.grill.server.api.GrillConfConstants;
import liquibase.Liquibase;
import liquibase.database.jvm.HsqlConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.FileSystemResourceAccessor;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.hive.conf.HiveConf;
import org.hsqldb.server.Server;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;

/*
 * #%L
 * Grill Server
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
public class TestUserConfigLoader {
  private HiveConf conf;

  @BeforeTest(alwaysRun = true)
  public void init() {
    GrillServerConf.conf = null;
    conf = GrillServerConf.get();
  }
  @AfterTest
  public void resetFactory() {
    init();
    UserConfigLoaderFactory.init(conf);
  }
  @Test
  public void testFixed() throws GrillException {
    conf.addResource(TestUserConfigLoader.class.getResourceAsStream("/user/fixed.xml"));
    UserConfigLoaderFactory.init(conf);
    HashMap<String, String> expected = new HashMap<String, String>() {
      {
        put(GrillConfConstants.GRILL_SESSION_CLUSTER_USER, "grilluser");
      }
    };
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user1"), expected);
  }

  @Test
  public void testPropertyBased() throws GrillException {
    conf.addResource(TestUserConfigLoader.class.getResourceAsStream("/user/propertybased.xml"));
    conf.set(GrillConfConstants.GRILL_SERVER_USER_RESOLVER_PROPERTYBASED_FILENAME, TestUserConfigLoader.class.getResource("/user/propertybased.txt").getPath());
    UserConfigLoaderFactory.init(conf);
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user1"), new HashMap<String, String>() {
      {
        put(GrillConfConstants.GRILL_SESSION_CLUSTER_USER, "clusteruser1");
        put(GrillConfConstants.MAPRED_JOB_QUEUE_NAME, "queue1");
      }
    });
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user2"), new HashMap<String, String>() {
      {
        put(GrillConfConstants.GRILL_SESSION_CLUSTER_USER, "clusteruser2");
        put(GrillConfConstants.MAPRED_JOB_QUEUE_NAME, "queue2");
      }
    });
  }

  public void setupHsqlDb(String dbName, String path) throws SQLException, LiquibaseException {
    Server server = new Server();
    server.setLogWriter(new PrintWriter(System.out));
    server.setErrWriter(new PrintWriter(System.out));
    server.setSilent(true);
    server.setDatabaseName(0, dbName);
    server.setDatabasePath(0, "file:" + path);
    server.start();
    BasicDataSource ds = DatabaseUserConfigLoader.getDataSourceFromConf(conf);
    Liquibase liquibase = new Liquibase(UserConfigLoader.class.getResource("/user/db_changelog.xml").getFile(),
      new FileSystemResourceAccessor(), new HsqlConnection(ds.getConnection()));
    liquibase.dropAll();
    liquibase.update("");
  }

  @Test
  public void testDatabase() throws GrillException, SQLException, LiquibaseException {
    String path = "target/userconfig_hsql.db";
    String dbName = "main";
    conf.addResource(TestUserConfigLoader.class.getResourceAsStream("/user/database.xml"));
    UserConfigLoaderFactory.init(conf);
    setupHsqlDb(dbName, path);
    String[][] valuesToVerify = new String[][] {
      {"user1", "clusteruser1", "queue12"},
      {"user2", "clusteruser2", "queue12"},
      {"user3", "clusteruser3", "queue34"},
      {"user4", "clusteruser4", "queue34"},
    };
    for(final String[] sa: valuesToVerify) {
      Assert.assertEquals(UserConfigLoaderFactory.getUserConfig(sa[0]), new HashMap<String, String>() {
        {
          put(GrillConfConstants.GRILL_SESSION_CLUSTER_USER, sa[1]);
          put(GrillConfConstants.MAPRED_JOB_QUEUE_NAME, sa[2]);
        }
      });
    }
  }
  @Test
  public void testCustom() throws GrillException {
    conf.addResource(TestUserConfigLoader.class.getResourceAsStream("/user/custom.xml"));
    UserConfigLoaderFactory.init(conf);
    Assert.assertEquals(UserConfigLoaderFactory.getUserConfig("user1"), FooBarConfigLoader.CONST_HASH_MAP);
  }
}
