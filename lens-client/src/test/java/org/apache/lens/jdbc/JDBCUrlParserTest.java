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
package org.apache.lens.jdbc;

import java.util.Map;

import org.apache.lens.client.LensClientConfig;
import org.apache.lens.client.LensConnectionParams;
import org.apache.lens.client.jdbc.JDBCUtils;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * The Class JDBCUrlParserTest.
 */
public class JDBCUrlParserTest {

  /**
   * Test illegal jdbc uri.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIllegalJDBCUri() {
    String uri = "jdbc:gril://localhost:1000";
    JDBCUtils.parseUrl(uri);
    Assert.fail("Illegal argument exception should have been thrown.");
  }

  /**
   * Test defaults with configuration variables.
   */
  @Test
  public void testDefaultsWithConfigurationVariables() {
    String uri = "jdbc:lens:///;username=johndoe;password=blah?conf1=blah1;conf2=blah2#var1=123;var2=456";
    LensConnectionParams params = JDBCUtils.parseUrl(uri);
    Assert.assertEquals(LensClientConfig.DEFAULT_DBNAME_VALUE, params.getDbName(),
      "The database should be default database");
    Assert.assertEquals(LensClientConfig.DEFAULT_SERVER_BASE_URL, params.getBaseConnectionUrl(),
      "The base url should be default");

    Map<String, String> sessionVars = params.getSessionVars();
    Assert.assertEquals(2, sessionVars.size(), "You should have two session variable");
    Assert.assertEquals("johndoe", sessionVars.get("username"), "The username should be johndoe");
    Assert.assertEquals("blah", sessionVars.get("password"), "The password should be blah");

    Map<String, String> lensConf = params.getLensConfs();
    Assert.assertEquals(2, lensConf.size(), "You should have two configuration variables");
    Assert.assertEquals("blah1", lensConf.get("conf1"), "The value for conf1 should be blah1");
    Assert.assertEquals("blah2", lensConf.get("conf2"), "The value for conf2 should be blah2");

    Map<String, String> lensVars = params.getLensVars();

    Assert.assertEquals(2, lensVars.size(), "You should have two lens variables");
    Assert.assertEquals("123", lensVars.get("var1"), "The value for var1 should be 123");
    Assert.assertEquals("456", lensVars.get("var2"), "The value for var2 should be 456");
  }

  /**
   * Test jdbc with custom host and port and db.
   */
  @Test
  public void testJDBCWithCustomHostAndPortAndDB() {
    String uri = "jdbc:lens://myhost:9000/mydb";
    LensConnectionParams params = JDBCUtils.parseUrl(uri);
    Assert.assertEquals(params.getBaseConnectionUrl(), "http://myhost:9000/lensapi",
      "The base url  should be http://myhost:9000/lensapi");
    Assert.assertEquals(params.getDbName(), "mydb", "The database should be mydb");
    Assert.assertTrue(params.getSessionVars().isEmpty(), "Session Variable list should be empty");
    Assert.assertTrue(params.getLensConfs().isEmpty(), "The conf list should be empty");
    Assert.assertTrue(params.getLensVars().isEmpty(), "The lens var list should be empty");
  }
}
