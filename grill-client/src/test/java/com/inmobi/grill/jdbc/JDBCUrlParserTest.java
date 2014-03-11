package com.inmobi.grill.jdbc;

import com.inmobi.grill.client.GrillClientConfig;
import com.inmobi.grill.client.GrillConnectionParams;
import junit.framework.Assert;
import org.testng.annotations.Test;


import java.util.Map;

public class JDBCUrlParserTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testIllegalJDBCUri() {
    String uri = "jdbc:gril://localhost:1000";
    JDBCUtils.parseUrl(uri);
    Assert.fail("Illegal argument exception should have been thrown.");
  }


  @Test
  public void testDefaultsWithConfigurationVariables() {
    String uri = "jdbc:grill:///;username=johndoe;password=blah?conf1=blah1;conf2=blah2#var1=123;var2=456";
    GrillConnectionParams params = JDBCUtils.parseUrl(uri);
    Assert.assertEquals("The database should be default database",
        GrillClientConfig.DEFAULT_DBNAME_VALUE, params.getDbName());
    Assert.assertEquals("The port should be default port",
        GrillClientConfig.DEFAULT_SERVER_PORT, params.getPort());
    Assert.assertEquals("The host should be dedault host",
        GrillClientConfig.DEFAULT_SERVER_HOST_VALUE, params.getHost());

    Map<String, String> sessionVars = params.getSessionVars();
    Assert.assertEquals("You should have two session variable", 2,
        sessionVars.size());
    Assert.assertEquals("The username should be johndoe", "johndoe",
        sessionVars.get("username"));
    Assert.assertEquals("The password should be blah", "blah",
        sessionVars.get("password"));

    Map<String, String> grillConf = params.getGrillConfs();
    Assert.assertEquals("You should have two configuration variables", 2,
        grillConf.size());
    Assert.assertEquals("The value for conf1 should be blah1", "blah1",
        grillConf.get("conf1"));
    Assert.assertEquals("The value for conf2 should be blah2", "blah2",
        grillConf.get("conf2"));

    Map<String, String> grillVars = params.getGrillVars();

    Assert.assertEquals("You should have two grill variables", 2,
        grillVars.size());
    Assert.assertEquals("The value for var1 should be 123", "123",
        grillVars.get("var1"));
    Assert.assertEquals("The value for var2 should be 456", "456",
        grillVars.get("var2"));
  }

  @Test
  public void testJDBCWithCustomHostAndPortAndDB() {
    String uri = "jdbc:grill://myhost:9000/mydb";
    GrillConnectionParams params = JDBCUtils.parseUrl(uri);
    Assert.assertEquals("The host name should be myhost", "myhost",
        params.getHost());
    Assert.assertEquals("The port should be 9000", 9000, params.getPort());
    Assert.assertEquals("The database should be mydb", "mydb",
        params.getDbName());
    Assert.assertTrue("Session Variable list should be empty",
        params.getSessionVars().isEmpty());
    Assert.assertTrue("The conf list should be empty",
        params.getGrillConfs().isEmpty());
    Assert.assertTrue("The grill var list should be empty",
        params.getGrillVars().isEmpty());
  }
}
