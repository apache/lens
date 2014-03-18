package com.inmobi.grill.jdbc;


import com.inmobi.grill.client.GrillClientConfig;
import com.inmobi.grill.client.GrillConnection;
import com.inmobi.grill.client.GrillConnectionParams;
import com.inmobi.grill.server.GrillJerseyTest;
import com.inmobi.grill.server.session.SessionApp;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.core.Application;


public class GrillConnectionTest extends GrillJerseyTest {
  @Override
  protected int getTestPort() {
    return 8080;
  }


  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected Application configure() {
    return new SessionApp();
  }

  @Test
  public void mysampleTest() {
    GrillClientConfig conf = new GrillClientConfig();
    conf.setGrillBasePath("grill-server");
    GrillConnection grillConnection = new GrillConnection(new GrillConnectionParams(conf));
    grillConnection.open();
  }
}
