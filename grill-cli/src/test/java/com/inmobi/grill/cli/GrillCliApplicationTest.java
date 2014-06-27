package com.inmobi.grill.cli;

import com.inmobi.grill.server.GrillAllApplicationJerseyTest;
import com.inmobi.grill.server.GrillServices;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;


public class GrillCliApplicationTest extends GrillAllApplicationJerseyTest {

  @Override
  protected int getTestPort() {
    return 9999;
  }

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri(getUri()).path("grillapi").build();
  }

  @BeforeTest
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterTest
  public void tearDown() throws Exception {
    GrillServices.get().setServiceMode(GrillServices.SERVICE_MODE.OPEN);
    super.tearDown();
  }


}
