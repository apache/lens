package com.inmobi.grill.server;

import javax.ws.rs.core.Application;

public abstract class GrillAllApplicationJerseyTest extends GrillJerseyTest {

  @Override
  protected Application configure() {
    return new AllApps();
  }

}
