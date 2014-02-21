package com.inmobi.grill.server;

import java.net.URI;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Application;

import org.apache.hadoop.hive.conf.HiveConf;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.driver.hive.TestRemoteHiveDriver;
import com.inmobi.grill.server.GrillServices;

public abstract class GrillJerseyTest extends JerseyTest {

  protected URI getUri() {
    return UriBuilder.fromUri("http://localhost/").port(getTestPort()).build();
  }

  protected abstract int getTestPort();

  @Override
  protected URI getBaseUri() {
    return UriBuilder.fromUri(getUri()).path("grill-server").build();
  }

  @BeforeSuite
  public void startAll() throws Exception {
    TestRemoteHiveDriver.createHS2Service();
    GrillServices.get().init(new HiveConf());
    GrillServices.get().start();
  }

  @AfterSuite
  public void stopAll() throws Exception {
    GrillServices.get().stop();
    TestRemoteHiveDriver.stopHS2Service();
  }

}
