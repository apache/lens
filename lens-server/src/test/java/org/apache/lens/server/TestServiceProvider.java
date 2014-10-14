package org.apache.lens.server;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.GrillServerConf;
import org.apache.lens.server.GrillServices;
import org.apache.lens.server.api.GrillConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;
import org.apache.lens.server.api.events.GrillEventService;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestServiceProvider extends GrillAllApplicationJerseyTest {
  @Test
  public void testServiceProvider() throws Exception {
    HiveConf conf = GrillServerConf.get();
    Class<? extends ServiceProviderFactory> spfClass = conf.getClass(GrillConfConstants.SERVICE_PROVIDER_FACTORY,
      null,
      ServiceProviderFactory.class);

    ServiceProviderFactory spf = spfClass.newInstance();

    ServiceProvider serviceProvider = spf.getServiceProvider();
    Assert.assertNotNull(serviceProvider);
    Assert.assertTrue(serviceProvider instanceof GrillServices);

    QueryExecutionService querySvc = (QueryExecutionService) serviceProvider.getService(QueryExecutionService.NAME);
    Assert.assertNotNull(querySvc);

    GrillEventService eventSvc = (GrillEventService) serviceProvider.getService(GrillEventService.NAME);
    Assert.assertNotNull(eventSvc);
  }

  @Override
  protected int getTestPort() {
    return 12121;
  }
}
