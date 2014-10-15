package org.apache.lens.server;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.lens.server.LensServerConf;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;
import org.apache.lens.server.api.events.LensEventService;
import org.apache.lens.server.api.query.QueryExecutionService;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestServiceProvider extends LensAllApplicationJerseyTest {
  @Test
  public void testServiceProvider() throws Exception {
    HiveConf conf = LensServerConf.get();
    Class<? extends ServiceProviderFactory> spfClass = conf.getClass(LensConfConstants.SERVICE_PROVIDER_FACTORY,
      null,
      ServiceProviderFactory.class);

    ServiceProviderFactory spf = spfClass.newInstance();

    ServiceProvider serviceProvider = spf.getServiceProvider();
    Assert.assertNotNull(serviceProvider);
    Assert.assertTrue(serviceProvider instanceof LensServices);

    QueryExecutionService querySvc = (QueryExecutionService) serviceProvider.getService(QueryExecutionService.NAME);
    Assert.assertNotNull(querySvc);

    LensEventService eventSvc = (LensEventService) serviceProvider.getService(LensEventService.NAME);
    Assert.assertNotNull(eventSvc);
  }

  @Override
  protected int getTestPort() {
    return 12121;
  }
}
