package com.inmobi.grill.server;

import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.ServiceProvider;
import com.inmobi.grill.server.api.ServiceProviderFactory;
import com.inmobi.grill.server.api.events.GrillEventService;
import com.inmobi.grill.server.api.query.QueryExecutionService;
import org.apache.hadoop.hive.conf.HiveConf;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestServiceProvider extends GrillAllApplicationJerseyTest {
  @Test
  public void testServiceProvider() throws Exception {
    HiveConf conf = GrillServerConf.get();
    Class<? extends ServiceProviderFactory> spfClass = conf.getClass(GrillConfConstants.GRILL_SERVICE_PROVIDER_FACTORY,
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
