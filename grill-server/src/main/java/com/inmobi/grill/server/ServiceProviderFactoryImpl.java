package com.inmobi.grill.server;

import com.inmobi.grill.server.api.ServiceProvider;
import com.inmobi.grill.server.api.ServiceProviderFactory;

public class ServiceProviderFactoryImpl implements ServiceProviderFactory{
  @Override
  public ServiceProvider getServiceProvider() {
    return GrillServices.get();
  }
}
