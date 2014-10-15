package org.apache.lens.server;

import org.apache.lens.server.api.ServiceProvider;
import org.apache.lens.server.api.ServiceProviderFactory;

public class ServiceProviderFactoryImpl implements ServiceProviderFactory{
  @Override
  public ServiceProvider getServiceProvider() {
    return LensServices.get();
  }
}
