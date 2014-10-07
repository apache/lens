package com.inmobi.grill.server.api;

import org.apache.hadoop.conf.Configuration;

public interface ServiceProviderFactory {
  public ServiceProvider getServiceProvider();
}
