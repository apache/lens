package org.apache.lens.server.api;

/**
 * A factory for creating ServiceProvider objects.
 */
public interface ServiceProviderFactory {
  public ServiceProvider getServiceProvider();
}
