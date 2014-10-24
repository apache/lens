package org.apache.lens.server.api;

import org.apache.hive.service.Service;

/**
 * The Interface ServiceProvider.
 */
public interface ServiceProvider {

  /**
   * Get an instance of a service by its name.
   *
   * @param <T>
   *          the generic type
   * @param sName
   *          the s name
   * @return the service
   */
  public <T extends Service> T getService(String sName);
}
