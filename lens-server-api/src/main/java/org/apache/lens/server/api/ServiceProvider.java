package org.apache.lens.server.api;

import org.apache.hive.service.Service;

public interface ServiceProvider {
  /**
   * Get an instance of a service by its name
   */
  public <T extends Service> T getService(String sName);
}
