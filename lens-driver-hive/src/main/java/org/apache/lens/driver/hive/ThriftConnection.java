package org.apache.lens.driver.hive;

import java.io.Closeable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.lens.api.LensException;

/**
 * The Interface ThriftConnection.
 */
public interface ThriftConnection extends Closeable {

  /**
   * Gets the client.
   *
   * @param conf
   *          the conf
   * @return the client
   * @throws LensException
   *           the lens exception
   */
  public CLIServiceClient getClient(HiveConf conf) throws LensException;
}
