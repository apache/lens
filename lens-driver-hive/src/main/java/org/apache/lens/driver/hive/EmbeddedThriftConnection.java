package org.apache.lens.driver.hive;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.lens.api.LensException;

/**
 * The Class EmbeddedThriftConnection.
 */
public class EmbeddedThriftConnection implements ThriftConnection {

  /** The client. */
  private ThriftCLIServiceClient client;

  /** The connected. */
  private boolean connected;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.driver.hive.ThriftConnection#getClient(org.apache.hadoop.hive.conf.HiveConf)
   */
  @Override
  public ThriftCLIServiceClient getClient(HiveConf conf) throws LensException {
    if (!connected) {
      client = new ThriftCLIServiceClient(new EmbeddedThriftBinaryCLIService());
      connected = true;
    }
    return client;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    // Does nothing
  }
}
