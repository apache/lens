package org.apache.lens.driver.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.RetryingThriftCLIServiceClient;
import org.apache.lens.api.LensException;

/**
 * Connect to a remote Hive Server 2 service to run driver queries.
 */
public class RemoteThriftConnection implements ThriftConnection {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(RemoteThriftConnection.class);

  /** The connected. */
  private boolean connected;

  /** The hs2 client. */
  private CLIServiceClient hs2Client;

  /**
   * Instantiates a new remote thrift connection.
   */
  public RemoteThriftConnection() {

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.driver.hive.ThriftConnection#getClient(org.apache.hadoop.hive.conf.HiveConf)
   */
  @Override
  public CLIServiceClient getClient(HiveConf conf) throws LensException {
    if (!connected) {
      try {
        LOG.info("HiveDriver connecting to HiveServer @ "
            + conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST) + ":"
            + conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT));
        hs2Client = RetryingThriftCLIServiceClient.newRetryingCLIServiceClient(conf);
        LOG.info("HiveDriver connected to HiveServer @ " + conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST)
            + ":" + conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT));

      } catch (HiveSQLException e) {
        throw new LensException(e);
      }
      connected = true;
    }
    return hs2Client;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() {
    connected = false;
    if (hs2Client instanceof RetryingThriftCLIServiceClient.CLIServiceClientWrapper) {
      ((RetryingThriftCLIServiceClient.CLIServiceClientWrapper) hs2Client).closeTransport();
    }
  }
}
