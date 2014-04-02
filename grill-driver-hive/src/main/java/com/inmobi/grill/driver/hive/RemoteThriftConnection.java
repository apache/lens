package com.inmobi.grill.driver.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.RetryingThriftCLIServiceClient;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.inmobi.grill.api.GrillException;

import javax.security.sasl.SaslException;

import java.util.HashMap;
import java.util.Map;

/**
 * Connect to a remote Hive Server 2 service to run driver queries
 */
public class RemoteThriftConnection implements ThriftConnection {
  public static final Log LOG = LogFactory.getLog(RemoteThriftConnection.class);
	private boolean connected;
  private CLIServiceClient hs2Client;
	
	public RemoteThriftConnection() {
		
	}

	@Override
	public CLIServiceClient getClient(HiveConf conf) throws GrillException {
		if (!connected) {
      try {
        hs2Client =
          RetryingThriftCLIServiceClient.newRetryingCLIServiceClient(conf);
        LOG.info("HiveDriver connected to HiveServer @ " + conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST)
          + ":" + conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT));

      } catch (HiveSQLException e) {
        throw new GrillException(e);
      }
      connected = true;
		}
		return hs2Client;
	}

	@Override
	public void close() {
		connected = false;
		if (hs2Client instanceof RetryingThriftCLIServiceClient.CLIServiceClientWrapper) {
			((RetryingThriftCLIServiceClient.CLIServiceClientWrapper) hs2Client).closeTransport();
		}
	}
}
