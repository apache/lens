package com.inmobi.grill.driver.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.thrift.RetryingThriftCLIServiceClient;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.inmobi.grill.api.GrillConfConstatnts;
import com.inmobi.grill.exception.GrillException;

import javax.security.sasl.SaslException;

import java.rmi.Remote;
import java.util.HashMap;
import java.util.Map;

/**
 * Connect to a remote Hive Server 2 service to run driver queries
 */
public class RemoteThriftConnection implements ThriftConnection {
  public static final Log LOG = LogFactory.getLog(RemoteThriftConnection.class);
  public static final String HS2_HOST = "hive.server2.thrift.bind.host";
  public static final String HS2_PORT = "hive.server2.thrift.port";
  public static final String HS2_CONNECTION_TIMEOUT = "grill.hive.server2.connect.timeout";
  public static final int HS2_DEFAULT_TIMEOUT = 60000;
  public static final int HS2_DEFAULT_PORT = 10000;

	private TTransport transport;
	private boolean connected;
	private TCLIService.Client client;
  private CLIServiceClient hs2Client;
	
  private static final String HIVE_AUTH_TYPE= "auth";
  private static final String HIVE_AUTH_SIMPLE = "noSasl";
  private static final String HIVE_AUTH_PRINCIPAL = "principal";
  private static final String HIVE_ANONYMOUS_USER = "anonymous";
  private static final String HIVE_ANONYMOUS_PASSWD = "anonymous";
  
	public RemoteThriftConnection() {
		
	}

	@Override
	public CLIServiceClient getClient(Configuration conf) throws GrillException {
		if (!connected) {
      String remoteHost = conf.get(HS2_HOST);
      if (remoteHost == null) {
        throw new GrillException("Hostname not specified for HiveServer");
      }
      int remotePort = conf.getInt(HS2_PORT, HS2_DEFAULT_PORT);
      LOG.info("Connecting to " + remoteHost + ":" + remotePort);
      openTransport(conf, remoteHost, remotePort, conf.getValByRegex(".*"));
			hs2Client =
        RetryingThriftCLIServiceClient.newRetryingCLIServiceClient(new HiveConf(conf, RemoteThriftConnection.class),
        client);
      connected = true;
      LOG.info("Connected");
		}
		return hs2Client;
	}

  /**
   * Indicate if next call of getClient should return a new connection
   */
  @Override
  public void setNeedsReconnect() {
    try {
      close();
    } catch (Exception e) {
      LOG.warn("Unable to close previous connection in setNeedsReconnect", e);
    } finally {
      connected = false;
    }
  }

  private void openTransport(Configuration conf, String host, int port, Map<String, String> sessConf )
    throws GrillException {
  	transport = new TSocket(host, port);
    ((TSocket) transport).setTimeout(conf.getInt(HS2_CONNECTION_TIMEOUT, HS2_DEFAULT_TIMEOUT));
    // handle secure connection if specified
    if (!sessConf.containsKey(HIVE_AUTH_TYPE)
      || !sessConf.get(HIVE_AUTH_TYPE).equals(HIVE_AUTH_SIMPLE)) {
      try {
        if (sessConf.containsKey(HIVE_AUTH_PRINCIPAL)) {
          transport = KerberosSaslHelper.getKerberosTransport(
            sessConf.get(HIVE_AUTH_PRINCIPAL), host, transport, sessConf);
        } else {
          String userName = sessConf.get(HiveDriver.GRILL_USER_NAME_KEY);
          if ((userName == null) || userName.isEmpty()) {
            userName = HIVE_ANONYMOUS_USER;
          }
          String passwd = sessConf.get(HiveDriver.GRILL_PASSWORD_KEY);
          if ((passwd == null) || passwd.isEmpty()) {
            passwd = HIVE_ANONYMOUS_PASSWD;
          }
          transport = PlainSaslHelper.getPlainTransport(userName, passwd, transport);
        }
      } catch (SaslException e) {
        throw new GrillException(e);
      }
    }

    TProtocol protocol = new TBinaryProtocol(transport);
    client = new TCLIService.Client(protocol);
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new GrillException(e);
    }
  }
	@Override
	public void close() {
		if (connected) {
			transport.close();
		}
	}
}
