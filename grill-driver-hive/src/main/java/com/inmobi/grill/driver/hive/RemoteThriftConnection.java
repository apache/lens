package com.inmobi.grill.driver.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.inmobi.grill.exception.GrillException;

import javax.security.sasl.SaslException;
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

  private static final String HIVE_AUTH_TYPE= "auth";
  private static final String HIVE_AUTH_SIMPLE = "noSasl";
  private static final String HIVE_AUTH_USER = "user";
  private static final String HIVE_AUTH_PRINCIPAL = "principal";
  private static final String HIVE_AUTH_PASSWD = "password";
  private static final String HIVE_ANONYMOUS_USER = "anonymous";
  private static final String HIVE_ANONYMOUS_PASSWD = "anonymous";

	private TTransport transport;
	private boolean connected;
	private TCLIService.Client client;
  private ThriftCLIServiceClient hs2Client;
	
	public RemoteThriftConnection() {
		
	}

	@Override
	public ThriftCLIServiceClient getClient(Configuration conf) throws GrillException {
		if (!connected) {
      String remoteHost = conf.get(HS2_HOST);
      int remotePort = conf.getInt(HS2_PORT, HS2_DEFAULT_PORT);
      openTransport(conf, remoteHost, remotePort, new HashMap<String, String>());
			hs2Client = new ThriftCLIServiceClient(client);
      connected = true;

		}
		return hs2Client;
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
          String userName = sessConf.get(HIVE_AUTH_USER);
          if ((userName == null) || userName.isEmpty()) {
            userName = HIVE_ANONYMOUS_USER;
          }
          String passwd = sessConf.get(HIVE_AUTH_PASSWD);
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
