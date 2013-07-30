package con.inmobi.grill.driver.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.inmobi.grill.exception.GrillException;

/**
 * Connect to a remote Hive Server 2 service to run driver queries
 */
public class RemoteThriftConnection implements ThriftConnection {

	public static final String HS2_HOST = "hive.hs2.host";
	public static final String HS2_PORT = "hive.hs2.port";
	public static final int HS2_DEFAULT_PORT = 8080;
	private TSocket transport;
	private boolean connected;
	private ThriftCLIServiceClient client;
	
	public RemoteThriftConnection() {
		
	}

	@Override
	public ThriftCLIServiceClient getClient(Configuration conf) throws GrillException {
		if (!connected) {
			transport = new TSocket(conf.get(HS2_HOST), conf.getInt(HS2_PORT, HS2_DEFAULT_PORT));
			try {
				transport.open();
				client = new ThriftCLIServiceClient(new TCLIService.Client(new TBinaryProtocol(transport)));
				connected = true;
			} catch (TTransportException e) {
				throw new GrillException(e);
			}
		}
		return client;
	}

	@Override
	public void close() {
		if (connected) {
			transport.close();
		}
	}
}
