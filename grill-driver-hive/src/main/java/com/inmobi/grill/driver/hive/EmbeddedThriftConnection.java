package com.inmobi.grill.driver.hive;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;

import com.inmobi.grill.api.GrillException;

public class EmbeddedThriftConnection implements ThriftConnection {

	private ThriftCLIServiceClient client;
	private boolean connected;
	
	@Override
	public ThriftCLIServiceClient getClient(HiveConf conf) throws GrillException {
		if (!connected) {
	    client = new ThriftCLIServiceClient(new EmbeddedThriftBinaryCLIService());
	    connected = true;
		}
		return client;
	}

	@Override
	public void close() throws IOException {
		// Does nothing
	}
}
