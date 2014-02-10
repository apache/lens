package com.inmobi.grill.driver.hive;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;

import com.inmobi.grill.exception.GrillException;

public class EmbeddedThriftConnection implements ThriftConnection {

	private ThriftCLIServiceClient client;
	private boolean connected;
	
	@Override
	public CLIServiceClient getClient(Configuration conf) throws GrillException {
		if (!connected) {
	    client = new ThriftCLIServiceClient(new EmbeddedThriftBinaryCLIService());
	    connected = true;
		}
		return client;
	}

  /**
   * Indicate if next call of getClient should return a new connection
   */
  @Override
  public void setNeedsReconnect() {
    connected = false;
  }

  @Override
	public void close() throws IOException {
		// Does nothing
	}
}
