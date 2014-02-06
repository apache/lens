package com.inmobi.grill.driver.hive;

import java.io.Closeable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;

import com.inmobi.grill.exception.GrillException;

public interface ThriftConnection extends Closeable {
	public ThriftCLIServiceClient getClient(Configuration conf) throws GrillException;

  /**
   * Indicate if next call of getClient should return a new connection
   */
  public void setNeedsReconnect();
}
