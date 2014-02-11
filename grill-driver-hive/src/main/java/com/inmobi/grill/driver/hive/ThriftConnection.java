package com.inmobi.grill.driver.hive;

import java.io.Closeable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;

import com.inmobi.grill.api.GrillException;

public interface ThriftConnection extends Closeable {
	public ThriftCLIServiceClient getClient(Configuration conf) throws GrillException;
}
