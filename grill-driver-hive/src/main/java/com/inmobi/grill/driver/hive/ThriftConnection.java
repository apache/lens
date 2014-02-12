package com.inmobi.grill.driver.hive;

import java.io.Closeable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.CLIServiceClient;

import com.inmobi.grill.exception.GrillException;

public interface ThriftConnection extends Closeable {
	public CLIServiceClient getClient(Configuration conf) throws GrillException;
}
