package com.inmobi.grill.driver.hive;

import java.io.Closeable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIServiceClient;

import com.inmobi.grill.api.GrillException;

public interface ThriftConnection extends Closeable {
	public CLIServiceClient getClient(HiveConf conf) throws GrillException;
}
