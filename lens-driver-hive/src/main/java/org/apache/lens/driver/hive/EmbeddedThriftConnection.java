package org.apache.lens.driver.hive;

/*
 * #%L
 * Lens Hive Driver
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.lens.api.LensException;


public class EmbeddedThriftConnection implements ThriftConnection {

	private ThriftCLIServiceClient client;
	private boolean connected;
	
	@Override
	public ThriftCLIServiceClient getClient(HiveConf conf) throws LensException {
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
