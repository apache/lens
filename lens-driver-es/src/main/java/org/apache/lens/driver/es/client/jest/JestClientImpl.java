/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.es.client.jest;

import org.apache.lens.driver.es.ESDriverConfig;
import org.apache.lens.driver.es.ESQuery;
import org.apache.lens.driver.es.client.ESClient;
import org.apache.lens.driver.es.client.ESResultSet;
import org.apache.lens.driver.es.exceptions.ESClientException;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Explain;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import lombok.NonNull;

/**
 * The ESRestClient for firing queries on elastic search
 */
public class JestClientImpl extends ESClient {

  private static final int DEFAULT_MAX_CONN = 10;
  private static final boolean DEFAULT_MULTI_THREADED = true;
  private static final String IS_MULTITHREADED = "lens.driver.es.jest.is.multi.threaded";
  private static final String MAX_TOTAL_CONN = "lens.driver.es.jest.max.conn";
  private static final String ES_SERVERS = "lens.driver.es.jest.servers";

  @NonNull
  private final JestClient client;

  public JestClientImpl(ESDriverConfig esDriverConfig, Configuration conf) {
    super(esDriverConfig, conf);
    final JestClientFactory factory = new JestClientFactory();
    factory.setHttpClientConfig(new HttpClientConfig
      .Builder(Validate.notNull(conf.getStringCollection(ES_SERVERS)))
      .maxTotalConnection(conf.getInt(MAX_TOTAL_CONN, DEFAULT_MAX_CONN))
      .multiThreaded(conf.getBoolean(IS_MULTITHREADED, DEFAULT_MULTI_THREADED))
      .readTimeout(esDriverConfig.getQueryTimeOutMs())
      .build());
    client = factory.getObject();
  }

  @Override
  public ESResultSet executeImpl(ESQuery esQuery) throws ESClientException {
    try {
      final Search search = new Search.Builder(esQuery.getQuery())
        .addIndex(esQuery.getIndex())
        .addType(esQuery.getType())
        .build();
      final SearchResult result = client.execute(search);
      if (result == null) {
        throw new NullPointerException("Got null result from client for " + esQuery);
      }
      return JestResultSetTransformer.transformFrom(
        result.getJsonObject(), esQuery.getSchema(), esQuery.getColumns());
    } catch (Exception e) {
      throw new ESClientException("Execution failed, ", e);
    }
  }

  public String explain(ESQuery esQuery) throws ESClientException {
    try {
      return client
        .execute(new Explain
          .Builder(esQuery.getIndex(), esQuery.getType(), null, esQuery.getQuery())
          .build())
        .getJsonString();
    } catch (Exception e) {
      throw new ESClientException("Explanation failed, ", e);
    }
  }

}
