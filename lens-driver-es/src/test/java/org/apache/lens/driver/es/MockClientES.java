/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.es;

import java.util.List;

import org.apache.lens.api.query.ResultRow;
import org.apache.lens.driver.es.client.ESClient;
import org.apache.lens.driver.es.client.ESResultSet;
import org.apache.lens.server.api.driver.LensResultSetMetadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

class MockClientES extends ESClient {

  private static final ImmutableMap<String, ResultSetProvider> QUERY_RESULTS_MAP;

  private interface ResultSetProvider {
    ESResultSet getResultSet();
  }

  static {
    ImmutableMap.Builder<String, ResultSetProvider> queryResultsMapBuilder = ImmutableMap.builder();
    queryResultsMapBuilder.put(
      "{\"from\":0,\"size\":1,\"fields\":[\"col1\"],\"sort\":[],\"timeout\":10000,\"filter\":{\"match_all\":{}}}",
      new ResultSetProvider() {
        @Override
        public ESResultSet getResultSet() {
          return new ESResultSet(
            1,
            Lists.newArrayList(
              new ResultRow(Lists.<Object>newArrayList("v1"))
            ),
            new LensResultSetMetadata() {
              @Override
              public List<ColumnDescriptor> getColumns() {
                return Lists.newArrayList(
                  new ColumnDescriptor("col1", "", new TypeDescriptor(Type.STRING_TYPE), 0)
                );
              }
            });
        }
      });
    queryResultsMapBuilder.put(
      "{\"from\":1,\"size\":1,\"fields\":[\"col1\"],\"sort\":[],\"timeout\":10000,\"filter\":{\"match_all\":{}}}",
      new ResultSetProvider() {
        @Override
        public ESResultSet getResultSet() {
          return new ESResultSet(
            1,
            Lists.newArrayList(
              new ResultRow(Lists.<Object>newArrayList("v1"))
            ),
            new LensResultSetMetadata() {
              @Override
              public List<ColumnDescriptor> getColumns() {
                return Lists.newArrayList(
                  new ColumnDescriptor("col1", "", new TypeDescriptor(Type.STRING_TYPE), 0)
                );
              }
            });
        }
      });
    queryResultsMapBuilder.put(
      "{\"from\":2,\"size\":1,\"fields\":[\"col1\"],\"sort\":[],\"timeout\":10000,\"filter\":{\"match_all\":{}}}",
      new ResultSetProvider() {
        @Override
        public ESResultSet getResultSet() {
          return new ESResultSet(
            1,
            Lists.newArrayList(
              new ResultRow(Lists.<Object>newArrayList("v1"))
            ),
            new LensResultSetMetadata() {
              @Override
              public List<ColumnDescriptor> getColumns() {
                return Lists.newArrayList(
                  new ColumnDescriptor("col1", "", new TypeDescriptor(Type.STRING_TYPE), 0)
                );
              }
            });
        }
      });
    queryResultsMapBuilder.put(
      "{\"from\":3,\"size\":1,\"fields\":[\"col1\"],\"sort\":[],\"timeout\":10000,\"filter\":{\"match_all\":{}}}",
      new ResultSetProvider() {
        @Override
        public ESResultSet getResultSet() {
          return new ESResultSet(
            0,
            Lists.<ResultRow>newArrayList(),
            new LensResultSetMetadata() {
              @Override
              public List<ColumnDescriptor> getColumns() {
                return Lists.newArrayList(
                  new ColumnDescriptor("col1", "", new TypeDescriptor(Type.STRING_TYPE), 0)
                );
              }
            });
        }
      });
    QUERY_RESULTS_MAP = queryResultsMapBuilder.build();
  }


  public MockClientES(ESDriverConfig esDriverConfig, Configuration conf) {
    super(esDriverConfig, conf);
  }

  @Override
  protected ESResultSet executeImpl(ESQuery esQuery) {
    return QUERY_RESULTS_MAP.get(esQuery.getQuery()).getResultSet();
  }

  @Override
  public String explain(ESQuery esQuery) {
    return QUERY_RESULTS_MAP.containsKey(esQuery.getQuery())
      ?
      "{}"
      :
      null;
  }
}
