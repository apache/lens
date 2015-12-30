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
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.driver.es.client.ESResultSet;
import org.apache.lens.driver.es.translator.ESVisitor;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Lists;

public class ScrollingQueryTest extends ESDriverTest{

  public ScrollingQueryTest() {
    super();
  }

  @Override
  protected void initializeConfig(Configuration config) {
    config.setInt(ESDriverConfig.TERM_FETCH_SIZE_KEY, 1);
    config.setInt(ESDriverConfig.QUERY_TIME_OUT_LENS_KEY, 10000);
    config.setInt(ESDriverConfig.MAX_ROW_SIZE_KEY, -1);
    config.setInt(ESDriverConfig.AGGR_BUCKET_SIZE_LENS_KEY, 100);
    config.setStrings(ESDriverConfig.CLIENT_CLASS_KEY, MockClientES.class.getCanonicalName());
    config.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    config.setStrings(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "es_storage");
  }

  @BeforeTest
  @Override
  public void beforeTest() throws LensException {
    super.beforeTest();
  }

  @Test
  protected void testScrollQueryExactLimit() throws LensException {
    ESResultSet resultSet =
      mockClientES.execute(ESVisitor.rewrite(esDriverConfig, "select col1 from index.type limit 3"));
    final List<ResultRow> rows = Lists.newArrayList();
    while (resultSet.hasNext()) {
      rows.add(resultSet.next());
    }
    Assert.assertEquals(rows.size(), 3, "Streaming failed!!!");
  }

  @Test
  protected void testScrollQueryLesserLimit() throws LensException {
    ESResultSet resultSet =
      mockClientES.execute(ESVisitor.rewrite(esDriverConfig, "select col1 from index.type limit 2"));
    final List<ResultRow> rows = Lists.newArrayList();
    while (resultSet.hasNext()) {
      rows.add(resultSet.next());
    }
    Assert.assertEquals(rows.size(), 2, "Streaming failed!!!");
  }

  @Test
  protected void testScrollQueryMoreLimit() throws LensException {
    ESResultSet resultSet =
      mockClientES.execute(ESVisitor.rewrite(esDriverConfig, "select col1 from index.type limit 4"));
    final List<ResultRow> rows = Lists.newArrayList();
    while (resultSet.hasNext()) {
      rows.add(resultSet.next());
    }
    Assert.assertEquals(rows.size(), 3, "Streaming failed!!!");
  }

}
