/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.query;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.lens.api.LensConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.driver.MockDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.LensMetricsRegistry;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

/**
 * Tests for abstract query context
 */
public class TestAbstractQueryContext {

  @Test
  public void testMetricsConfigEnabled() throws LensException {
    Configuration conf = new Configuration();
    List<LensDriver> testDrivers = new ArrayList<LensDriver>();
    MockDriver mdriver = new MockDriver();
    mdriver.configure(conf);
    testDrivers.add(mdriver);
    conf.setBoolean(LensConfConstants.ENABLE_QUERY_METRICS, true);
    MockQueryContext ctx = new MockQueryContext("mock query", new LensConf(), conf, testDrivers);
    String uniqueMetridId = ctx.getConf().get(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY);
    Assert.assertNotNull(uniqueMetridId);
    UUID.fromString(uniqueMetridId);
    StringBuilder expectedStackName = new StringBuilder();
    expectedStackName.append(uniqueMetridId).append("-").append(MockDriver.class.getSimpleName());
    Assert.assertEquals(ctx.getDriverConf(mdriver).get(LensConfConstants.QUERY_METRIC_DRIVER_STACK_NAME),
      expectedStackName.toString());
  }

  @Test
  public void testMetricsConfigDisabled() throws LensException {
    Configuration conf = new Configuration();
    List<LensDriver> testDrivers = new ArrayList<LensDriver>();
    MockDriver mdriver = new MockDriver();
    mdriver.configure(conf);
    testDrivers.add(mdriver);
    conf.setBoolean(LensConfConstants.ENABLE_QUERY_METRICS, false);
    MockQueryContext ctx = new MockQueryContext("mock query", new LensConf(), conf, testDrivers);
    Assert.assertNull(ctx.getConf().get(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY));
    Assert.assertNull(ctx.getDriverConf(mdriver).get(LensConfConstants.QUERY_METRIC_DRIVER_STACK_NAME));
  }

  @Test
  public void testEstimateGauges() throws LensException {
    Configuration conf = new Configuration();
    List<LensDriver> testDrivers = new ArrayList<LensDriver>();
    MockDriver mdriver = new MockDriver();
    mdriver.configure(conf);
    testDrivers.add(mdriver);
    conf.set(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, TestAbstractQueryContext.class.getSimpleName());
    MockQueryContext ctx = new MockQueryContext("mock query", new LensConf(), conf, testDrivers);
    ctx.estimateCostForDrivers();
    MetricRegistry reg = LensMetricsRegistry.getStaticRegistry();

    Assert.assertTrue(reg.getGauges().keySet().containsAll(Arrays.asList(
      "lens.MethodMetricGauge.TestAbstractQueryContext-MockDriver-driverEstimate")));
  }

  @Test
  public void testTransientState() throws LensException, IOException, ClassNotFoundException {
    Configuration conf = new Configuration();
    List<LensDriver> testDrivers = new ArrayList<LensDriver>();
    MockDriver mdriver = new MockDriver();
    mdriver.configure(conf);
    testDrivers.add(mdriver);
    MockQueryContext ctx = new MockQueryContext("mock query", new LensConf(), conf, testDrivers);
    ByteArrayOutputStream bios = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bios);
    byte[] ctxBytes = null;
    try {
      out.writeObject(ctx);
      ctxBytes = bios.toByteArray();
    } finally {
      out.close();
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(ctxBytes);
    ObjectInputStream in = new ObjectInputStream(bais);
    MockQueryContext ctxRead = null;
    try {
      ctxRead = (MockQueryContext) in.readObject();
    } finally {
      in.close();
    }
    ctxRead.initTransientState();
    ctxRead.setConf(conf);
    Assert.assertNotNull(ctxRead.getHiveConf());
  }
}
