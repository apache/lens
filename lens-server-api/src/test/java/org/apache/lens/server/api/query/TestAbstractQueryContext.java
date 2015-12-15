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

import static org.apache.lens.api.Priority.HIGH;
import static org.apache.lens.server.api.LensConfConstants.*;
import static org.apache.lens.server.api.LensServerAPITestUtil.getConfiguration;

import static org.testng.Assert.*;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import org.apache.lens.api.Priority;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.driver.MockDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.LensMetricsRegistry;
import org.apache.lens.server.api.query.priority.MockQueryPriorityDecider;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

/**
 * Tests for abstract query context
 */
public class TestAbstractQueryContext {

  @Test
  public void testMetricsConfigEnabled() throws LensException {
    MockQueryContext ctx = new MockQueryContext(getConfiguration(ENABLE_QUERY_METRICS, true));
    String uniqueMetridId = ctx.getConf().get(QUERY_METRIC_UNIQUE_ID_CONF_KEY);
    assertNotNull(uniqueMetridId);
    assertEquals(ctx.getSelectedDriverConf().get(QUERY_METRIC_DRIVER_STACK_NAME),
      uniqueMetridId + "-" + new MockDriver().getFullyQualifiedName());
  }

  @Test
  public void testMetricsConfigDisabled() throws LensException {
    MockQueryContext ctx = new MockQueryContext(getConfiguration(ENABLE_QUERY_METRICS, false));
    assertNull(ctx.getConf().get(QUERY_METRIC_UNIQUE_ID_CONF_KEY));
    assertNull(ctx.getSelectedDriverConf().get(QUERY_METRIC_DRIVER_STACK_NAME));
  }

  @Test
  public void testEstimateGauges() throws LensException {
    MockQueryContext ctx = new MockQueryContext(getConfiguration(QUERY_METRIC_UNIQUE_ID_CONF_KEY,
      TestAbstractQueryContext.class.getSimpleName()));
    ctx.estimateCostForDrivers();
    MetricRegistry reg = LensMetricsRegistry.getStaticRegistry();
    assertTrue(reg.getGauges().keySet().containsAll(Arrays.asList(
      "lens.MethodMetricGauge.TestAbstractQueryContext-"+new MockDriver().getFullyQualifiedName()+"-driverEstimate")));
  }

  @Test
  public void testTransientState() throws LensException, IOException, ClassNotFoundException {
    MockQueryContext ctx = new MockQueryContext();
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
    ctxRead.setConf(ctx.getConf());
    assertNotNull(ctxRead.getHiveConf());
  }

  @Test
  public void testPrioritySetting() throws LensException {
    MockQueryContext ctx = new MockQueryContext();
    Priority p = ctx.decidePriority(ctx.getSelectedDriver(), new MockQueryPriorityDecider());
    assertEquals(p, HIGH);
    assertEquals(ctx.getPriority(), HIGH);
  }

  @Test
  public void testReadAndWriteExternal() throws Exception {
    Configuration conf = new Configuration();
    List<LensDriver> drivers = MockQueryContext.getDrivers(conf);
    MockQueryContext ctx = new MockQueryContext(drivers);
    String driverQuery = "driver query";
    ctx.setSelectedDriverQuery(driverQuery);
    assertNotNull(ctx.getSelectedDriverQuery());
    assertEquals(ctx.getSelectedDriverQuery(), driverQuery);
    assertEquals(ctx.getDriverQuery(ctx.getSelectedDriver()), driverQuery);

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
    ctxRead.setConf(ctx.getConf());
    assertNotNull(ctxRead.getHiveConf());
    assertNotNull(ctxRead.getSelectedDriverQuery());
    assertEquals(ctxRead.getSelectedDriverQuery(), driverQuery);

    //Create DriverSelectorQueryContext by passing all the drivers and the user query
    DriverSelectorQueryContext driverCtx = new DriverSelectorQueryContext(ctxRead.getUserQuery(), ctxRead.getConf(),
      drivers);
    ctxRead.setDriverContext(driverCtx);
    ctxRead.getDriverContext().setSelectedDriver(ctx.getSelectedDriver());
    ctxRead.setDriverQuery(ctxRead.getSelectedDriver(), ctxRead.getSelectedDriverQuery());
    assertEquals(ctxRead.getDriverQuery(ctxRead.getSelectedDriver()), driverQuery);
  }
}
