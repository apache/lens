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
package org.apache.lens.server.api.metrics;

import org.apache.lens.server.api.LensConfConstants;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

/**
 * Tests for method metrics factory
 */
public class TestMethodMetricsFactory {

  @Test
  public void testMetricGauge() throws InterruptedException {
    Configuration conf = new Configuration();
    MethodMetricsContext mg = MethodMetricsFactory.createMethodGauge(conf, false, "nogauge");
    Assert.assertNotNull(mg);
    Assert.assertTrue(mg instanceof DisabledMethodMetricsContext);
    conf.set(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, "TestMethodMetricsFactory");
    conf.set(LensConfConstants.QUERY_METRIC_DRIVER_STACK_NAME, "TestMethodMetricsFactoryStackName");
    mg = MethodMetricsFactory.createMethodGauge(conf, false, "nostackgauge");
    Assert.assertNotNull(mg);
    Assert.assertTrue(mg instanceof MethodMetricGauge);
    Thread.sleep(1);
    mg.markSuccess();
    MetricRegistry reg = LensMetricsRegistry.getStaticRegistry();

    Assert.assertTrue(reg.getGauges().keySet().contains(
      "lens.MethodMetricGauge.TestMethodMetricsFactory-nostackgauge"));
    // assert gauge value. It will be in nano seconds
    Assert.assertTrue(((Long)reg.getGauges().get(
      "lens.MethodMetricGauge.TestMethodMetricsFactory-nostackgauge").getValue()) > 1000000);

    mg = MethodMetricsFactory.createMethodGauge(conf, true, "stackgauge");
    Assert.assertNotNull(mg);
    Thread.sleep(1);
    mg.markSuccess();
    Assert.assertTrue(reg.getGauges().keySet().contains(
      "lens.MethodMetricGauge.TestMethodMetricsFactoryStackName-stackgauge"));
    Assert.assertTrue(((Long)reg.getGauges().get(
      "lens.MethodMetricGauge.TestMethodMetricsFactoryStackName-stackgauge").getValue()) > 1000000);
  }
}
