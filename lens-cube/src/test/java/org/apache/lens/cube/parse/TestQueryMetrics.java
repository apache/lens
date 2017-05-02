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

package org.apache.lens.cube.parse;

import static org.apache.lens.cube.metadata.DateFactory.TWO_DAYS_RANGE;

import java.util.Set;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.metrics.LensMetricsRegistry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import jersey.repackaged.com.google.common.collect.Sets;

public class TestQueryMetrics extends TestQueryRewrite {

  @Test
  public void testMethodGauges() throws Exception {
    Configuration conf = new Configuration();
    conf.set(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, TestQueryMetrics.class.getSimpleName());
    conf.set(LensConfConstants.QUERY_METRIC_DRIVER_STACK_NAME, "testCubeRewriteStackName");

    rewriteCtx("select" + " SUM(msr2) from testCube where " + TWO_DAYS_RANGE, conf);
    MetricRegistry reg = LensMetricsRegistry.getStaticRegistry();
    CubeQueryRewriter cubeQueryRewriter = new CubeQueryRewriter(new Configuration(), new HiveConf());
    Set<String> expected = Sets.newHashSet();
    int index = 0;
    for (ContextRewriter contextRewriter : cubeQueryRewriter.getRewriters()) {
      expected.add("lens.MethodMetricGauge.testCubeRewriteStackName-"
        + contextRewriter.getClass().getName() + "-ITER-" + index);
      index++;
    }
    Assert.assertEquals(reg.getGauges().keySet(), expected);
  }
}
