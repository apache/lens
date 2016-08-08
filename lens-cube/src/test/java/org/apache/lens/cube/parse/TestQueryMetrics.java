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

import java.util.Arrays;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.metrics.LensMetricsRegistry;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

public class TestQueryMetrics extends TestQueryRewrite {

  @Test
  public void testMethodGauges() throws Exception {
    Configuration conf = new Configuration();
    conf.set(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, TestQueryMetrics.class.getSimpleName());
    conf.set(LensConfConstants.QUERY_METRIC_DRIVER_STACK_NAME, "testCubeRewriteStackName");

    rewriteCtx("select" + " SUM(msr2) from testCube where " + TWO_DAYS_RANGE, conf);
    MetricRegistry reg = LensMetricsRegistry.getStaticRegistry();

    Assert.assertTrue(reg.getGauges().keySet().containsAll(Arrays.asList(
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.AggregateResolver-ITER-6",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.AliasReplacer-ITER-1",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.CandidateTableResolver-ITER-11",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.CandidateTableResolver-ITER-5",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.ColumnResolver-ITER-0",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.DenormalizationResolver-ITER-16",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.DenormalizationResolver-ITER-3",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.ExpressionResolver-ITER-17",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.ExpressionResolver-ITER-2",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.FieldValidator-ITER-8",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.GroupbyResolver-ITER-7",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.JoinResolver-ITER-9",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.LeastPartitionResolver-ITER-19",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.LightestDimensionResolver-ITER-20",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.LightestFactResolver-ITER-18",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.MaxCoveringFactResolver-ITER-14",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.StorageTableResolver-ITER-12",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.StorageTableResolver-ITER-13",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.StorageTableResolver-ITER-15",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.TimeRangeChecker-ITER-10",
        "lens.MethodMetricGauge.testCubeRewriteStackName-org.apache.lens.cube.parse.TimerangeResolver-ITER-4")
    ), reg.getGauges().keySet().toString());
  }
}
