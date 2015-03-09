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


import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import lombok.NonNull;

/**
 * Holds the gauge value indicating the time taken for the method.
 *
 * When we have methods which can take variable time with respect to the parameters passed, the timers available in
 * {@link MethodMetrics} (which aggregated values over all calls) does not provide information on how each call
 * performed. Having gauge for each call will solve the purpose, which resulted in this class.
 *
 * The gauge added here should be created with unique name for each call so that the gauges are not lost to
 * the latest calls.
 */
public class MethodMetricGauge implements MethodMetricsContext {
  private final long startTime;
  private long totalTime;
  private final String gaugeName;
  private final MetricRegistry metricRegistry;

  /**
   * The gauge for method time.
   *
   * @param metricRegistry The metric registry
   * @param gaugeName Gauge name.
   *  It should be unique for each creation. Callers have to take care of passing unique name
   */
  public MethodMetricGauge(@NonNull MetricRegistry metricRegistry, @NonNull String gaugeName) {
    this.startTime = System.nanoTime();
    this.gaugeName = gaugeName;
    this.metricRegistry = metricRegistry;
  }

  @Override
  public void markError() {
  }

  @Override
  public void markSuccess() {
    this.totalTime = System.nanoTime() - startTime;
    metricRegistry.register(MetricRegistry.name("lens", MethodMetricGauge.class.getSimpleName(), gaugeName),
      new Gauge<Long>() {
        @Override
        public Long getValue() {
          return totalTime;
        }
      });
  }
}
