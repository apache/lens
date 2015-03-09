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

import com.codahale.metrics.MetricRegistry;

/**
 * Lens MetricsRegistry.
 */
public class LensMetricsRegistry {
  private LensMetricsRegistry() {
  }

  private static MetricRegistry metricRegistry;

  /**
   * Static instance of metrics registry. This instance is registered with reporters. For all the use of publishing
   * metrics to reporter, this should be used.
   *
   * @return LensMetricsRegistry
   */
  public static synchronized MetricRegistry getStaticRegistry() {
    if (metricRegistry == null) {
      metricRegistry = new MetricRegistry();
    }
    return metricRegistry;
  }

  /**
   * This clears the registry, would called on server stop, included only for restart tests.
   */
  static synchronized void clearRegistry() {
    metricRegistry = null;
  }
}
