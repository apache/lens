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

import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.ResourceMethod;

/**
 * The Interface MetricsService.
 */
public interface MetricsService {

  /**
   * The Constant NAME.
   */
  String NAME = "metrics";

  /**
   * Increment a counter with the given name Actual name of the counter will be
   * <p/>
   * <pre>MetricRegistry.name(MetricsService.class, counter)
   * <p/>
   * <pre>
   *
   * @param counter the counter
   */
  void incrCounter(String counter);

  /**
   * Increment a counter with the name constructed using given class and counter name Actual name of the counter will
   * be
   * <p/>
   * <pre>MetricRegistry.name(cls, counter)
   * <p/>
   * <pre>
   *
   * @param cls     Class of the counter for namespacing the counter
   * @param counter the counter
   */
  void incrCounter(Class<?> cls, String counter);

  /**
   * Decrement a counter with the name costructed using given class and counter name Actual name of the counter will be
   * <p/>
   * <pre>MetricRegistry.name(cls, counter)
   * <p/>
   * <pre>
   *
   * @param cls     Class of the counter for namespacing of counters
   * @param counter the counter
   */
  void decrCounter(Class<?> cls, String counter);

  /**
   * Decrement a counter with the given name Actual name of the counter will be
   * <p/>
   * <pre>MetricRegistry.name(MetricsService.class, counter)
   * <p/>
   * <pre>
   *
   * @param counter the counter
   */
  void decrCounter(String counter);

  /**
   * Get current value of the counter.
   *
   * @param counter the counter
   * @return the counter
   */
  long getCounter(String counter);

  /**
   * Get current value of the counter.
   *
   * @param cls     the cls
   * @param counter the counter
   * @return the counter
   */
  long getCounter(Class<?> cls, String counter);

  /**
   * Query engine counter names.
   */
  String CANCELLED_QUERIES = "cancelled-queries";

  /**
   * The Constant FAILED_QUERIES.
   */
  String FAILED_QUERIES = "failed-queries";

  /**
   * The Constant ACCEPTED_QUERIES.
   */
  String ACCEPTED_QUERIES = "accepted-queries";

  /**
   * Query engine gauge names.
   */
  String QUEUED_QUERIES = "queued-queries";

  /**
   * The Constant RUNNING_QUERIES.
   */
  String RUNNING_QUERIES = "running-queries";

  /**
   * The Constant FINISHED_QUERIES.
   */
  String FINISHED_QUERIES = "finished-queries";

  long getQueuedQueries();

  long getRunningQueries();

  long getFinishedQueries();

  long getTotalAcceptedQueries();

  long getTotalSuccessfulQueries();

  long getTotalFinishedQueries();

  long getTotalCancelledQueries();

  long getTotalFailedQueries();

  /**
   * Publish report.
   */
  void publishReport();

  /**
   * API method for getting metrics measuring context for given resource method and container request
   *
   * @param method           the resource method
   * @param containerRequest container request
   * @return method metrics context.
   * @see org.glassfish.jersey.server.ContainerRequest
   * @see org.glassfish.jersey.server.model.ResourceMethod
   * @see MethodMetricsContext
   */
  MethodMetricsContext getMethodMetricsContext(ResourceMethod method, ContainerRequest containerRequest);
}
