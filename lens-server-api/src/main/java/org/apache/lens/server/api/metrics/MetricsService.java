package org.apache.lens.server.api.metrics;

/*
 * #%L
 * Grill API for server and extensions
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


public interface MetricsService {
  public static final String NAME = "metrics";
  
  /**
   * Increment a counter with the given name
   * Actual name of the counter will be <pre>MetricRegistry.name(MetricsService.class, counter)<pre>
   * @param counter
   */
  public void incrCounter(String counter);
  
  /**
   * Increment a counter with the name constructed using given class and counter name
   * Actual name of the counter will be <pre>MetricRegistry.name(cls, counter)<pre>
   * @param counter
   * @param cls Class of the counter for namespacing the counter
   */
  public void incrCounter(Class<?> cls, String counter);
  
  /**
   * Decrement a counter with the name costructed using given class and counter name
   * Actual name of the counter will be <pre>MetricRegistry.name(cls, counter)<pre>
   * @param cls Class of the counter for namespacing of counters
   * @param counter
   */
  public void decrCounter(Class<?> cls, String counter);
  
  /**
   * Decrement a counter with the given name
   * Actual name of the counter will be <pre>MetricRegistry.name(MetricsService.class, counter)<pre>
   * @param counter
   */
  public void decrCounter(String counter);
  
  /**
   * Get current value of the counter
   */
  public long getCounter(String counter);
  
  /**
   * Get current value of the counter
   */
  public long getCounter(Class<?> cls, String counter);
  
  /**
   * Query engine counter names
   */
  public static final String CANCELLED_QUERIES = "cancelled-queries";
  public static final String FAILED_QUERIES = "failed-queries";
  public static final String ACCEPTED_QUERIES = "accepted-queries";
  
  /**
   * Query engine gauge names
   */
  public static final String QUEUED_QUERIES = "queued-queries";
  public static final String RUNNING_QUERIES = "running-queries";
  public static final String FINISHED_QUERIES = "finished-queries";
  
  public long getQueuedQueries();
  public long getRunningQueries();
  public long getFinishedQueries();
  
  public long getTotalAcceptedQueries();
  public long getTotalSuccessfulQueries();
  public long getTotalFinishedQueries();
  public long getTotalCancelledQueries();
  public long getTotalFailedQueries();
  public void publishReport();
}
