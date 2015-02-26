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
package org.apache.lens.server.metrics;

import org.apache.lens.server.api.metrics.MethodMetricsContext;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import lombok.NonNull;

/**
 * metrics measuring for resource methods. Every resource method has one timer and two meters associated with it. One
 * meter is marking number of invocations of the method, another is marking number of invocations where exception
 * occurred.
 */

public class MethodMetrics {
  // Corresponding meter and timer counts are same, but they are marked at different times.
  // meter is marked at the start of event, timer is marked at the end of the event.
  private final Meter meter;
  private final Timer successTimer;
  private final Timer exceptionTimer;

  public MethodMetrics(
    @NonNull Meter meter, @NonNull Timer successTimer, @NonNull Timer exceptionTimer) {
    this.meter = meter;
    this.successTimer = successTimer;
    this.exceptionTimer = exceptionTimer;
  }

  /**
   * Inner class instances handle contexts for the same instance of MethodMetrics. This is useful since one method can
   * be executing multiple times in parallel. But the contexts will be different.
   */
  public class Context implements MethodMetricsContext {
    private Timer.Context successTimerContext;
    private Timer.Context exceptionTimerContext;

    private Context() {
      meter.mark();
      successTimerContext = successTimer.time();
      exceptionTimerContext = exceptionTimer.time();
    }

    @Override
    public void markError() {
      exceptionTimerContext.close();
    }

    @Override
    public void markSuccess() {
      successTimerContext.close();
    }
  }

  /** Create and return new context since an execution of this method has started */
  public MethodMetricsContext newContext() {
    return new Context();
  }

  public long getCount() {
    return meter.getCount();
  }

  public long getSuccessCount() {
    return successTimer.getCount();
  }

  public long getErrorCount() {
    return exceptionTimer.getCount();
  }
}
