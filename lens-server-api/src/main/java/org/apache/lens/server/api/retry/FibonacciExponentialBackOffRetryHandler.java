/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.api.retry;

import static com.google.common.base.Preconditions.checkArgument;


/**
 * A exponential backoff retry handler.
 *
 * It allows the the failures to be retried at a next update time, which can increase exponentially.
 *
 */
public class FibonacciExponentialBackOffRetryHandler<FC extends FailureContext> implements BackOffRetryHandler<FC> {
  final int[] fibonacci;
  final long maxDelay;
  final long waitMillis;

  public FibonacciExponentialBackOffRetryHandler(int numRetries, long maxDelay, long waitMillis) {
    checkArgument(numRetries > 2);
    fibonacci = new int[numRetries];
    fibonacci[0] = 1;
    fibonacci[1] = 1;
    for(int i = 2; i < numRetries; ++i) {
      fibonacci[i] = fibonacci[i-1] + fibonacci[i-2];
    }
    this.maxDelay = maxDelay;
    this.waitMillis = waitMillis;
  }

  public boolean canTryOpNow(FC failContext) {
    synchronized (failContext) {
      if (failContext.getFailCount() != 0) {
        long now = System.currentTimeMillis();
        if (now < getOperationNextTime(failContext)) {
          return false;
        }
      }
      return true;
    }
  }

  public long getOperationNextTime(FC failContext) {
    synchronized (failContext) {
      if (failContext.getFailCount() >= fibonacci.length) {
        return failContext.getLastFailedTime() + maxDelay;
      }
      long delay = Math.min(maxDelay, fibonacci[failContext.getFailCount()] * waitMillis);
      return failContext.getLastFailedTime() + delay;
    }
  }

  public boolean hasExhaustedRetries(FC failContext) {
    synchronized (failContext) {
      if (failContext.getFailCount() >= fibonacci.length) {
        return true;
      }
      return false;
    }
  }
}
