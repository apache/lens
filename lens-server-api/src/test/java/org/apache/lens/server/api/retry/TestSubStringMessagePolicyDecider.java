/**
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

import org.apache.lens.server.api.LensConfConstants;

import org.apache.hadoop.conf.Configuration;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestSubStringMessagePolicyDecider {
  public void testConfiguredPolicyDecider() {
    Configuration conf = new Configuration();
    conf.set(LensConfConstants.RETRY_MESSAGE_MAP,
      "this string can be anywhere=org.apache.lens.server.api.retry.ImmediateRetryHandler(2),"
        + "Another Message=org.apache.lens.server.api.retry.FibonacciExponentialBackOffRetryHandler(3 1 1),"
        + "Third Message=org.apache.lens.server.api.retry.ImmediateRetryHandler(3)");

    SubstringMessagePolicyDecider decider = new SubstringMessagePolicyDecider();
    decider.setConf(conf);
    BackOffRetryHandler handler = decider
      .decidePolicy("abcd random messag )" + "Das__DAS this string can be anywhere and then some other string");
    FailureContext fc1 = new FailureContext() {
      @Override
      public long getLastFailedTime() {
        return 0;
      }

      @Override
      public int getFailCount() {
        return 1;
      }
    };

    FailureContext fc2 = new FailureContext() {
      @Override
      public long getLastFailedTime() {
        return 0;
      }

      @Override
      public int getFailCount() {
        return 4;
      }
    };
    Assert.assertEquals(handler instanceof ImmediateRetryHandler, true);
    Assert.assertEquals(handler.hasExhaustedRetries(fc1), false);
    Assert.assertEquals(handler.hasExhaustedRetries(fc2), true);

    handler = decider.decidePolicy("some error message string Another Message here there and everywhere");
    Assert.assertEquals(handler instanceof FibonacciExponentialBackOffRetryHandler, true);

    handler = decider.decidePolicy("some error message string Third Message here there and everywhere");
    Assert.assertEquals(handler instanceof ImmediateRetryHandler, true);
    Assert.assertEquals(handler.hasExhaustedRetries(fc1), false);
    Assert.assertEquals(handler.hasExhaustedRetries(fc2), true);

    handler = decider.decidePolicy("this should return null");
    Assert.assertNull(handler);
  }
}

