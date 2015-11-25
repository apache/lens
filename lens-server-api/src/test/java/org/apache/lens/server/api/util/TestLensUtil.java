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
package org.apache.lens.server.api.util;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for lens util
 */
public class TestLensUtil {

  @Test
  public void testExceptionCauseMessage() {
    Throwable th = null;

    try {
      try {
        throw new IOException("base cause");
      } catch (IOException e) {
        throw new RuntimeException("run time exception", e);
      }
    } catch (Exception e) {
      th = e;
    }
    Assert.assertEquals(LensUtil.getCauseMessage(th), "base cause");

    // no message in base exception
    try {
      try {
        throw new IOException();
      } catch (IOException e) {
        throw new RuntimeException("run time exception", e);
      }
    } catch (Exception e) {
      th = e;
    }
    Assert.assertEquals(LensUtil.getCauseMessage(th), "run time exception");
  }
}
