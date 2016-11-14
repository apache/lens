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
package org.apache.lens.server.api.driver;

import static org.apache.lens.server.api.LensConfConstants.DRIVER_PFX;

import static org.testng.Assert.*;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DriverConfigurationTest {
  public static final String DRIVER_TYPE = "type";
  public static final String DRIVER_CLASS_TYPE = "mock";
  public static final String KEY = "key";
  public static final String VALUE = "value";
  public static final String[] PREFIXES = new String[]{
    DRIVER_PFX + DRIVER_TYPE + ".",
    DRIVER_PFX + DRIVER_CLASS_TYPE + ".",
    DRIVER_PFX,
    "",
  };

  @DataProvider
  public Object[][] keyData() {
    Object[][] data = new Object[16][2];
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 4; j++) {
        data[i * 4 + j][0] = PREFIXES[i] + KEY;
        data[i * 4 + j][1] = PREFIXES[j] + KEY;
      }
    }
    return data;
  }

  @Test(dataProvider = "keyData")
  public void testSetAndGet(String keyToSet, String keyToGet) {
    DriverConfiguration conf = new DriverConfiguration(DRIVER_TYPE, MockDriver.class);
    conf.set(keyToSet, VALUE);
    assertEquals(conf.get(keyToGet), VALUE);
  }
}
