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
package org.apache.lens.cube.metadata;

import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.Random;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class UpdatePeriodTest {
  public static class RandomDateGenerator {
    Random random = new Random();
    long begin = 1377424977000L;
    long end = 1598349777000L;

    public Date nextDate() {
      return new Date(begin + (random.nextLong() % (end - begin)));
    }
  }

  @DataProvider(name = "update-periods")
  public static Object[][] provideUpdatePeriods() {
    UpdatePeriod[] values = UpdatePeriod.values();
    Object[][] ret = new Object[values.length][1];
    for (int i = 0; i < values.length; i++) {
      ret[i][0] = values[i];
    }
    return ret;
  }

  @Test(dataProvider = "update-periods")
  public void testFormat(UpdatePeriod period) throws Exception {
    RandomDateGenerator randomDateGenerator = new RandomDateGenerator();
    for (int i = 0; i < 5000; i++) {
      Date randomDate = randomDateGenerator.nextDate();
      randomDate = period.truncate(randomDate);
      assertEquals(randomDate, period.parse(period.format(randomDate)));
    }
  }
}
