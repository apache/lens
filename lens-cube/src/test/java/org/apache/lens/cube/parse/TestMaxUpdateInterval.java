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

package org.apache.lens.cube.parse;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.lens.cube.metadata.CubeFactTable;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.testng.Assert;
import org.testng.annotations.Test;

/*
 * Unit test for maxUpdateIntervalIn method in CubeFactTable
 */
public class TestMaxUpdateInterval {
  public static final String[] testpairs = { "2013-Jan-01", "2013-Jan-31", "2013-Jan-01", "2013-May-31", "2013-Jan-01",
      "2013-Dec-31", "2013-Feb-01", "2013-Apr-25", "2012-Feb-01", "2013-Feb-01", "2011-Feb-01", "2013-Feb-01",
      "2013-Feb-01", "2013-Feb-21", "2013-Feb-01", "2013-Feb-4" };

  public static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MMM-dd");

  private final Date pairs[];

  public TestMaxUpdateInterval() {
    pairs = new Date[testpairs.length];
    for (int i = 0; i < testpairs.length; i++) {
      try {
        pairs[i] = DATE_FMT.parse(testpairs[i]);
        System.out.println(pairs[i].toString());
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testMaxUpdatePeriodInInterval() throws Exception {
    Set<UpdatePeriod> allPeriods = new HashSet<UpdatePeriod>();
    allPeriods.addAll(Arrays.asList(UpdatePeriod.values()));

    int i = 0;
    Assert.assertEquals(UpdatePeriod.WEEKLY, CubeFactTable.maxIntervalInRange(pairs[i], pairs[i + 1], allPeriods),
        "2013-Jan-01 to 2013-Jan-31");

    i += 2;
    Assert.assertEquals(UpdatePeriod.QUARTERLY, CubeFactTable.maxIntervalInRange(pairs[i], pairs[i + 1], allPeriods),
        "2013-Jan-01 to 2013-May-31");

    i += 2;
    Assert.assertEquals(UpdatePeriod.QUARTERLY, CubeFactTable.maxIntervalInRange(pairs[i], pairs[i + 1], allPeriods),
        "2013-Jan-01 to 2013-Dec-31");

    i += 2;
    Assert.assertEquals(UpdatePeriod.MONTHLY, CubeFactTable.maxIntervalInRange(pairs[i], pairs[i + 1], allPeriods),
        "2013-Feb-01 to 2013-Apr-25");

    i += 2;
    Assert.assertEquals(UpdatePeriod.QUARTERLY, CubeFactTable.maxIntervalInRange(pairs[i], pairs[i + 1], allPeriods),
        "2012-Feb-01 to 2013-Feb-01");

    i += 2;
    Assert.assertEquals(UpdatePeriod.YEARLY, CubeFactTable.maxIntervalInRange(pairs[i], pairs[i + 1], allPeriods),
        "2011-Feb-01 to 2013-Feb-01");

    i += 2;
    Assert.assertEquals(UpdatePeriod.WEEKLY, CubeFactTable.maxIntervalInRange(pairs[i], pairs[i + 1], allPeriods),
        "2013-Feb-01 to 2013-Feb-21");

    i += 2;
    Assert.assertEquals(UpdatePeriod.DAILY, CubeFactTable.maxIntervalInRange(pairs[i], pairs[i + 1], allPeriods),
        "2013-Feb-01 to 2013-Feb-4");
  }

}
