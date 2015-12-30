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

import static org.testng.Assert.*;

import java.util.Calendar;
import java.util.Date;

import org.apache.lens.server.api.error.LensException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestTimePartition {
  public static final Date NOW = new Date();

  @DataProvider(name = "update-periods")
  public Object[][] provideUpdatePeriods() {
    return UpdatePeriodTest.provideUpdatePeriods();
  }

  @Test(dataProvider = "update-periods")
  public void test(UpdatePeriod up) throws LensException {
    // Normal date object parsable
    String nowStr = up.format(NOW);
    // Create partition by date object or it's string representation -- both should be same.
    TimePartition nowPartition = TimePartition.of(up, NOW);
    TimePartition nowStrPartition = TimePartition.of(up, nowStr);
    assertEquals(nowPartition, nowStrPartition);

    // Test next and previous
    assertTrue(nowPartition.next().after(nowPartition));
    assertTrue(nowPartition.previous().before(nowPartition));

    // date parse failures should give lens exception
    assertEquals(getLensExceptionFromPartitionParsing(up, "garbage").getMessage(),
      TimePartition.getWrongUpdatePeriodMessage(up, "garbage"));
    getLensExceptionFromPartitionParsing(up, (Date) null);
    getLensExceptionFromPartitionParsing(up, (String) null);
    getLensExceptionFromPartitionParsing(up, "");

    // parse with other update periods
    for (UpdatePeriod up2 : UpdatePeriod.values()) {
      // handles the equality case and the case where monthly-quarterly have same format strings.
      if (up.formatStr().equals(up2.formatStr())) {
        continue;
      }
      // Parsing a string representation with differnet update period should give lens exception.
      assertEquals(getLensExceptionFromPartitionParsing(up2, nowStr).getMessage(),
        TimePartition.getWrongUpdatePeriodMessage(up2, nowStr));
    }
  }

  private LensException getLensExceptionFromPartitionParsing(UpdatePeriod up, String dateStr) {
    try {
      TimePartition.of(up, dateStr);
      fail("Should have thrown LensException");
    } catch (LensException e) {
      return e;
    }
    return null; // redundant
  }

  private LensException getLensExceptionFromPartitionParsing(UpdatePeriod up, Date date) {
    try {
      TimePartition.of(up, date);
      fail("Should have thrown LensException");
    } catch (LensException e) {
      return e;
    }
    return null; // redundant
  }


  public static Date timeAtDiff(Date date, UpdatePeriod period, int d) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    if (period.equals(UpdatePeriod.QUARTERLY)) {
      d *= 3;
    }
    cal.add(period.calendarField(), d);
    return cal.getTime();
  }

  @Test
  public void testTimeRange() throws LensException {
    // test for all update periods
    for (UpdatePeriod up : UpdatePeriod.values()) {
      // create two partition of different time
      TimePartition nowPartition = TimePartition.of(up, NOW);
      TimePartition tenLater = TimePartition.of(up, timeAtDiff(NOW, up, 10));

      // a.upto(b) == b.from(a)
      TimePartitionRange range = nowPartition.rangeUpto(tenLater);
      assertEquals(range, tenLater.rangeFrom(nowPartition));
      // size check
      assertEquals(range.size(), 10);
      // test singleton range
      assertEquals(nowPartition.singletonRange().size(), 1);
      // test begin belongs to [begin, end) and end doesn't belong
      assertTrue(range.contains(nowPartition));
      assertFalse(range.contains(tenLater));
      // test partition parsing for string arguments.
      // a,b == [a,b)
      // Other possible arguments: [a,b), [a,b], (a,b), (a,b]
      String nowStr = nowPartition.getDateString();
      String tenLaterStr = tenLater.getDateString();
      assertEquals(TimePartitionRange.parseFrom(up, nowStr, tenLaterStr), range);
      assertEquals(TimePartitionRange.parseFrom(up, "[" + nowStr, tenLaterStr + ")"), range);
      assertEquals(TimePartitionRange.parseFrom(up, "[" + nowStr, tenLaterStr + "]"),
        nowPartition.rangeUpto(tenLater.next()));
      assertEquals(TimePartitionRange.parseFrom(up, "(" + nowStr, tenLaterStr + "]"),
        nowPartition.next().rangeUpto(
          tenLater.next()));
      assertEquals(TimePartitionRange.parseFrom(up, "(" + nowStr, tenLaterStr + ")"),
        nowPartition.next().rangeUpto(tenLater));
    }
  }

  @Test(expectedExceptions = LensException.class)
  public void testPartitionRangeValidity() throws LensException {
    // begin and end partitions should follow begin <= end
    TimePartition.of(UpdatePeriod.HOURLY, NOW)
      .rangeFrom(TimePartition.of(UpdatePeriod.HOURLY, timeAtDiff(NOW, UpdatePeriod.HOURLY, 10)));
  }

  @Test(expectedExceptions = LensException.class)
  public void testTimeRangeCreationWithDifferentUpdatePeriod() throws LensException {
    // begin and end partitions should have same update period for range creation to succeed.
    TimePartition.of(UpdatePeriod.HOURLY, NOW).rangeUpto(TimePartition.of(UpdatePeriod.DAILY, NOW));
  }

}
