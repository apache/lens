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

import java.util.Calendar;
import java.util.Date;

import org.apache.lens.api.LensException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestTimePartition {
  public static Date NOW = new Date();

  @Test
  public void test() throws LensException {
    for (UpdatePeriod up : UpdatePeriod.values()) {
      String nowStr = up.format().format(NOW);
      TimePartition nowPartition = TimePartition.of(up, NOW);
      TimePartition nowStrPartition = TimePartition.of(up, nowStr);
      Assert.assertEquals(nowPartition, nowStrPartition);
      Assert.assertTrue(nowPartition.next().after(nowPartition));
      Assert.assertTrue(nowPartition.previous().before(nowPartition));
      Assert.assertEquals(getLensExceptionFromPartitionParsing(up, "garbage").getMessage(),
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
        Assert.assertEquals(getLensExceptionFromPartitionParsing(up2, nowStr).getMessage(),
          TimePartition.getWrongUpdatePeriodMessage(up2, nowStr));
      }
    }
  }

  private LensException getLensExceptionFromPartitionParsing(UpdatePeriod up, String dateStr) {
    try {
      TimePartition.of(up, dateStr);
      Assert.fail("Should have thrown LensException");
    } catch (LensException e) {
      return e;
    }
    return null; // redundant
  }

  private LensException getLensExceptionFromPartitionParsing(UpdatePeriod up, Date date) {
    try {
      TimePartition.of(up, date);
      Assert.fail("Should have thrown LensException");
    } catch (LensException e) {
      return e;
    }
    return null; // redundant
  }

  private LensException getLensExceptionInPartitionRangeCreation(TimePartition begin, TimePartition end) {
    try {
      TimePartition.TimePartitionRange range = begin.rangeUpto(end);
      Assert.fail("Should have thrown LensException. Can't create range: " + range);
    } catch (LensException e) {
      return e;
    }
    return null; // redundant
  }

  public static Date timeAtDiff(Date date, UpdatePeriod period, int d) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.add(period.calendarField(), d);
    return cal.getTime();
  }

  @Test
  public void testTimeRange() throws LensException {
    for (UpdatePeriod up : UpdatePeriod.values()) {
      TimePartition nowPartition = TimePartition.of(up, NOW);
      TimePartition tenLater = TimePartition.of(up, timeAtDiff(NOW, up, 10));
      getLensExceptionInPartitionRangeCreation(tenLater, nowPartition);
      TimePartition.TimePartitionRange range = nowPartition.rangeUpto(tenLater);
      Assert.assertEquals(range, tenLater.rangeFrom(nowPartition));
      if (up != UpdatePeriod.QUARTERLY) {
        Assert.assertEquals(range.size(), 10);
        Assert.assertEquals(nowPartition.singletonRange().size(), 1);
      }
      Assert.assertTrue(range.contains(nowPartition));
      Assert.assertFalse(range.contains(tenLater));
      String nowStr = nowPartition.getDateString();
      String tenLaterStr = tenLater.getDateString();
      Assert.assertEquals(TimePartition.TimePartitionRange.parseFrom(up, nowStr, tenLaterStr), range);
      Assert.assertEquals(TimePartition.TimePartitionRange.parseFrom(up, "[" + nowStr, tenLaterStr + ")"), range);
      Assert.assertEquals(TimePartition.TimePartitionRange.parseFrom(up, "[" + nowStr, tenLaterStr + "]"),
        nowPartition.rangeUpto(tenLater.next()));
      Assert.assertEquals(TimePartition.TimePartitionRange.parseFrom(up, "(" + nowStr, tenLaterStr + "]"),
        nowPartition.next().rangeUpto(
          tenLater.next()));
      Assert.assertEquals(TimePartition.TimePartitionRange.parseFrom(up, "(" + nowStr, tenLaterStr + ")"),
        nowPartition.next().rangeUpto(tenLater));
    }
    getLensExceptionInPartitionRangeCreation(TimePartition.of(UpdatePeriod.HOURLY, NOW), TimePartition.of(
      UpdatePeriod.DAILY, NOW));
  }
}
