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

import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.MONTH;

import static org.apache.lens.cube.metadata.DateUtil.*;
import static org.apache.lens.cube.metadata.UpdatePeriod.*;

import static org.apache.commons.lang.time.DateUtils.addMilliseconds;

import static org.testng.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Set;

import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.time.DateUtils;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

/**
 * Unit tests for cube DateUtil class TestDateUtil.
 */
@Slf4j
public class TestDateUtil {

  public static final String[] TEST_PAIRS = {
    "2013-Jan-01", "2013-Jan-31", "2013-Jan-01", "2013-May-31",
    "2013-Jan-01", "2013-Dec-31", "2013-Feb-01", "2013-Apr-25",
    "2012-Feb-01", "2013-Feb-01", "2011-Feb-01", "2013-Feb-01",
    "2013-Jan-02", "2013-Feb-02", "2013-Jan-02", "2013-Mar-02",
  };

  public static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy-MMM-dd");

  private Date[] pairs;

  @BeforeTest
  public void setUp() {
    pairs = new Date[TEST_PAIRS.length];
    for (int i = 0; i < TEST_PAIRS.length; i++) {
      try {
        pairs[i] = DATE_FMT.parse(TEST_PAIRS[i]);
      } catch (ParseException e) {
        log.error("Parsing exception while setup.", e);
      }
    }
  }


  @Test
  public void testMonthsBetween() throws Exception {
    int i = 0;
    assertEquals(getMonthlyCoveringInfo(pairs[i], DateUtils.round(pairs[i + 1], MONTH)),
      new CoveringInfo(1, true),
      "2013-Jan-01 to 2013-Jan-31");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], DateUtils.round(pairs[i + 1], MONTH)),
      new CoveringInfo(5, true),
      "2013-Jan-01 to 2013-May-31");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], DateUtils.round(pairs[i + 1], MONTH)),
      new CoveringInfo(12, true),
      "2013-Jan-01 to 2013-Dec-31");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(2, false),
      "2013-Feb-01 to 2013-Apr-25");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(12, true),
      "2012-Feb-01 to 2013-Feb-01");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(24, true),
      "2011-Feb-01 to 2013-Feb-01");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "2013-Jan-02 to 2013-Feb-02");

    i += 2;
    assertEquals(getMonthlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(1, false),
      "2013-Jan-02 to 2013-Mar-02");
  }

  @Test
  public void testQuartersBetween() throws Exception {
    int i = 0;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "2013-Jan-01 to 2013-Jan-31");

    i += 2;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(1, false),
      "2013-Jan-01 to 2013-May-31");

    i += 2;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], DateUtils.round(pairs[i + 1], MONTH)),
      new CoveringInfo(4, true),
      "2013-Jan-01 to 2013-Dec-31");

    i += 2;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "2013-Feb-01 to 2013-Apr-25");

    i += 2;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(3, false),
      "2012-Feb-01 to 2013-Feb-01");

    i += 2;
    assertEquals(getQuarterlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(7, false),
      "2011-Feb-01 to 2013-Feb-01");
  }

  @Test
  public void testYearsBetween() throws Exception {
    int i = 0;
    assertEquals(getYearlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "" + pairs[i] + "->" + pairs[i + 1]);

    i += 2;
    assertEquals(getYearlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "" + pairs[i] + "->" + pairs[i + 1]);

    i += 2;
    assertEquals(getYearlyCoveringInfo(pairs[i], DateUtils.round(pairs[i + 1], MONTH)),
      new CoveringInfo(1, true), ""
        + pairs[i] + "->" + pairs[i + 1]);

    i += 2;
    assertEquals(getYearlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "" + pairs[i] + "->" + pairs[i + 1]);

    i += 2;
    assertEquals(getYearlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(0, false),
      "" + pairs[i] + "->" + pairs[i + 1]);

    i += 2;
    assertEquals(getYearlyCoveringInfo(pairs[i], pairs[i + 1]), new CoveringInfo(1, false),
      "" + pairs[i] + "->" + pairs[i + 1]);
  }

  @Test
  public void testWeeksBetween() throws Exception {
    CoveringInfo weeks;

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-26"), DATE_FMT.parse("2013-Jun-2"));
    assertEquals(weeks, new CoveringInfo(1, true), "2013-May-26 to 2013-Jun-2");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-27"), DATE_FMT.parse("2013-Jun-3"));
    assertEquals(weeks, new CoveringInfo(0, false), "2013-May-26 to 2013-Jun-2");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-27"), DATE_FMT.parse("2013-Jun-9"));
    assertEquals(weeks, new CoveringInfo(1, false), "2013-May-26 to 2013-Jun-2");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-27"), DATE_FMT.parse("2013-Jun-1"));
    assertEquals(weeks, new CoveringInfo(0, false), "2013-May-27 to 2013-Jun-1");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-25"), DATE_FMT.parse("2013-Jun-2"));
    assertEquals(weeks, new CoveringInfo(1, false), "2013-May-25 to 2013-Jun-1");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-26"), DATE_FMT.parse("2013-Jun-9"));
    assertEquals(weeks, new CoveringInfo(2, true), "2013-May-26 to 2013-Jun-8");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2013-May-26"), DATE_FMT.parse("2013-Jun-10"));
    assertEquals(weeks, new CoveringInfo(2, false), "2013-May-26 to 2013-Jun-10");

    weeks = getWeeklyCoveringInfo(DATE_FMT.parse("2015-Dec-27"), DATE_FMT.parse("2016-Jan-03"));
    assertEquals(weeks, new CoveringInfo(1, true), "2015-Dec-27 to 2016-Jan-03");
  }

  @Test
  public void testNowWithGranularity() throws Exception {
    String dateFmt = "yyyy/MM/dd-HH.mm.ss.SSS";
    // Tuesday Sept 23, 2014, 12.02.05.500 pm
    String testDateStr = "2014/09/23-12.02.05.500";
    final SimpleDateFormat sdf = new SimpleDateFormat(dateFmt);
    final Date testDate = sdf.parse(testDateStr);

    System.out.print("@@ testDateStr=" + testDateStr + " parsed date=" + testDate);

    // Tests without a diff, just resolve now with different granularity
    assertEquals(testDateStr, sdf.format(resolveDate("now", testDate)));
    assertEquals("2014/01/01-00.00.00.000", sdf.format(resolveDate("now.year", testDate)));
    assertEquals("2014/09/01-00.00.00.000", sdf.format(resolveDate("now.month", testDate)));
    // Start of week resolves to Sunday
    assertEquals("2014/09/21-00.00.00.000", sdf.format(resolveDate("now.week", testDate)));
    assertEquals("2014/09/23-00.00.00.000", sdf.format(resolveDate("now.day", testDate)));
    assertEquals("2014/09/23-12.00.00.000", sdf.format(resolveDate("now.hour", testDate)));
    assertEquals("2014/09/23-12.02.00.000", sdf.format(resolveDate("now.minute", testDate)));
    assertEquals("2014/09/23-12.02.05.000", sdf.format(resolveDate("now.second", testDate)));

    // Tests with a diff
    assertEquals("2014/09/22-00.00.00.000", sdf.format(resolveDate("now.day -1day", testDate)));
    assertEquals("2014/09/23-10.00.00.000", sdf.format(resolveDate("now.hour -2hour", testDate)));
    assertEquals("2014/09/24-12.00.00.000", sdf.format(resolveDate("now.hour +24hour", testDate)));
    assertEquals("2015/01/01-00.00.00.000", sdf.format(resolveDate("now.year +1year", testDate)));
    assertEquals("2014/02/01-00.00.00.000", sdf.format(resolveDate("now.year +1month", testDate)));
  }

  @Test
  public void testFloorDate() throws ParseException {
    Date date = ABSDATE_PARSER.get().parse("2015-01-01-00:00:00,000");
    Date curDate = date;
    for (int i = 0; i < 284; i++) {
      assertEquals(getFloorDate(curDate, YEARLY), date);
      curDate = addMilliseconds(curDate, 111111111);
    }
    assertEquals(getFloorDate(curDate, YEARLY), DateUtils.addYears(date, 1));
    assertEquals(getFloorDate(date, WEEKLY), ABSDATE_PARSER.get().parse("2014-12-28-00:00:00,000"));
  }

  @Test
  public void testCeilDate() throws ParseException {
    Date date = ABSDATE_PARSER.get().parse("2015-12-26-06:30:15,040");
    assertEquals(getCeilDate(date, YEARLY), ABSDATE_PARSER.get().parse("2016-01-01-00:00:00,000"));
    assertEquals(getCeilDate(date, MONTHLY), ABSDATE_PARSER.get().parse("2016-01-01-00:00:00,000"));
    assertEquals(getCeilDate(date, DAILY), ABSDATE_PARSER.get().parse("2015-12-27-00:00:00,000"));
    assertEquals(getCeilDate(date, HOURLY), ABSDATE_PARSER.get().parse("2015-12-26-07:00:00,000"));
    assertEquals(getCeilDate(date, MINUTELY), ABSDATE_PARSER.get().parse("2015-12-26-06:31:00,000"));
    assertEquals(getCeilDate(date, SECONDLY), ABSDATE_PARSER.get().parse("2015-12-26-06:30:16,000"));
    assertEquals(getCeilDate(date, WEEKLY), ABSDATE_PARSER.get().parse("2015-12-27-00:00:00,000"));
  }

  @Test
  public void testTimeDiff() throws LensException {
    ArrayList<String> minusFourDays =
      Lists.newArrayList("-4 days", "-4days", "-4day", "-4 day", "- 4days", "- 4 day");
    ArrayList<String> plusFourDays =
      Lists.newArrayList("+4 days", "4 days", "+4days", "4day", "4 day", "+ 4days", "+ 4 day", "+4 day");
    Set<TimeDiff> diffs = Sets.newHashSet();
    for (String diffStr : minusFourDays) {
      diffs.add(TimeDiff.parseFrom(diffStr));
    }
    assertEquals(diffs.size(), 1);
    TimeDiff minusFourDaysDiff = diffs.iterator().next();
    assertEquals(minusFourDaysDiff.quantity, -4);
    assertEquals(minusFourDaysDiff.updatePeriod, DAILY);

    diffs.clear();
    for (String diffStr : plusFourDays) {
      diffs.add(TimeDiff.parseFrom(diffStr));
    }
    assertEquals(diffs.size(), 1);
    TimeDiff plusFourDaysDiff = diffs.iterator().next();
    assertEquals(plusFourDaysDiff.quantity, 4);
    assertEquals(plusFourDaysDiff.updatePeriod, DAILY);
    Date now = new Date();
    assertEquals(minusFourDaysDiff.offsetFrom(plusFourDaysDiff.offsetFrom(now)), now);
    assertEquals(plusFourDaysDiff.offsetFrom(minusFourDaysDiff.offsetFrom(now)), now);
    assertEquals(minusFourDaysDiff.negativeOffsetFrom(now), plusFourDaysDiff.offsetFrom(now));
    assertEquals(minusFourDaysDiff.offsetFrom(now), plusFourDaysDiff.negativeOffsetFrom(now));
  }

  @Test
  public void testRelativeToAbsolute() throws LensException {
    Date now = new Date();
    Date nowDay = DateUtils.truncate(now, DAY_OF_MONTH);
    Date nowDayMinus2Days = DateUtils.add(nowDay, DAY_OF_MONTH, -2);
    assertEquals(relativeToAbsolute("now", now), DateUtil.ABSDATE_PARSER.get().format(now));
    assertEquals(relativeToAbsolute("now.day", now), DateUtil.ABSDATE_PARSER.get().format(nowDay));
    assertEquals(relativeToAbsolute("now.day - 2 days", now), DateUtil.ABSDATE_PARSER.get().format(nowDayMinus2Days));
    assertEquals(relativeToAbsolute("now.day - 2 day", now), DateUtil.ABSDATE_PARSER.get().format(nowDayMinus2Days));
    assertEquals(relativeToAbsolute("now.day - 2day", now), DateUtil.ABSDATE_PARSER.get().format(nowDayMinus2Days));
    assertEquals(relativeToAbsolute("now.day -2 day", now), DateUtil.ABSDATE_PARSER.get().format(nowDayMinus2Days));
    assertEquals(relativeToAbsolute("now.day -2 days", now), DateUtil.ABSDATE_PARSER.get().format(nowDayMinus2Days));
  }
  @Test
  public void testTimestamp() throws LensException {
    Date now = new Date();
    assertEquals(DateUtil.resolveDate(String.valueOf(now.getTime()+1), now), new Date(now.getTime()+1));
  }
}
