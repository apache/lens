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


import static org.apache.lens.cube.metadata.UpdatePeriod.*;

import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;

public class DateFactory {
  private DateFactory() {

  }

  public static class DateOffsetProvider extends HashMap<Integer, Date> {
    private final UpdatePeriod updatePeriod;
    Calendar calendar = Calendar.getInstance();

    public DateOffsetProvider(UpdatePeriod updatePeriod) {
      this(updatePeriod, false);
    }

    public DateOffsetProvider(UpdatePeriod updatePeriod, boolean truncate) {
      this.updatePeriod = updatePeriod;
      Date date = calendar.getTime();
      if (truncate) {
        date = updatePeriod.truncate(date);
        calendar.setTime(date);
      }
      put(0, date);
    }

    @Override
    public Date get(Object key) {
      if (!containsKey(key) && key instanceof Integer) {
        calendar.setTime(super.get(0));
        calendar.add(updatePeriod.calendarField(), (Integer) key);
        put((Integer) key, calendar.getTime());
      }
      return super.get(key);
    }
  }

  public static class GeneralDateOffsetProvider extends HashMap<UpdatePeriod, DateOffsetProvider> {
    boolean truncate;
    public GeneralDateOffsetProvider(boolean truncate) {
      this.truncate = truncate;
    }

    @Override
    public DateOffsetProvider get(Object key) {
      if (!containsKey(key) && key instanceof UpdatePeriod) {
        UpdatePeriod up = (UpdatePeriod) key;
        put(up, new DateOffsetProvider(up, truncate));
      }
      return super.get(key);
    }

    public Date get(UpdatePeriod updatePeriod, int offset) {
      return get(updatePeriod).get(offset);
    }
  }

  public static final GeneralDateOffsetProvider GENERAL_DATE_OFFSET_PROVIDER = new GeneralDateOffsetProvider(false);
  public static final GeneralDateOffsetProvider GENERAL_TRUNCATED_DATE_OFFSET_PROVIDER
    = new GeneralDateOffsetProvider(true);


  public static Date getDateWithOffset(UpdatePeriod up, int offset) {
    return GENERAL_DATE_OFFSET_PROVIDER.get(up, offset);
  }

  public static Date getTruncatedDateWithOffset(UpdatePeriod up, int offset) {
    return GENERAL_TRUNCATED_DATE_OFFSET_PROVIDER.get(up, offset);
  }

  public static String getDateStringWithOffset(UpdatePeriod up, int offset) {
    return getDateStringWithOffset(up, offset, up);
  }

  public static String getDateStringWithOffset(UpdatePeriod up, int offset, UpdatePeriod formatWith) {
    return formatWith.format(GENERAL_DATE_OFFSET_PROVIDER.get(up, offset));
  }

  public static String getTimeRangeString(final String timeDim, final String startDate, final String endDate) {
    return "time_range_in(" + timeDim + ", '" + startDate + "','" + endDate + "')";
  }

  public static String getTimeRangeString(final String timeDim, final UpdatePeriod updatePeriod,
    final int startOffset, final int endOffset) {
    return getTimeRangeString(timeDim,
      getDateStringWithOffset(updatePeriod, startOffset), getDateStringWithOffset(updatePeriod, endOffset));
  }

  public static String getTimeRangeString(final String startDate, final String endDate) {
    return getTimeRangeString("d_time", startDate, endDate);
  }

  public static String getTimeRangeString(final UpdatePeriod updatePeriod,
    final int startOffset, final int endOffset) {
    return getTimeRangeString("d_time", updatePeriod, startOffset, endOffset);
  }

  public static String getTimeRangeString(String partCol, UpdatePeriod updatePeriod, int startOffset, int endOffset,
    UpdatePeriod formatWith) {
    return getTimeRangeString(partCol,
      formatWith.format(getDateWithOffset(updatePeriod, startOffset)),
      formatWith.format(getDateWithOffset(updatePeriod, endOffset)));
  }

  public static String getTimeRangeString(String partCol, UpdatePeriod updatePeriod, int startOffset, int endOffset,
    DateFormat formatWith) {
    return getTimeRangeString(partCol,
      formatWith.format(getDateWithOffset(updatePeriod, startOffset)),
      formatWith.format(getDateWithOffset(updatePeriod, endOffset)));
  }

  public static String getTimeRangeString(UpdatePeriod updatePeriod, int startOffset, int endOffset,
    UpdatePeriod formatWith) {
    return getTimeRangeString("d_time", updatePeriod, startOffset, endOffset, formatWith);
  }

  public static String getTimeRangeString(UpdatePeriod updatePeriod, int startOffset, int endOffset,
    DateFormat formatWith) {
    return getTimeRangeString("d_time", updatePeriod, startOffset, endOffset, formatWith);
  }

  // Time Instances as Date Type
  public static final Date NOW;
  public static final Date ONEDAY_BACK;
  public static final Date TWODAYS_BACK;
  public static final Date TWO_MONTHS_BACK;
  public static final Date THIS_MONTH_TRUNCATED;
  public static final Date ONE_MONTH_BACK_TRUNCATED;
  public static final Date TWO_MONTHS_BACK_TRUNCATED;
  public static final Date THREE_MONTHS_BACK_TRUNCATED;
  public static final Date BEFORE_6_DAYS;
  public static final Date BEFORE_4_DAYS;

  // Time Ranges
  public static final String LAST_HOUR_TIME_RANGE;
  public static final String TWO_DAYS_RANGE;
  public static final String TWO_DAYS_RANGE_SPLIT_OVER_UPDATE_PERIODS;
  public static final String TWO_DAYS_RANGE_TTD;
  public static final String TWO_DAYS_RANGE_TTD_BEFORE_4_DAYS;
  public static final String TWO_DAYS_RANGE_TTD2;
  public static final String TWO_DAYS_RANGE_TTD2_BEFORE_4_DAYS;
  public static final String THREE_DAYS_RANGE_IT;
  public static final String TWO_DAYS_RANGE_IT;
  public static final String ONE_DAY_RANGE_IT;
  public static final String THIS_YEAR_RANGE;
  public static final String LAST_YEAR_RANGE;
  public static final String TWO_MONTHS_RANGE_UPTO_MONTH;
  public static final String TWO_MONTHS_RANGE_UPTO_DAYS;
  public static final String TWO_MONTHS_RANGE_UPTO_HOURS;
  public static final String TWO_DAYS_RANGE_BEFORE_4_DAYS;
  public static final String THREE_MONTHS_RANGE_UPTO_DAYS;
  public static final String THREE_MONTHS_RANGE_UPTO_MONTH;
  private static boolean zerothHour;


  public static boolean isZerothHour() {
    return zerothHour;
  }

  static {
    NOW = getDateWithOffset(HOURLY, 0);

    // Figure out if current hour is 0th hour
    zerothHour = getDateStringWithOffset(HOURLY, 0).endsWith("-00");

    ONEDAY_BACK = getDateWithOffset(DAILY, -1);
    System.out.println("Test ONEDAY_BACK:" + ONEDAY_BACK);

    TWODAYS_BACK = getDateWithOffset(DAILY, -2);
    System.out.println("Test TWODAYS_BACK:" + TWODAYS_BACK);

    // two months back
    TWO_MONTHS_BACK = getDateWithOffset(MONTHLY, -2);
    System.out.println("Test TWO_MONTHS_BACK:" + TWO_MONTHS_BACK);

    THIS_MONTH_TRUNCATED = getTruncatedDateWithOffset(MONTHLY, 0);
    ONE_MONTH_BACK_TRUNCATED  = getTruncatedDateWithOffset(MONTHLY, -1);
    TWO_MONTHS_BACK_TRUNCATED  = getTruncatedDateWithOffset(MONTHLY, -2);
    THREE_MONTHS_BACK_TRUNCATED  = getTruncatedDateWithOffset(MONTHLY, -3);


    // Before 4days
    BEFORE_4_DAYS = getDateWithOffset(DAILY, -4);
    BEFORE_6_DAYS = getDateWithOffset(DAILY, -6);

    TWO_DAYS_RANGE_BEFORE_4_DAYS = getTimeRangeString(DAILY, -6, -4, HOURLY);

    TWO_DAYS_RANGE = getTimeRangeString(HOURLY, -48, 0);
    TWO_DAYS_RANGE_TTD = getTimeRangeString("test_time_dim", DAILY, -2, 0, HOURLY);
    TWO_DAYS_RANGE_TTD_BEFORE_4_DAYS = getTimeRangeString("test_time_dim", DAILY, -6, -4, HOURLY);
    TWO_DAYS_RANGE_TTD2 = getTimeRangeString("test_time_dim2", DAILY, -2, 0, HOURLY);
    TWO_DAYS_RANGE_TTD2_BEFORE_4_DAYS = getTimeRangeString("test_time_dim2", DAILY, -6, -4, HOURLY);
    THREE_DAYS_RANGE_IT = getTimeRangeString("it", DAILY, -3, 0, HOURLY);
    TWO_DAYS_RANGE_IT = getTimeRangeString("it", DAILY, -2, 0, HOURLY);
    ONE_DAY_RANGE_IT = getTimeRangeString("it", DAILY, -1, 0, DAILY);
    THIS_YEAR_RANGE = getTimeRangeString(YEARLY, 0, 1);
    LAST_YEAR_RANGE = getTimeRangeString(YEARLY, -1, 0);
    TWO_MONTHS_RANGE_UPTO_MONTH = getTimeRangeString(MONTHLY, -2, 0);
    TWO_MONTHS_RANGE_UPTO_DAYS = getTimeRangeString(MONTHLY, -2, 0, DAILY);
    TWO_MONTHS_RANGE_UPTO_HOURS = getTimeRangeString(MONTHLY, -2, 0, HOURLY);
    THREE_MONTHS_RANGE_UPTO_DAYS = getTimeRangeString(MONTHLY, -3, 0, DAILY);
    THREE_MONTHS_RANGE_UPTO_MONTH = getTimeRangeString(MONTHLY, -3, 0, MONTHLY);

    // calculate LAST_HOUR_TIME_RANGE
    LAST_HOUR_TIME_RANGE = getTimeRangeString(HOURLY, -1, 0);

    TWO_DAYS_RANGE_SPLIT_OVER_UPDATE_PERIODS = StringUtils.join(" OR ", Lists.newArrayList(
      getTimeRangeString(getDateStringWithOffset(HOURLY, -48), getDateStringWithOffset(DAILY, -1)),
      getTimeRangeString(getDateStringWithOffset(DAILY, 0), getDateStringWithOffset(HOURLY, 0)),
      getTimeRangeString(getDateStringWithOffset(DAILY, -1), getDateStringWithOffset(DAILY, 0))
    ));
  }
}
