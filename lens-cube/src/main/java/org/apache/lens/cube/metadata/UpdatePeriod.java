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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;

import org.apache.lens.cube.parse.DateUtil;

public enum UpdatePeriod implements Named {
  SECONDLY(Calendar.SECOND, 1000, "yyyy-MM-dd-HH-mm-ss"), MINUTELY(Calendar.MINUTE, 60 * SECONDLY.weight(),
      "yyyy-MM-dd-HH-mm"), HOURLY(Calendar.HOUR_OF_DAY, 60 * MINUTELY.weight(), "yyyy-MM-dd-HH"), DAILY(
      Calendar.DAY_OF_MONTH, 24 * HOURLY.weight(), "yyyy-MM-dd"), WEEKLY(Calendar.WEEK_OF_YEAR, 7 * DAILY.weight(),
      "YYYY-'W'ww"), MONTHLY(Calendar.MONTH, 30 * DAILY.weight(), "yyyy-MM"), QUARTERLY(Calendar.MONTH, 3 * MONTHLY
      .weight(), "yyyy-MM"), YEARLY(Calendar.YEAR, 12 * MONTHLY.weight(), "yyyy");

  public static final long MIN_INTERVAL = SECONDLY.weight();
  private final int calendarField;
  private final long weight;
  private final String format;

  private static DateFormat getSecondlyFormat() {
    if (secondlyFormat == null) {
      secondlyFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat(SECONDLY.formatStr());
        }
      };
    }
    return secondlyFormat.get();
  }

  private static DateFormat getMinutelyFormat() {
    if (minutelyFormat == null) {
      minutelyFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat(MINUTELY.formatStr());
        }
      };
    }
    return minutelyFormat.get();
  }

  private static DateFormat getHourlyFormat() {
    if (hourlyFormat == null) {
      hourlyFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat(HOURLY.formatStr());
        }
      };
    }
    return hourlyFormat.get();
  }

  private static DateFormat getDailyFormat() {
    if (dailyFormat == null) {
      dailyFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat(DAILY.formatStr());
        }
      };
    }
    return dailyFormat.get();
  }

  private static DateFormat getWeeklyFormat() {
    if (weeklyFormat == null) {
      weeklyFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat(WEEKLY.formatStr());
        }
      };
    }
    return weeklyFormat.get();
  }

  private static DateFormat getMonthlyFormat() {
    if (monthlyFormat == null) {
      monthlyFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat(MONTHLY.formatStr());
        }
      };
    }
    return monthlyFormat.get();
  }

  private static DateFormat getQuarterlyFormat() {
    if (quarterlyFormat == null) {
      quarterlyFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat(QUARTERLY.formatStr());
        }
      };
    }
    return quarterlyFormat.get();
  }

  private static DateFormat getYearlyFormat() {
    if (yearlyFormat == null) {
      yearlyFormat = new ThreadLocal<DateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat(YEARLY.formatStr());
        }
      };
    }
    return yearlyFormat.get();
  }

  private static ThreadLocal<DateFormat> secondlyFormat;
  private static ThreadLocal<DateFormat> minutelyFormat;
  private static ThreadLocal<DateFormat> hourlyFormat;
  private static ThreadLocal<DateFormat> dailyFormat;
  private static ThreadLocal<DateFormat> weeklyFormat;
  private static ThreadLocal<DateFormat> monthlyFormat;
  private static ThreadLocal<DateFormat> quarterlyFormat;
  private static ThreadLocal<DateFormat> yearlyFormat;

  UpdatePeriod(int calendarField, long diff, String format) {
    this.calendarField = calendarField;
    this.weight = diff;
    this.format = format;
  }

  public int calendarField() {
    return this.calendarField;
  }

  public long weight() {
    return this.weight;
  }

  public long monthWeight(Date date) {
    return DateUtil.getNumberofDaysInMonth(date) * DAILY.weight();
  }

  public DateFormat format() {
    switch (this) {
    case SECONDLY:
      return getSecondlyFormat();
    case MINUTELY:
      return getMinutelyFormat();
    case HOURLY:
      return getHourlyFormat();
    case DAILY:
      return getDailyFormat();
    case WEEKLY:
      return getWeeklyFormat();
    case MONTHLY:
      return getMonthlyFormat();
    case QUARTERLY:
      return getQuarterlyFormat();
    case YEARLY:
      return getYearlyFormat();
    default:
      return null;
    }
  }

  public String formatStr() {
    return this.format;
  }

  @Override
  public String getName() {
    return name();
  }

  public static class UpdatePeriodComparator implements Comparator<UpdatePeriod> {
    @Override
    public int compare(UpdatePeriod o1, UpdatePeriod o2) {
      if (o1 == null && o2 != null) {
        return -1;
      } else if (o1 != null && o2 == null) {
        return 1;
      } else if (o1 == null && o2 == null) {
        return 0;
      } else {
        if (o1.weight > o2.weight) {
          return 1;
        } else if (o1.weight < o2.weight) {
          return -1;
        } else {
          return 0;
        }
      }
    }
  }
}
