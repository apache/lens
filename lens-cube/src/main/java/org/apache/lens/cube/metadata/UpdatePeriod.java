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

import static java.util.Calendar.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang3.time.DateUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.Getter;

public enum UpdatePeriod implements Named {
  SECONDLY("second", SECOND, 1000, 1.4f, "yyyy-MM-dd-HH-mm-ss"),
  MINUTELY("minute", MINUTE, 60 * SECONDLY.weight(), 1.35f, "yyyy-MM-dd-HH-mm"),
  HOURLY("hour", HOUR_OF_DAY, 60 * MINUTELY.weight(), 1.3f, "yyyy-MM-dd-HH"),
  DAILY("day", DAY_OF_MONTH, 24 * HOURLY.weight(), 1f, "yyyy-MM-dd"),
  WEEKLY("week", WEEK_OF_YEAR, 7 * DAILY.weight(), 0.7f, "YYYY-'W'ww"),
  MONTHLY("month", MONTH, 30 * DAILY.weight(), 0.6f, "yyyy-MM"),
  QUARTERLY("quarter", MONTH, 3 * MONTHLY.weight(), 0.55f, "yyyy-MM"),
  YEARLY("year", YEAR, 12 * MONTHLY.weight(), 0.52f, "yyyy"),
  CONTINUOUS("continuous", Calendar.SECOND, 1, 1.5f, "yyyy-MM-dd-HH-mm-ss");

  public static final long MIN_INTERVAL = values()[0].weight();
  @Getter
  private String unitName;
  private final int calendarField;
  private final long weight;
  /**
   * Normalization factor is calculated in comparison with daily update period. What it means is that
   * for a fixed time range, reading partitions of this update period is expensive/cheap as compared to
   * reading partitions of daily update period by this factor. Values are tentatively picked based on
   * similar logic in an existing system at InMobi.
   */
  private final float normalizationFactor;
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

  UpdatePeriod(String unitName, int calendarField, long diff, float normalizationFactor, String format) {
    this.unitName = unitName;
    this.calendarField = calendarField;
    this.weight = diff;
    this.normalizationFactor = normalizationFactor;
    this.format = format;
  }

  public int calendarField() {
    return this.calendarField;
  }

  public long weight() {
    return this.weight;
  }

  public static UpdatePeriod fromUnitName(String unitName) throws LensException {
    for (UpdatePeriod up : values()) {
      if (up.getUnitName().equals(unitName)) {
        return up;
      }
    }
    throw new LensException(LensCubeErrorCode.INVALID_TIME_UNIT.getLensErrorInfo(), unitName);
  }

  public DateFormat format() {
    switch (this) {
    case CONTINUOUS:
      return getSecondlyFormat();
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
      throw new IllegalArgumentException("Update period illegal, or doesn't have defined format");
    }
  }

  Cache<Date, String> dateToStringCache = CacheBuilder.newBuilder()
    .expireAfterWrite(2, TimeUnit.HOURS).maximumSize(100).build();
  Cache<String, Date> stringToDateCache = CacheBuilder.newBuilder()
    .expireAfterWrite(2, TimeUnit.HOURS).maximumSize(100).build();

  public String format(final Date date) {
    try {
      return dateToStringCache.get(date, new Callable<String>() {
        @Override
        public String call() {
          return format().format(date);
        }
      });
    } catch (ExecutionException e) {
      return format().format(date);
    }
  }

  public Date parse(final String dateString) throws ParseException {
    try {
      return stringToDateCache.get(dateString, new Callable<Date>() {
        @Override
        public Date call() throws Exception {
          return format().parse(dateString);
        }
      });
    } catch (ExecutionException e) {
      return format().parse(dateString);
    }
  }

  public String formatStr() {
    return this.format;
  }

  @Override
  public String getName() {
    return name();
  }

  public boolean canParseDateString(String dateString) {
    return formatStr().replaceAll("'", "").length() == dateString.length();
  }

  public float getNormalizationFactor() {
    return normalizationFactor;
  }

  public Date truncate(Date date) {
    switch (this) {
    case WEEKLY:
      Date truncDate = DateUtils.truncate(date, Calendar.DAY_OF_MONTH);
      Calendar cal = Calendar.getInstance();
      cal.setTime(truncDate);
      cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek());
      return cal.getTime();
    case QUARTERLY:
      Date dt = DateUtils.truncate(date, this.calendarField());
      dt.setMonth(dt.getMonth() - (dt.getMonth() % 3));
      return dt;
    default:
      return DateUtils.truncate(date, this.calendarField());
    }
  }

  public Calendar truncate(Calendar calendar) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(truncate(calendar.getTime()));
    return cal;
  }

  public void increment(Calendar calendar, int increment) {
    switch (this) {
    case QUARTERLY:
      increment *= 3;
    }
    calendar.add(calendarField(), increment);
  }

  public Date getCeilDate(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    boolean hasFraction = false;
    switch (this) {
    case YEARLY:
      if (cal.get(MONTH) != 0) {
        hasFraction = true;
        break;
      }
    case MONTHLY:
      if (cal.get(DAY_OF_MONTH) != 1) {
        hasFraction = true;
        break;
      }
    case DAILY:
      if (cal.get(Calendar.HOUR_OF_DAY) != 0) {
        hasFraction = true;
        break;
      }
    case HOURLY:
      if (cal.get(Calendar.MINUTE) != 0) {
        hasFraction = true;
        break;
      }
    case MINUTELY:
      if (cal.get(Calendar.SECOND) != 0) {
        hasFraction = true;
        break;
      }
    case SECONDLY:
    case CONTINUOUS:
      if (cal.get(Calendar.MILLISECOND) != 0) {
        hasFraction = true;
      }
      break;
    case WEEKLY:
      if (cal.get(Calendar.DAY_OF_WEEK) != 1) {
        hasFraction = true;
        break;
      }
    }

    if (hasFraction) {
      cal.add(this.calendarField(), 1);
      return getFloorDate(cal.getTime());
    } else {
      return date;
    }
  }

  public Date getFloorDate(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    switch(this) {
    case WEEKLY:
      cal.set(Calendar.DAY_OF_WEEK, 1);
      break;
    }
    switch (this) {
    case YEARLY:
      cal.set(MONTH, 0);
    case MONTHLY:
      cal.set(DAY_OF_MONTH, 1);
    case WEEKLY:
      // Already covered, only here for fall through cases
    case DAILY:
      cal.set(Calendar.HOUR_OF_DAY, 0);
    case HOURLY:
      cal.set(Calendar.MINUTE, 0);
    case MINUTELY:
      cal.set(Calendar.SECOND, 0);
    case SECONDLY:
    case CONTINUOUS:
      cal.set(Calendar.MILLISECOND, 0);
      break;
    }
    return cal.getTime();
  }

  public static class UpdatePeriodComparator implements Comparator<UpdatePeriod> {
    @Override
    public int compare(UpdatePeriod o1, UpdatePeriod o2) {
      if (o1 == null && o2 != null) {
        return -1;
      } else if (o1 != null && o2 == null) {
        return 1;
      } else if (o1 == null) {
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
