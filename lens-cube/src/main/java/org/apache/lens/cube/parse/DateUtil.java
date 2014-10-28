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
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.log4j.Logger;

public class DateUtil {
  public static final Logger LOG = Logger.getLogger(DateUtil.class);

  /*
   * NOW -> new java.util.Date() NOW-7DAY -> a date one week earlier NOW (+-)
   * <NUM>UNIT or Hardcoded dates in DD-MM-YYYY hh:mm:ss,sss
   */
  public static final String UNIT = "year|month|week|day|hour|minute|second";
  public static final String GRANULARITY = "\\.(" + UNIT + ")";
  public static final String RELATIVE = "(now){1}(" + GRANULARITY + "){0,1}";
  public static final Pattern P_RELATIVE = Pattern.compile(RELATIVE, Pattern.CASE_INSENSITIVE);

  public static final String WSPACE = "\\s+";
  public static final Pattern P_WSPACE = Pattern.compile(WSPACE);

  public static final String SIGNAGE = "\\+|\\-";
  public static final Pattern P_SIGNAGE = Pattern.compile(SIGNAGE);

  public static final String QUANTITY = "\\d+";
  public static final Pattern P_QUANTITY = Pattern.compile(QUANTITY);

  public static final Pattern P_UNIT = Pattern.compile(UNIT, Pattern.CASE_INSENSITIVE);

  public static final String RELDATE_VALIDATOR_STR = RELATIVE + "(" + WSPACE + ")?" + "((" + SIGNAGE + ")" + "("
      + WSPACE + ")?" + "(" + QUANTITY + ")(" + UNIT + ")){0,1}" + "(s?)";

  public static final Pattern RELDATE_VALIDATOR = Pattern.compile(RELDATE_VALIDATOR_STR, Pattern.CASE_INSENSITIVE);

  public static String YEAR_FMT = "[0-9]{4}";
  public static String MONTH_FMT = YEAR_FMT + "-[0-9]{2}";
  public static String DAY_FMT = MONTH_FMT + "-[0-9]{2}";
  public static String HOUR_FMT = DAY_FMT + "-[0-9]{2}";
  public static String MINUTE_FMT = HOUR_FMT + ":[0-9]{2}";
  public static String SECOND_FMT = MINUTE_FMT + ":[0-9]{2}";
  public static final String ABSDATE_FMT = "yyyy-MM-dd-HH:mm:ss,SSS";
  public static final SimpleDateFormat ABSDATE_PARSER = new SimpleDateFormat(ABSDATE_FMT);

  public static String formatDate(Date dt) {
    return ABSDATE_PARSER.format(dt);
  }

  public static String getAbsDateFormatString(String str) {
    if (str.matches(YEAR_FMT)) {
      return str + "-01-01-00:00:00,000";
    } else if (str.matches(MONTH_FMT)) {
      return str + "-01-00:00:00,000";
    } else if (str.matches(DAY_FMT)) {
      return str + "-00:00:00,000";
    } else if (str.matches(HOUR_FMT)) {
      return str + ":00:00,000";
    } else if (str.matches(MINUTE_FMT)) {
      return str + ":00,000";
    } else if (str.matches(SECOND_FMT)) {
      return str + ",000";
    } else if (str.matches(ABSDATE_FMT)) {
      return str;
    }
    throw new IllegalArgumentException("Unsupported formatting for date" + str);
  }

  public static Date resolveDate(String str, Date now) throws SemanticException {
    if (RELDATE_VALIDATOR.matcher(str).matches()) {
      return resolveRelativeDate(str, now);
    } else {
      try {
        return ABSDATE_PARSER.parse(getAbsDateFormatString(str));
      } catch (ParseException e) {
        LOG.error("Invalid date format. expected only " + ABSDATE_FMT + " date provided:" + str, e);
        throw new SemanticException(e, ErrorMsg.WRONG_TIME_RANGE_FORMAT, ABSDATE_FMT, str);
      }
    }
  }

  public static Date resolveRelativeDate(String str, Date now) throws SemanticException {
    if (StringUtils.isBlank(str)) {
      throw new SemanticException(ErrorMsg.NULL_DATE_VALUE);
    }

    // Resolve NOW with proper granularity
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(now);

    str = str.toLowerCase();
    Matcher relativeMatcher = P_RELATIVE.matcher(str);
    if (relativeMatcher.find()) {
      String nowWithGranularity = relativeMatcher.group();
      nowWithGranularity = nowWithGranularity.replaceAll("now", "");
      nowWithGranularity = nowWithGranularity.replaceAll("\\.", "");

      Matcher granularityMatcher = P_UNIT.matcher(nowWithGranularity);
      if (granularityMatcher.find()) {
        String unit = granularityMatcher.group().toLowerCase();
        if ("year".equals(unit)) {
          calendar = DateUtils.truncate(calendar, Calendar.YEAR);
        } else if ("month".equals(unit)) {
          calendar = DateUtils.truncate(calendar, Calendar.MONTH);
        } else if ("week".equals(unit)) {
          calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
          calendar = DateUtils.truncate(calendar, Calendar.DAY_OF_MONTH);
        } else if ("day".equals(unit)) {
          calendar = DateUtils.truncate(calendar, Calendar.DAY_OF_MONTH);
        } else if ("hour".equals(unit)) {
          calendar = DateUtils.truncate(calendar, Calendar.HOUR_OF_DAY);
        } else if ("minute".equals(unit)) {
          calendar = DateUtils.truncate(calendar, Calendar.MINUTE);
        } else if ("second".equals(unit)) {
          calendar = DateUtils.truncate(calendar, Calendar.SECOND);
        } else {
          throw new SemanticException(ErrorMsg.INVALID_TIME_UNIT, unit);
        }
      }
    }

    // Get rid of 'now' part and whitespace
    String raw = str.replaceAll(GRANULARITY, "").replace(WSPACE, "");

    if (raw.isEmpty()) { // String is just "now" with granularity
      return calendar.getTime();
    }

    // Get the relative diff part to get eventual date based on now.
    Matcher qtyMatcher = P_QUANTITY.matcher(raw);
    int qty = 1;
    if (qtyMatcher.find()) {
      qty = Integer.parseInt(qtyMatcher.group());
    }

    Matcher signageMatcher = P_SIGNAGE.matcher(raw);
    if (signageMatcher.find()) {
      String sign = signageMatcher.group();
      if ("-".equals(sign)) {
        qty = -qty;
      }
    }

    Matcher unitMatcher = P_UNIT.matcher(raw);
    if (unitMatcher.find()) {
      String unit = unitMatcher.group().toLowerCase();
      if ("year".equals(unit)) {
        calendar.add(Calendar.YEAR, qty);
      } else if ("month".equals(unit)) {
        calendar.add(Calendar.MONTH, qty);
      } else if ("week".equals(unit)) {
        calendar.add(Calendar.DAY_OF_MONTH, 7 * qty);
      } else if ("day".equals(unit)) {
        calendar.add(Calendar.DAY_OF_MONTH, qty);
      } else if ("hour".equals(unit)) {
        calendar.add(Calendar.HOUR_OF_DAY, qty);
      } else if ("minute".equals(unit)) {
        calendar.add(Calendar.MINUTE, qty);
      } else if ("second".equals(unit)) {
        calendar.add(Calendar.SECOND, qty);
      } else {
        throw new SemanticException(ErrorMsg.INVALID_TIME_UNIT, unit);
      }
    }

    return calendar.getTime();
  }

  public static Date getCeilDate(Date fromDate, UpdatePeriod interval) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(fromDate);
    boolean hasFraction = false;
    switch (interval) {
    case YEARLY:
      if (cal.get(Calendar.MONTH) != 1) {
        hasFraction = true;
        break;
      }
    case MONTHLY:
      if (cal.get(Calendar.DAY_OF_MONTH) != 1) {
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
      cal.add(interval.calendarField(), 1);
      return getFloorDate(cal.getTime(), interval);
    } else {
      return fromDate;
    }
  }

  public static Date getFloorDate(Date toDate, UpdatePeriod interval) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(toDate);
    switch (interval) {
    case YEARLY:
      cal.set(Calendar.MONTH, 1);
    case MONTHLY:
      cal.set(Calendar.DAY_OF_MONTH, 1);
    case DAILY:
      cal.set(Calendar.HOUR_OF_DAY, 0);
    case HOURLY:
      cal.set(Calendar.MINUTE, 0);
    case MINUTELY:
      cal.set(Calendar.SECOND, 0);
    case SECONDLY:
      break;
    case WEEKLY:
      cal.set(Calendar.DAY_OF_WEEK, 1);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      break;
    }
    return cal.getTime();
  }

  public static int getNumberofDaysInMonth(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
  }

  public static int getMonthsBetween(Date from, Date to) {
    // Move 'from' to end of month, unless its the first day of month
    if (!from.equals(DateUtils.truncate(from, Calendar.MONTH))) {
      from = DateUtils.addMonths(DateUtils.truncate(from, Calendar.MONTH), 1);
    }

    // Move 'to' to beginning of month, unless its the last day of the month
    if (!to.equals(DateUtils.round(to, Calendar.MONTH))) {
      to = DateUtils.truncate(to, Calendar.MONTH);
    }

    int months = 0;
    while (from.before(to)) {
      from = DateUtils.addMonths(from, 1);
      months++;
    }
    return months;
  }

  public static int getQuartersBetween(Date from, Date to) {
    int months = getMonthsBetween(from, to);
    if (months < 3) {
      return 0;
    }

    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    int fromMonth = cal.get(Calendar.MONTH);
    int fromYear = cal.get(Calendar.YEAR);

    // Get the start date of the quarter
    int qtrStartMonth;
    if (fromMonth % 3 == 0) {
      qtrStartMonth = fromMonth;
    } else {
      qtrStartMonth = fromMonth - (fromMonth % 3);
    }

    cal.clear();
    cal.set(Calendar.MONTH, qtrStartMonth);
    cal.set(Calendar.YEAR, fromYear);
    cal.set(Calendar.DAY_OF_MONTH, 1);
    Date fromQtrStartDate = cal.getTime();

    int moveUp = 0;
    if (fromQtrStartDate.before(from)) {
      moveUp = 3 - (fromMonth % 3);
    }

    if (months % 3 != 0) {
      months = months - (months % 3);
    }
    return (months - moveUp) / 3;
  }

  public static int getYearsBetween(Date from, Date to) {
    int months = getMonthsBetween(from, to);
    if (months < 12) {
      return 0;
    }

    // Get start of year for 'from' date
    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    int fromMonth = cal.get(Calendar.MONTH);
    int fromYear = cal.get(Calendar.YEAR);

    cal.clear();
    cal.set(Calendar.MONTH, Calendar.JANUARY);
    cal.set(Calendar.YEAR, fromYear);
    cal.set(Calendar.DAY_OF_MONTH, 1);

    Date yearStartDate = cal.getTime();

    int moveUp = 0;
    if (yearStartDate.before(from)) {
      moveUp = 12 - (fromMonth % 12);
    }

    if (months % 12 != 0) {
      months = months - (months % 12);
    }

    return (months - moveUp) / 12;
  }

  public static int getWeeksBetween(Date from, Date to) {
    int dayDiff = 0;
    Date tmpFrom = from;
    while (tmpFrom.before(to)) {
      tmpFrom = DateUtils.addDays(tmpFrom, 1);
      dayDiff++;
    }

    if (dayDiff < 7) {
      return 0;
    }

    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    int fromWeek = cal.get(Calendar.WEEK_OF_YEAR);
    int fromDay = cal.get(Calendar.DAY_OF_WEEK);
    int fromYear = cal.get(Calendar.YEAR);

    cal.clear();
    cal.set(Calendar.YEAR, fromYear);
    cal.set(Calendar.WEEK_OF_YEAR, fromWeek);
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
    int maxDayInWeek = cal.getActualMaximum(Calendar.DAY_OF_WEEK);
    Date fromWeekStartDate = cal.getTime();

    if (fromWeekStartDate.before(from)) {
      // Count from the start of next week
      dayDiff -= (maxDayInWeek - (fromDay - Calendar.SUNDAY));
    }

    return dayDiff / 7;
  }

  static long getTimeDiff(Date from, Date to, UpdatePeriod interval) {
    long diff = to.getTime() - from.getTime();
    switch (interval) {
    case SECONDLY:
      return diff / 1000;
    case MINUTELY:
      return diff / (1000 * 60);
    case HOURLY:
      return diff / (1000 * 60 * 60);
    case DAILY:
      return diff / (1000 * 60 * 60 * 24);
    case WEEKLY:
      // return diff/(1000 * 60 * 60 * 24 * 7);
      return getWeeksBetween(from, to);
    case MONTHLY:
      // return (long) (diff/(60 * 60 * 1000 * 24 * 30.41666666));
      return getMonthsBetween(from, to);
    case QUARTERLY:
      return getQuartersBetween(from, to);
    case YEARLY:
      // return (diff/(60 * 60 * 1000 * 24 * 365));
      return getYearsBetween(from, to);
    default:
      return -1;
    }
  }

}
