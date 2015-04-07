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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lens.cube.metadata.UpdatePeriod;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.log4j.Logger;

import lombok.Data;

public final class DateUtil {
  private DateUtil() {

  }

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

  public static final String YEAR_FMT = "[0-9]{4}";
  public static final String MONTH_FMT = YEAR_FMT + "-[0-9]{2}";
  public static final String DAY_FMT = MONTH_FMT + "-[0-9]{2}";
  public static final String HOUR_FMT = DAY_FMT + "-[0-9]{2}";
  public static final String MINUTE_FMT = HOUR_FMT + ":[0-9]{2}";
  public static final String SECOND_FMT = MINUTE_FMT + ":[0-9]{2}";
  public static final String ABSDATE_FMT = "yyyy-MM-dd-HH:mm:ss,SSS";

  public static final ThreadLocal<DateFormat> ABSDATE_PARSER =
    new ThreadLocal<DateFormat>() {
      @Override
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(ABSDATE_FMT);
      }
    };

  public static String formatDate(Date dt) {
    return ABSDATE_PARSER.get().format(dt);
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
        return ABSDATE_PARSER.get().parse(getAbsDateFormatString(str));
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

  public static CoveringInfo getMonthlyCoveringInfo(Date from, Date to) {
    // Move 'from' to end of month, unless its the first day of month
    boolean coverable = true;
    if (!from.equals(DateUtils.truncate(from, Calendar.MONTH))) {
      from = DateUtils.addMonths(DateUtils.truncate(from, Calendar.MONTH), 1);
      coverable = false;
    }

    // Move 'to' to beginning of next month, unless its the first day of the month
    if (!to.equals(DateUtils.truncate(to, Calendar.MONTH))) {
      to = DateUtils.truncate(to, Calendar.MONTH);
      coverable = false;
    }

    int months = 0;
    while (from.before(to)) {
      from = DateUtils.addMonths(from, 1);
      months++;
    }
    return new CoveringInfo(months, coverable);
  }

  public static CoveringInfo getQuarterlyCoveringInfo(Date from, Date to) {
    CoveringInfo monthlyCoveringInfo = getMonthlyCoveringInfo(from, to);
    if (monthlyCoveringInfo.getCountBetween() < 3) {
      return new CoveringInfo(0, false);
    }
    boolean coverable = monthlyCoveringInfo.isCoverable();
    if (!from.equals(DateUtils.truncate(from, Calendar.MONTH))) {
      from = DateUtils.addMonths(DateUtils.truncate(from, Calendar.MONTH), 1);
      coverable = false;
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    int fromMonth = cal.get(Calendar.MONTH);

    // Get the start date of the quarter
    int beginOffset = (3 - fromMonth % 3) % 3;
    int endOffset = (monthlyCoveringInfo.getCountBetween() - beginOffset) % 3;
    if (beginOffset > 0 || endOffset > 0) {
      coverable = false;
    }
    return new CoveringInfo((monthlyCoveringInfo.getCountBetween() - beginOffset - endOffset) / 3, coverable);
  }


  public static CoveringInfo getYearlyCoveringInfo(Date from, Date to) {
    CoveringInfo monthlyCoveringInfo = getMonthlyCoveringInfo(from, to);
    if (monthlyCoveringInfo.getCountBetween() < 12) {
      return new CoveringInfo(0, false);
    }
    boolean coverable = monthlyCoveringInfo.isCoverable();
    if (!from.equals(DateUtils.truncate(from, Calendar.MONTH))) {
      from = DateUtils.addMonths(DateUtils.truncate(from, Calendar.MONTH), 1);
      coverable = false;
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    int fromMonth = cal.get(Calendar.MONTH);
    int beginOffset = (12 - fromMonth % 12) % 12;
    int endOffset = (monthlyCoveringInfo.getCountBetween() - beginOffset) % 12;
    if (beginOffset > 0 || endOffset > 0) {
      coverable = false;
    }
    return new CoveringInfo((monthlyCoveringInfo.getCountBetween() - beginOffset - endOffset) / 12, coverable);
  }

  public static CoveringInfo getWeeklyCoveringInfo(Date from, Date to) {
    int dayDiff = 0;
    Date tmpFrom = from;
    while (tmpFrom.before(to)) {
      tmpFrom = DateUtils.addDays(tmpFrom, 1);
      dayDiff++;
    }

    if (dayDiff < 7) {
      return new CoveringInfo(0, false);
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
    boolean coverable = dayDiff % 7 == 0;
    if (fromWeekStartDate.before(from)) {
      // Count from the start of next week
      dayDiff -= (maxDayInWeek - (fromDay - Calendar.SUNDAY));
      coverable = false;
    }

    return new CoveringInfo(dayDiff / 7, coverable);
  }

  static CoveringInfo getCoveringInfo(Date from, Date to, UpdatePeriod interval) {
    switch (interval) {
    case SECONDLY:
      return getMilliSecondCoveringInfo(from, to, 1000);
    case MINUTELY:
      return getMilliSecondCoveringInfo(from, to, 1000 * 60);
    case HOURLY:
      return getMilliSecondCoveringInfo(from, to, 1000 * 60 * 60);
    case DAILY:
      return getMilliSecondCoveringInfo(from, to, 1000 * 60 * 60 * 24);
    case WEEKLY:
      return getWeeklyCoveringInfo(from, to);
    case MONTHLY:
      return getMonthlyCoveringInfo(from, to);
    case QUARTERLY:
      return getQuarterlyCoveringInfo(from, to);
    case YEARLY:
      return getYearlyCoveringInfo(from, to);
    default:
      return new CoveringInfo(0, false);
    }
  }

  private static CoveringInfo getMilliSecondCoveringInfo(Date from, Date to, int millisInInterval) {
    long diff = to.getTime() - from.getTime();
    return new CoveringInfo((int) (diff / millisInInterval), diff % millisInInterval == 0);
  }

  static boolean isCoverableBy(Date from, Date to, Set<UpdatePeriod> intervals) {
    for (UpdatePeriod period : intervals) {
      if (getCoveringInfo(from, to, period).isCoverable()) {
        return true;
      }
    }
    return false;
  }

  public static int getTimeDiff(Date fromDate, Date toDate, UpdatePeriod updatePeriod) {
    return getCoveringInfo(fromDate, toDate, updatePeriod).getCountBetween();
  }

  @Data
  public static class CoveringInfo {
    int countBetween;
    boolean coverable;

    public CoveringInfo(int countBetween, boolean coverable) {
      this.countBetween = countBetween;
      this.coverable = coverable;
    }
  }
}
