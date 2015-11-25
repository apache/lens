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

import static java.util.Calendar.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DateUtil {
  private DateUtil() {

  }
  /*
   * NOW -> new java.util.Date() NOW-7DAY -> a date one week earlier NOW (+-)
   * <NUM>UNIT or Hardcoded dates in DD-MM-YYYY hh:mm:ss,sss
   */
  public static final String UNIT;

  static {
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (UpdatePeriod up : UpdatePeriod.values()) {
      sb.append(sep).append(up.getUnitName());
      sep = "|";
    }
    UNIT = sb.toString();
  }

  public static final String GRANULARITY = "\\.(" + UNIT + ")";
  public static final String RELATIVE = "(now)(" + GRANULARITY + ")?";
  public static final Pattern P_RELATIVE = Pattern.compile(RELATIVE, Pattern.CASE_INSENSITIVE);

  public static final String WSPACE = "\\s+";
  public static final String OPTIONAL_WSPACE = "\\s*";

  public static final String SIGNAGE = "\\+|\\-";
  public static final Pattern P_SIGNAGE = Pattern.compile(SIGNAGE);

  public static final String QUANTITY = "\\d+";
  public static final Pattern P_QUANTITY = Pattern.compile(QUANTITY);

  public static final Pattern P_UNIT = Pattern.compile(UNIT, Pattern.CASE_INSENSITIVE);

  public static final String RELDATE_VALIDATOR_STR = RELATIVE + OPTIONAL_WSPACE + "((" + SIGNAGE + ")" + "("
    + WSPACE + ")?" + "(" + QUANTITY + ")" + OPTIONAL_WSPACE + "(" + UNIT + "))?" + "(s?)";

  public static final Pattern RELDATE_VALIDATOR = Pattern.compile(RELDATE_VALIDATOR_STR, Pattern.CASE_INSENSITIVE);

  public static final String YEAR_FMT = "[0-9]{4}";
  public static final String MONTH_FMT = YEAR_FMT + "-[0-9]{2}";
  public static final String DAY_FMT = MONTH_FMT + "-[0-9]{2}";
  public static final String HOUR_FMT = DAY_FMT + "-[0-9]{2}";
  public static final String MINUTE_FMT = HOUR_FMT + ":[0-9]{2}";
  public static final String SECOND_FMT = MINUTE_FMT + ":[0-9]{2}";
  public static final String MILLISECOND_FMT = SECOND_FMT + ",[0-9]{3}";
  public static final String ABSDATE_FMT = "yyyy-MM-dd-HH:mm:ss,SSS";
  public static final String HIVE_QUERY_DATE_FMT = "yyyy-MM-dd HH:mm:ss";

  public static final ThreadLocal<DateFormat> ABSDATE_PARSER =
    new ThreadLocal<DateFormat>() {
      @Override
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(ABSDATE_FMT);
      }
    };
  public static final ThreadLocal<DateFormat> HIVE_QUERY_DATE_PARSER =
    new ThreadLocal<DateFormat>() {
      @Override
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(HIVE_QUERY_DATE_FMT);
      }
    };

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
    } else if (str.matches(MILLISECOND_FMT)) {
      return str;
    }
    throw new IllegalArgumentException("Unsupported formatting for date" + str);
  }

  public static Date resolveDate(String str, Date now) throws LensException {
    if (RELDATE_VALIDATOR.matcher(str).matches()) {
      return resolveRelativeDate(str, now);
    } else {
      return resolveAbsoluteDate(str);
    }
  }

  public static String relativeToAbsolute(String relative) throws LensException {
    return relativeToAbsolute(relative, new Date());
  }

  public static String relativeToAbsolute(String relative, Date now) throws LensException {
    if (RELDATE_VALIDATOR.matcher(relative).matches()) {
      return ABSDATE_PARSER.get().format(resolveRelativeDate(relative, now));
    } else {
      return relative;
    }
  }

  public static Date resolveAbsoluteDate(String str) throws LensException {
    try {
      return ABSDATE_PARSER.get().parse(getAbsDateFormatString(str));
    } catch (ParseException e) {
      log.error("Invalid date format. expected only {} date provided:{}", ABSDATE_FMT, str, e);
      throw new LensException(LensCubeErrorCode.WRONG_TIME_RANGE_FORMAT.getLensErrorInfo(), ABSDATE_FMT, str);
    }
  }

  public static Date resolveRelativeDate(String str, Date now) throws LensException {
    if (StringUtils.isBlank(str)) {
      throw new LensException(LensCubeErrorCode.NULL_DATE_VALUE.getLensErrorInfo());
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
        calendar = UpdatePeriod.fromUnitName(granularityMatcher.group().toLowerCase()).truncate(calendar);
      }
    }

    // Get rid of 'now' part and whitespace
    String diffStr = str.replaceAll(RELATIVE, "").replace(WSPACE, "");
    TimeDiff diff = TimeDiff.parseFrom(diffStr);
    return diff.offsetFrom(calendar.getTime());
  }

  public static Date getCeilDate(Date fromDate, UpdatePeriod interval) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(fromDate);
    boolean hasFraction = false;
    switch (interval) {
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
      cal.set(MONTH, 0);
    case MONTHLY:
      cal.set(DAY_OF_MONTH, 1);
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
    case WEEKLY:
      cal.set(Calendar.DAY_OF_WEEK, 1);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MILLISECOND, 0);
      break;
    }
    return cal.getTime();
  }

  public static CoveringInfo getMonthlyCoveringInfo(Date from, Date to) {
    // Move 'from' to end of month, unless its the first day of month
    boolean coverable = true;
    if (!from.equals(DateUtils.truncate(from, MONTH))) {
      from = DateUtils.addMonths(DateUtils.truncate(from, MONTH), 1);
      coverable = false;
    }

    // Move 'to' to beginning of next month, unless its the first day of the month
    if (!to.equals(DateUtils.truncate(to, MONTH))) {
      to = DateUtils.truncate(to, MONTH);
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
    if (!from.equals(DateUtils.truncate(from, MONTH))) {
      from = DateUtils.addMonths(DateUtils.truncate(from, MONTH), 1);
      coverable = false;
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    int fromMonth = cal.get(MONTH);

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
    if (!from.equals(DateUtils.truncate(from, MONTH))) {
      from = DateUtils.addMonths(DateUtils.truncate(from, MONTH), 1);
      coverable = false;
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(from);
    int fromMonth = cal.get(MONTH);
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
    int fromYear = cal.get(YEAR);

    cal.clear();
    cal.set(YEAR, fromYear);
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
    case CONTINUOUS:
      return getMilliSecondCoveringInfo(from, to, 1000);
    case MINUTELY:
    case HOURLY:
    case DAILY:
      return getMilliSecondCoveringInfo(from, to, interval.weight());
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

  private static CoveringInfo getMilliSecondCoveringInfo(Date from, Date to, long millisInInterval) {
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
    if (fromDate.before(toDate)) {
      return getCoveringInfo(fromDate, toDate, updatePeriod).getCountBetween();
    } else {
      return -getCoveringInfo(toDate, fromDate, updatePeriod).getCountBetween();
    }
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

  @EqualsAndHashCode
  public static class TimeDiff {
    int quantity;
    UpdatePeriod updatePeriod;

    private TimeDiff(int quantity, UpdatePeriod updatePeriod) {
      this.quantity = quantity;
      this.updatePeriod = updatePeriod;
    }

    public static TimeDiff parseFrom(String diffStr) throws LensException {
      // Get the relative diff part to get eventual date based on now.
      Matcher qtyMatcher = P_QUANTITY.matcher(diffStr);
      int qty = 1;
      if (qtyMatcher.find()) {
        qty = Integer.parseInt(qtyMatcher.group());
      }

      Matcher signageMatcher = P_SIGNAGE.matcher(diffStr);
      if (signageMatcher.find()) {
        String sign = signageMatcher.group();
        if ("-".equals(sign)) {
          qty = -qty;
        }
      }

      Matcher unitMatcher = P_UNIT.matcher(diffStr);
      if (unitMatcher.find()) {
        return new TimeDiff(qty, UpdatePeriod.fromUnitName(unitMatcher.group().toLowerCase()));
      }
      return new TimeDiff(0, UpdatePeriod.CONTINUOUS);
    }

    public Date offsetFrom(Date time) {
      return DateUtils.add(time, updatePeriod.calendarField(), quantity);
    }

    public Date negativeOffsetFrom(Date time) {
      return DateUtils.add(time, updatePeriod.calendarField(), -quantity);
    }
  }

}
