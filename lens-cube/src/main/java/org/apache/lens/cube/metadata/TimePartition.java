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

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang3.time.DateUtils;

import lombok.Data;
import lombok.NonNull;

/** stores a partition's update period, date and string representation. Provides some utility methods around it */
@Data
public class TimePartition implements Comparable<TimePartition>, Named {
  private static final String UPDATE_PERIOD_WRONG_ERROR_MESSAGE = "Update period %s not correct for parsing %s";
  private final UpdatePeriod updatePeriod;
  private final Date date;
  private final String dateString;

  private TimePartition(@NonNull UpdatePeriod updatePeriod, @NonNull Date date) {
    this.updatePeriod = updatePeriod;
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    this.date = truncate(date, updatePeriod);
    this.dateString = updatePeriod.format().format(this.date);
  }

  public static TimePartition of(UpdatePeriod updatePeriod, Date date) throws LensException {
    if (date == null) {
      throw new LensException("time parition date is null");
    }
    return new TimePartition(updatePeriod, date);
  }

  public static TimePartition of(UpdatePeriod updatePeriod, String dateString) throws LensException {
    if (dateString == null || dateString.isEmpty()) {
      throw new LensException("time parition date string is null or blank");
    } else {
      if (!updatePeriod.canParseDateString(dateString)) {
        throw new LensException(getWrongUpdatePeriodMessage(updatePeriod, dateString));
      }
      try {
        return TimePartition.of(updatePeriod, updatePeriod.format().parse(dateString));
      } catch (ParseException e) {
        throw new LensException(getWrongUpdatePeriodMessage(updatePeriod, dateString), e);
      }
    }
  }

  public String toString() {
    return dateString;
  }

  @Override
  public int compareTo(TimePartition o) {
    if (o == null) {
      return 1;
    }
    return this.date.compareTo(o.date);
  }

  public TimePartition partitionAtDiff(int increment) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    if (getUpdatePeriod().equals(UpdatePeriod.QUARTERLY)) {
      increment *= 3;
    }
    cal.add(updatePeriod.calendarField(), increment);
    return new TimePartition(updatePeriod, cal.getTime());
  }

  public TimePartition previous() {
    return partitionAtDiff(-1);
  }

  public TimePartition next() {
    return partitionAtDiff(1);
  }

  public boolean before(TimePartition when) {
    return this.date.before(when.date);
  }

  public boolean after(TimePartition when) {
    return this.date.after(when.date);
  }

  private Date truncate(Date date, UpdatePeriod updatePeriod) {
    if (updatePeriod.equals(UpdatePeriod.WEEKLY)) {
      Date truncDate = DateUtils.truncate(date, Calendar.DAY_OF_MONTH);
      Calendar cal = Calendar.getInstance();
      cal.setTime(truncDate);
      cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek());
      return cal.getTime();
    } else if (updatePeriod.equals(UpdatePeriod.QUARTERLY)) {
      Date dt = DateUtils.truncate(date, updatePeriod.calendarField());
      dt.setMonth(dt.getMonth() - dt.getMonth() % 3);
      return dt;
    } else {
      return DateUtils.truncate(date, updatePeriod.calendarField());
    }
  }

  protected static String getWrongUpdatePeriodMessage(UpdatePeriod up, String dateString) {
    return String.format(UPDATE_PERIOD_WRONG_ERROR_MESSAGE, up, dateString);
  }

  public TimePartitionRange rangeUpto(TimePartition to) throws LensException {
    return new TimePartitionRange(this, to);
  }

  public TimePartitionRange rangeFrom(TimePartition from) throws LensException {
    return new TimePartitionRange(from, this);
  }

  public TimePartitionRange singletonRange() throws LensException {
    return rangeUpto(next());
  }

  @Override
  public String getName() {
    return getDateString();
  }
}
