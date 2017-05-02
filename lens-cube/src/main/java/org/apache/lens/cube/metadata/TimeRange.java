/*
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

import static java.util.Comparator.naturalOrder;

import static org.apache.lens.cube.metadata.DateUtil.ABSDATE_PARSER;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

/**
 * Timerange data structure
 */
@JsonIgnoreProperties({"astNode", "parent"})
@Data
@EqualsAndHashCode(of = {"partitionColumn", "fromDate", "toDate"})
@Builder
public class TimeRange {
  private final String partitionColumn;
  private final Date toDate;
  @NonNull
  private final Date fromDate;
  private final ASTNode astNode;
  private final ASTNode parent;
  private final int childIndex;

  public TimeRange truncate(Date candidateStartTime, Date candidateEndTime) {
    return TimeRange.builder().partitionColumn(getPartitionColumn())
      .fromDate(Stream.of(getFromDate(), candidateStartTime).max(naturalOrder()).orElse(candidateStartTime))
      .toDate(Stream.of(getToDate(), candidateEndTime).min(naturalOrder()).orElse(candidateEndTime))
      .build();
  }

  public boolean isCoverableBy(Set<UpdatePeriod> updatePeriods) {
    return DateUtil.isCoverableBy(fromDate, toDate, updatePeriods);
  }

  /**
   * Truncate time range using the update period.
   * The lower value of the truncated time range is the smallest date value equal to or larger than original
   * time range's lower value which lies at the update period's boundary. Similarly for higher value.
   * @param updatePeriod   Update period to truncate time range with
   * @return               truncated time range
   * @throws LensException If the truncated time range is invalid.
   */
  public TimeRange truncate(UpdatePeriod updatePeriod) throws LensException {
    TimeRange timeRange = TimeRange.builder().partitionColumn(partitionColumn)
      .fromDate(updatePeriod.getCeilDate(fromDate)).toDate(updatePeriod.getFloorDate(toDate)).build();
    timeRange.validate();
    return timeRange;
  }

  public long milliseconds() {
    return toDate.getTime() - fromDate.getTime();
  }


  public TimeRangeBuilder cloneAsBuilder() {
    return builder().
      astNode(getAstNode()).childIndex(getChildIndex()).parent(getParent()).partitionColumn(getPartitionColumn());
  }
  private boolean fromEqualsTo() {
    return fromDate.equals(toDate);
  }
  private boolean fromAfterTo() {
    return fromDate.after(toDate);
  }
  public boolean isValid() {
    return !(fromEqualsTo() || fromAfterTo());
  }
  public void validate() throws LensException {
    if (partitionColumn == null || fromDate == null || toDate == null || fromEqualsTo()) {
      throw new LensException(LensCubeErrorCode.INVALID_TIME_RANGE.getLensErrorInfo());
    }

    if (fromAfterTo()) {
      throw new LensException(LensCubeErrorCode.FROM_AFTER_TO.getLensErrorInfo(),
          fromDate.toString(), toDate.toString());
    }
  }

  public String toTimeDimWhereClause(String prefix, String column) {
    if (StringUtils.isNotBlank(column)) {
      column = prefix + "." + column;
    }
    return column + " >= '" + DateUtil.HIVE_QUERY_DATE_PARSER.get().format(fromDate) + "'"
      + " AND " + column + " < '" + DateUtil.HIVE_QUERY_DATE_PARSER.get().format(toDate) + "'";
  }

  @Override
  public String toString() {
    return partitionColumn + " [" + ABSDATE_PARSER.get().format(fromDate) + " to "
      + ABSDATE_PARSER.get().format(toDate) + ")";
  }

  /** iterable from fromDate(including) to toDate(excluding) incrementing increment units of updatePeriod */
  public static Iterable iterable(Date fromDate, Date toDate, UpdatePeriod updatePeriod, int increment) {
    return TimeRange.builder().fromDate(fromDate).toDate(toDate).build().iterable(updatePeriod, increment);
  }

  /** iterable from fromDate(including) incrementing increment units of updatePeriod. Do this numIters times */
  public static Iterable iterable(Date fromDate, int numIters, UpdatePeriod updatePeriod, int increment) {
    return TimeRange.builder().fromDate(fromDate).build().iterable(updatePeriod, numIters, increment);
  }

  private Iterable iterable(UpdatePeriod updatePeriod, int numIters, int increment) {
    return new Iterable(updatePeriod, numIters, increment);
  }

  public Iterable iterable(UpdatePeriod updatePeriod, int increment) {
    if (increment == 0) {
      throw new UnsupportedOperationException("Can't iterate if iteration increment is zero");
    }
    long numIters = DateUtil.getTimeDiff(fromDate, toDate, updatePeriod) / increment;
    return new Iterable(updatePeriod, numIters, increment);
  }

  /** Iterable so that foreach is supported */
  public class Iterable implements java.lang.Iterable<Date> {
    private UpdatePeriod updatePeriod;
    private long numIters;
    private int increment;

    Iterable(UpdatePeriod updatePeriod, long numIters, int increment) {
      this.updatePeriod = updatePeriod;
      this.numIters = numIters;
      if (this.numIters < 0) {
        this.numIters = 0;
      }
      this.increment = increment;
    }

    @Override
    public Iterator iterator() {
      return new Iterator();
    }

    public class Iterator implements java.util.Iterator<Date> {
      Calendar calendar;
      // Tracks the index of the item returned after the last next() call.
      // Index here refers to the index if the iterator were iterated and converted into a list.
      @Getter
      int counter = -1;

      public Iterator() {
        calendar = Calendar.getInstance();
        calendar.setTime(fromDate);
      }

      @Override
      public boolean hasNext() {
        return counter < numIters - 1;
      }

      @Override
      public Date next() {
        Date cur = calendar.getTime();
        updatePeriod.increment(calendar, increment);
        counter++;
        return cur;
      }

      public Date peekNext() {
        return calendar.getTime();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove from timerange iterator");
      }

      public long getNumIters() {
        return numIters;
      }
    }
  }
}
