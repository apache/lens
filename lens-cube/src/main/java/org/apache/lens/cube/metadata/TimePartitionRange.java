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

import java.util.Date;
import java.util.Iterator;

import org.apache.lens.server.api.error.LensException;

import lombok.Data;

/**
 * Range of time partition. [begin,end). i.e. inclusive begin and exclusive end.
 */
@Data
public class TimePartitionRange implements Iterable<TimePartition>, Named {
  private TimePartition begin;
  private TimePartition end;

  public static TimePartitionRange between(Date from, Date to, UpdatePeriod period) throws LensException {
    return TimePartition.of(period, from).rangeUpto(TimePartition.of(period, to));
  }

  public TimePartitionRange(TimePartition begin, TimePartition end) throws LensException {
    if (end.before(begin)) {
      throw new LensException("condition of creation of timepartition failed: end>=begin");
    }
    if (end.getUpdatePeriod() != begin.getUpdatePeriod()) {
      throw new LensException("update periods are not same");
    }
    this.begin = begin;
    this.end = end;
  }

  @Override
  public String toString() {
    return "[" + begin.getDateString() + ", " + end.getDateString() + ")";
  }

  /**
   * returns TimePartition objects starting from begin and upto(excluding) end. interval of iteration is the update
   * period of the partitions. Assumes both partitions have same update period.
   */
  @Override
  public Iterator<TimePartition> iterator() {

    return new Iterator<TimePartition>() {
      TimePartition current = begin;

      @Override
      public boolean hasNext() {
        return current.before(end);
      }

      @Override
      public TimePartition next() {
        TimePartition ret = current;
        current = current.next();
        return ret;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove not supported");
      }
    };
  }

  /**
   * @param partition
   * @return begin &lt;= partition &lt; end
   */
  public boolean contains(TimePartition partition) {
    return !partition.before(begin) && partition.before(end);
  }

  /**
   * @return if range is empty range.
   */
  public boolean isEmpty() {
    return begin.equals(end);
  }

  @Override
  public String getName() {
    return toString();
  }

  public static TimePartitionRange parseFrom(UpdatePeriod updatePeriod, String from, String to) throws LensException {
    boolean incrementFrom = false;
    boolean incrementTo = false;
    if (from.charAt(0) == '[') {
      from = from.substring(1);
    } else if (from.charAt(0) == '(') {
      from = from.substring(1);
      incrementFrom = true;
    }
    if (to.charAt(to.length() - 1) == ']') {
      to = to.substring(0, to.length() - 1);
      incrementTo = true;
    } else if (to.charAt(to.length() - 1) == ')') {
      to = to.substring(0, to.length() - 1);
    }
    TimePartition fromPartition = TimePartition.of(updatePeriod, from);
    TimePartition toPartition = TimePartition.of(updatePeriod, to);
    if (incrementFrom) {
      fromPartition = fromPartition.next();
    }
    if (incrementTo) {
      toPartition = toPartition.next();
    }
    return new TimePartitionRange(fromPartition, toPartition);
  }

  public long size() {
    return DateUtil.getTimeDiff(begin.getDate(), end.getDate(), begin.getUpdatePeriod());
  }

  public boolean isValidAndNonEmpty() {
    return begin.before(end);
  }
}
