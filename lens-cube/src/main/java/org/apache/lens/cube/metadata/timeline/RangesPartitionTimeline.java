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
package org.apache.lens.cube.metadata.timeline;


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.TimePartitionRange;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.error.LensException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * One implementation of PartitionTimeline that stores ranges of partition presence, Basically a list of tuples each
 * tuple represents a range of presence. range is of the form [from, end) i.e. including the first element and excluding
 * the second element of the tuple
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class RangesPartitionTimeline extends PartitionTimeline {
  private List<TimePartitionRange> ranges = Lists.newArrayList();

  public RangesPartitionTimeline(String storageTableName, UpdatePeriod updatePeriod,
    String partCol) {
    super(storageTableName, updatePeriod, partCol);
  }

  @Override
  public boolean add(TimePartition partition) throws LensException {
    int ind = getStrictlyAfterIndex(partition);
    int added = 0;
    if (ind > 0) {
      if (ranges.get(ind - 1).contains(partition)) {
        return true;
      }
      if (ranges.get(ind - 1).getEnd().equals(partition)) {
        added++;
        ranges.get(ind - 1).setEnd(partition.next());
      }
    }
    if (ind < ranges.size()) {
      if (partition.equals(ranges.get(ind).getBegin().previous())) {
        added++;
        ranges.get(ind).setBegin(partition);
      }
    }
    switch (added) {
    case 0:
      ranges.add(ind, partition.singletonRange());
      break;
    case 2:
      ranges.get(ind - 1).setEnd(ranges.get(ind).getEnd());
      ranges.remove(ind);
      break;
    case 1:
      // Nothing needs to be done.
    default:
      break;

    }
    return true;
  }

  private int getStrictlyAfterIndex(TimePartition part) {
    int start = 0;
    int end = getRanges().size();
    int mid;
    while (end - start > 0) {
      mid = (start + end) / 2;
      if (ranges.get(mid).getBegin().after(part)) {
        end = mid;
      } else {
        start = mid + 1;
      }
    }
    return end;
  }

  private void mergeRanges() {
    for (int i = 0; i < ranges.size() - 1; i++) {
      if (ranges.get(i).getEnd().equals(ranges.get(i + 1).getBegin())) {
        TimePartitionRange removed = ranges.remove(i + 1);
        ranges.get(i).setEnd(removed.getEnd());
        i--; // check again at same index
      }
    }
  }

  @Override
  public boolean drop(TimePartition toDrop) throws LensException {
    int ind = getStrictlyAfterIndex(toDrop);
    if (ind == 0) {
      return true; // nothing to do
    }
    if (ranges.get(ind - 1).getBegin().equals(toDrop)) {
      ranges.get(ind - 1).setBegin(toDrop.next());
    } else if (ranges.get(ind - 1).getEnd().previous().equals(toDrop)) {
      ranges.get(ind - 1).setEnd(toDrop);
    } else {
      TimePartition end = ranges.get(ind - 1).getEnd();
      ranges.get(ind - 1).setEnd(toDrop);
      ranges.add(ind, toDrop.next().rangeUpto(end));
    }
    if (ranges.get(ind - 1).isEmpty()) {
      ranges.remove(ind - 1);
    }
    return true;
  }


  @Override
  public TimePartition latest() {
    if (isEmpty()) {
      return null;
    }
    return ranges.get(ranges.size() - 1).getEnd().previous();
  }

  @Override
  public Map<String, String> toProperties() {
    HashMap<String, String> ret = Maps.newHashMap();
    MetastoreUtil.addNameStrings(ret, "ranges", ranges);
    return ret;
  }

  @Override
  public boolean initFromProperties(Map<String, String> properties) throws LensException {
    ranges.clear();
    String rangesStr = MetastoreUtil.getNamedStringValue(properties, "ranges");
    if (!Strings.isNullOrEmpty(rangesStr)) {
      String[] split = rangesStr.split("\\s*,\\s*");
      if (split.length % 2 == 1) {
        throw new LensException("Ranges incomplete");
      }
      for (int i = 0; i < split.length; i += 2) {
        ranges.add(TimePartitionRange.parseFrom(getUpdatePeriod(), split[i], split[i + 1]));
      }
    }
    return isConsistent();
  }


  public boolean isEmpty() {
    return ranges.isEmpty();
  }

  @Override
  public boolean isConsistent() {
    if (isEmpty()) {
      return true;
    }
    if (!ranges.get(0).getBegin().before(ranges.get(0).getEnd())) {
      return false;
    }
    for (int i = 0; i < ranges.size() - 1; i++) {
      if (!ranges.get(i).getEnd().before(ranges.get(i + 1).getBegin())) {
        return false;
      }
      if (!ranges.get(i + 1).getBegin().before(ranges.get(i + 1).getEnd())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean exists(TimePartition toCheck) {
    if (isEmpty()) {
      return false;
    }
    for (TimePartitionRange range : ranges) {
      if (range.contains(toCheck)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Iterator<TimePartition> iterator() {

    return new Iterator<TimePartition>() {
      Iterator<TimePartitionRange> uber = ranges.iterator();
      Iterator<TimePartition> cur = null;

      @Override
      public boolean hasNext() {
        if (cur == null || !cur.hasNext()) {
          if (!uber.hasNext()) {
            return false;
          }
          cur = uber.next().iterator();
        }
        return cur.hasNext();
      }

      @Override
      public TimePartition next() {
        return cur.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }


  public long getTimeCovered() {
    long t = 0;
    for (TimePartitionRange range : ranges) {
      t += (range.getEnd().getDate().getTime() - range.getBegin().getDate().getTime());
    }
    return t;
  }
}
