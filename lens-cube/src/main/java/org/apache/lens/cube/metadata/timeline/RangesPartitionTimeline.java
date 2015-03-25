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

import org.apache.lens.api.LensException;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.UpdatePeriod;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.ToString;

/**
 * One implementation of PartitionTimeline that stores first partition, latest partition and a collection of holes in
 * between them, excluding the edges(start and end values).  This is the default Timeline for tables that don't specify
 * which Timeline Class to use in it's params.
 */
@Data
@ToString(callSuper = true)
public class RangesPartitionTimeline extends PartitionTimeline {
  private List<TimePartition.TimePartitionRange> ranges = Lists.newArrayList();

  public RangesPartitionTimeline(CubeMetastoreClient client, String storageTableName, UpdatePeriod updatePeriod,
    String partCol) {
    super(client, storageTableName, updatePeriod, partCol);
  }

  @Override
  public boolean add(TimePartition partition) throws LensException {
    int ind = findIndexToInsert(partition);
    if (ind == 0 || !ranges.get(ind - 1).contains(partition)) {
      ranges.add(ind, partition.singletonRange());
    }
    mergeRanges();
    return true;
  }

  private void mergeRanges() {
    for (int i = 0; i < ranges.size() - 1; i++) {
      if (ranges.get(i).getEnd().equals(ranges.get(i + 1).getBegin())) {
        TimePartition.TimePartitionRange removed = ranges.remove(i + 1);
        ranges.get(i).setEnd(removed.getEnd());
        i--; // check again at same index
      }
    }
  }

  private int findIndexToInsert(TimePartition partition) {
    //TODO: binary search
    int i = 0;
    for (; i < ranges.size(); i++) {
      if (ranges.get(i).getBegin().after(partition)) {
        break;
      }
    }
    return i;
  }

  @Override
  public boolean drop(TimePartition toDrop) throws LensException {
    if (morePartitionsExist(toDrop.getDateString())) {
      return true;
    }
    //TODO: binary search
    for (int i = 0; i < ranges.size(); i++) {
      TimePartition.TimePartitionRange cur = ranges.get(i);
      if (cur.contains(toDrop)) {
        ranges.add(i, cur.getBegin().rangeUpto(toDrop));
        ranges.get(i + 1).setBegin(toDrop.next());
      }
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
    if (isEmpty()) {
      return ret;
    }
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (TimePartition.TimePartitionRange range : ranges) {
      sb.append(sep);
      sep = ",";
      sb.append(range.getBegin()).append(sep).append(range.getEnd());
    }
    ret.put("ranges", sb.toString());
    return ret;
  }

  @Override
  public boolean initFromProperties(Map<String, String> properties) throws LensException {
    ranges.clear();
    String rangesStr = properties.get("ranges");
    if (!Strings.isNullOrEmpty(rangesStr)) {
      String[] split = rangesStr.split("\\s*,\\s*");
      if (split.length % 2 == 1) {
        throw new LensException("Ranges incomplete");
      }
      for (int i = 0; i < split.length; i += 2) {
        ranges.add(TimePartition.of(getUpdatePeriod(), split[i]).rangeUpto(TimePartition.of(getUpdatePeriod(),
          split[i + 1])));
      }
    }
    return isConsistent();
  }


  public boolean isEmpty() {
    return ranges.isEmpty();
  }

  @Override
  public boolean isConsistent() {
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
    for (TimePartition.TimePartitionRange range : ranges) {
      if (range.contains(toCheck)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Iterator<TimePartition> iterator() {

    return new Iterator<TimePartition>() {
      Iterator<TimePartition.TimePartitionRange> uber = ranges.iterator();
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
}
