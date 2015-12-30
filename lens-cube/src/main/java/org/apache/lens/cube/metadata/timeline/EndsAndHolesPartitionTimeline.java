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


import java.util.*;

import org.apache.lens.cube.metadata.MetastoreUtil;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.error.LensException;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

/**
 * One implementation of PartitionTimeline that stores first partition, latest partition and a collection of holes in
 * between them, excluding the edges(start and end values).  This is the default Timeline for tables that don't specify
 * which Timeline Class to use in it's params.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class EndsAndHolesPartitionTimeline extends PartitionTimeline {
  private TimePartition first;
  private TreeSet<TimePartition> holes = Sets.newTreeSet();
  private TimePartition latest;

  public EndsAndHolesPartitionTimeline(String storageTableName, UpdatePeriod updatePeriod,
    String partCol) {
    super(storageTableName, updatePeriod, partCol);
  }

  @Override
  public boolean add(@NonNull TimePartition partition) throws LensException {
    if (isEmpty()) {
      // First partition being added
      first = partition;
      latest = partition;
      return true;
    }
    if (partition.before(first)) {
      addHolesBetween(partition, first, partition.getUpdatePeriod());
      first = partition;
      return true;
    } else if (partition.after(latest)) {
      addHolesBetween(latest, partition, partition.getUpdatePeriod());
      latest = partition;
      return true;
    } else {
      return holes.remove(partition);
    }
  }

  @Override
  public boolean drop(@NonNull TimePartition toDrop) throws LensException {
    if (first.equals(latest) && first.equals(toDrop)) {
      this.first = null;
      this.latest = null;
      this.holes.clear();
      return true;
    } else if (first.equals(toDrop)) {
      this.first = this.getNextPartition(first, latest, 1);
      return true;
    } else if (latest.equals(toDrop)) {
      this.latest = this.getNextPartition(latest, first, -1);
      return true;
    } else {
      return addHole(toDrop);
    }
  }

  @Override
  public TimePartition latest() {
    return latest;
  }

  @Override
  public Map<String, String> toProperties() {
    HashMap<String, String> ret = Maps.newHashMap();
    ret.put("first", "");
    ret.put("latest", "");
    MetastoreUtil.addNameStrings(ret, "holes", holes);
    if (isEmpty()) {
      return ret;
    }
    ret.put("first", first.getDateString());
    ret.put("latest", latest.getDateString());
    return ret;
  }

  @Override
  public boolean initFromProperties(Map<String, String> properties) throws LensException {
    first = null;
    latest = null;
    holes.clear();
    String firstStr = properties.get("first");
    String latestStr = properties.get("latest");
    String holesStr = MetastoreUtil.getNamedStringValue(properties, "holes");
    if (!Strings.isNullOrEmpty(firstStr)) {
      first = TimePartition.of(getUpdatePeriod(), firstStr);
    }
    if (!Strings.isNullOrEmpty(latestStr)) {
      latest = TimePartition.of(getUpdatePeriod(), latestStr);
    }
    holes = Sets.newTreeSet();
    if (!Strings.isNullOrEmpty(holesStr)) {
      for (String hole : holesStr.split("\\s*,\\s*")) {
        holes.add(TimePartition.of(getUpdatePeriod(), hole));
      }
    }
    return isConsistent();
  }

  private boolean addHole(TimePartition toDrop) {
    return holes.add(toDrop);
  }

  private void addHolesBetween(TimePartition begin, TimePartition end, UpdatePeriod updatePeriod) throws LensException {
    for (Date date : TimeRange.iterable(begin.next().getDate(), end.getDate(), updatePeriod, 1)) {
      addHole(TimePartition.of(updatePeriod, date));
    }
  }

  private TimePartition getNextPartition(TimePartition begin, TimePartition end, int increment) throws LensException {
    for (Date date : TimeRange.iterable(begin.partitionAtDiff(increment).getDate(),
      end.partitionAtDiff(increment).getDate(), begin.getUpdatePeriod(), increment)) {
      TimePartition value = TimePartition.of(begin.getUpdatePeriod(), date);
      if (!holes.contains(value)) {
        return value;
      } else {
        holes.remove(value);
      }
    }
    return null;
  }


  public boolean isEmpty() {
    return first == null && latest == null && holes.isEmpty();
  }

  @Override
  public boolean isConsistent() {
    if (first == null && latest != null) {
      return false;
    }
    if (latest == null && first != null) {
      return false;
    }
    for (TimePartition hole : holes) {
      if (!hole.after(first) || !hole.before(latest)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean exists(TimePartition toCheck) {
    return !isEmpty() && !toCheck.before(first) && !toCheck.after(latest) && !holes.contains(toCheck);
  }

  @Override
  public Iterator<TimePartition> iterator() {

    return new Iterator<TimePartition>() {
      TimePartition cur = getFirst();

      @Override
      public boolean hasNext() {
        return cur != null && getLatest() != null && !cur.after(getLatest());
      }

      @Override
      public TimePartition next() {
        while (holes.contains(cur)) {
          cur = cur.next();
        }
        TimePartition toReturn = cur;
        cur = cur.next();
        return toReturn;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
