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

import org.apache.lens.api.LensException;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.parse.TimeRange;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.NonNull;

/**
 * One implementation of PartitionTimeline that stores first partition, latest partition and a collection of holes in
 * between them, excluding the edges(start and end values).  This is the default Timeline for tables that don't specify
 * which Timeline Class to use in it's params.
 */
@Data
public class EndsAndHolesPartitionTimeline extends PartitionTimeline {
  private TimePartition first;
  private TreeSet<TimePartition> holes = new TreeSet<TimePartition>();
  private TimePartition latest;

  public EndsAndHolesPartitionTimeline(CubeMetastoreClient client, String storageTableName, UpdatePeriod updatePeriod,
    String partCol) {
    super(client, storageTableName, updatePeriod, partCol);
  }

  @Override
  public boolean add(TimePartition partition) throws LensException {
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
  public boolean add(@NonNull Collection<TimePartition> partitions) throws LensException {
    boolean result = true;
    for (TimePartition partition : partitions) {
      result &= add(partition);
    }
    // Can also return the failed to add items.
    return result;
  }

  @Override
  public boolean drop(TimePartition toDrop) throws LensException {
    if (morePartitionsExist(toDrop.getDateString())) {
      return true;
    }
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
    if (isEmpty()) {
      return ret;
    }
    ret.put("first", first.getDateString());
    ret.put("latest", latest.getDateString());
    ret.put("holes", StringUtils.join(holes, ","));
    return ret;
  }

  @Override
  public boolean initFromProperties(Map<String, String> properties) throws LensException {
    first = TimePartition.of(getUpdatePeriod(), properties.get("first"));
    latest = TimePartition.of(getUpdatePeriod(), properties.get("latest"));
    holes = Sets.newTreeSet();
    String holesStr = properties.get("holes");
    if (!Strings.isNullOrEmpty(holesStr)) {
      for (String hole : properties.get("holes").split("\\s*,\\s*")) {
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
