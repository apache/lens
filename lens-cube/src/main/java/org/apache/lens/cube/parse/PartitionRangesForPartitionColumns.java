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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.metadata.timeline.RangesPartitionTimeline;
import org.apache.lens.server.api.error.LensException;

import com.google.common.collect.Sets;


public class PartitionRangesForPartitionColumns extends HashMap<String, RangesPartitionTimeline> {

  public void add(String partCol, TimePartition partition) throws LensException {
    if (get(partCol) == null) {
      put(partCol, new RangesPartitionTimeline("", UpdatePeriod.values()[0], partCol));
    }
    get(partCol).add(partition.withUpdatePeriod(UpdatePeriod.values()[0])
      .rangeUpto(partition.next().withUpdatePeriod(UpdatePeriod.values()[0])));
  }

  public Set<String> toSet(Set<String> partColsQueried) {
    Set<String> ret = Sets.newHashSet();
    for (Map.Entry<String, RangesPartitionTimeline> entry : entrySet()) {
      if (partColsQueried.contains(entry.getKey())) {
        ret.add(entry.getKey() + ":" + entry.getValue().getRanges());
      }
    }
    return ret;
  }

  public void add(FactPartition part) throws LensException {
    add(part.getPartCol(), part.getTimePartition());
  }
}
