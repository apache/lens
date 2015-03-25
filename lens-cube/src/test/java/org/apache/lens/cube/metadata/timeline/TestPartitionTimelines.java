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

import org.testng.Assert;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Lists;

public class TestPartitionTimelines {
  CubeMetastoreClient client = null;
  private static final String TABLE_NAME = "storage_fact";
  private static final UpdatePeriod PERIOD = UpdatePeriod.HOURLY;
  private static final String PART_COL = "pt";
  private static final Date DATE = new Date();
  private static final List<Class<? extends PartitionTimeline>> TIMELINE_IMPLEMENTATIONS = Arrays.asList(
    StoreAllPartitionTimeline.class,
    EndsAndHolesPartitionTimeline.class,
    RangesPartitionTimeline.class
  );

  @Test
  public void testPropertiesContractsForAllSubclasses() throws LensException {
    for (Class<? extends PartitionTimeline> clazz : TIMELINE_IMPLEMENTATIONS) {
      testPropertiesContract(clazz);
    }
  }

  @Test
  public void testEquivalence() throws LensException {
    for (int j = 0; j < 10; j++) {
      Random randomGenerator = new Random();
      List<PartitionTimeline> timelines = Lists.newArrayList();
      for (Class<? extends PartitionTimeline> clazz : TIMELINE_IMPLEMENTATIONS) {
        timelines.add(getInstance(clazz));
      }
      final List<TimePartition> addedPartitions = Lists.newArrayList();
      for (int i = 0; i < 200; i++) {
        int randomInt = randomGenerator.nextInt(100) - 50;
        TimePartition part = TimePartition.of(PERIOD, timeAtHourDiff(randomInt));
        addedPartitions.add(part);
        for (PartitionTimeline timeline : timelines) {
          timeline.add(part);
        }
      }
      Iterator<TimePartition> sourceOfTruth = timelines.get(0).iterator();
      List<Iterator<TimePartition>> otherIterators = Lists.newArrayList();
      for (int i = 1; i < TIMELINE_IMPLEMENTATIONS.size() - 1; i++) {
        otherIterators.add(timelines.get(i).iterator());
      }
      while (sourceOfTruth.hasNext()) {
        TimePartition cur = sourceOfTruth.next();
        for (Iterator<TimePartition> iterator : otherIterators) {
          Assert.assertTrue(iterator.hasNext());
          Assert.assertEquals(iterator.next(), cur);
        }
      }
      for (Iterator<TimePartition> iterator : otherIterators) {
        Assert.assertFalse(iterator.hasNext());
      }
      Collections.shuffle(addedPartitions);
      Iterator<TimePartition> iter = addedPartitions.iterator();
      while (iter.hasNext()) {
        TimePartition part = iter.next();
        iter.remove();
        if (!addedPartitions.contains(part)) {
          for (PartitionTimeline timeline : timelines) {
            timeline.drop(part);
          }
        }
      }
      for (PartitionTimeline timeline : timelines) {
        Assert.assertTrue(timeline.isEmpty());
      }
    }
  }

  private Date timeAtHourDiff(int d) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(DATE);
    cal.add(PERIOD.calendarField(), d);
    return cal.getTime();
  }

  private <T extends PartitionTimeline> T getInstance(Class<T> clz) {
    try {
      return clz.getConstructor(String.class, UpdatePeriod.class, String.class)
        .newInstance(TABLE_NAME, PERIOD, PART_COL);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private <T extends PartitionTimeline> void testPropertiesContract(Class<T> clz) throws LensException {
    T inst1 = getInstance(clz);
    T inst2 = getInstance(clz);
    Map<String, String> props = inst1.toProperties();
    Assert.assertTrue(inst2.initFromProperties(props));
    Assert.assertEquals(inst1, inst2);
    Assert.assertTrue(inst1.isEmpty());
    Assert.assertTrue(inst2.isEmpty());
    Assert.assertTrue(inst1.add(TimePartition.of(PERIOD, DATE)));
    Assert.assertFalse(inst1.equals(inst2));
    Assert.assertTrue(inst2.add(TimePartition.of(PERIOD, DATE)));
    Assert.assertTrue(inst1.isConsistent());
    Assert.assertTrue(inst2.isConsistent());
    Assert.assertEquals(inst1, inst2);
    Assert.assertTrue(inst2.initFromProperties(props));
    Assert.assertFalse(inst1.equals(inst2));
    Assert.assertTrue(inst2.initFromProperties(inst1.toProperties()));
    Assert.assertEquals(inst1, inst2);
  }
}
