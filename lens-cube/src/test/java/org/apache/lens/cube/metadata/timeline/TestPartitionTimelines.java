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

import static org.testng.Assert.*;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import org.apache.lens.cube.metadata.TestTimePartition;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.cube.metadata.UpdatePeriodTest;
import org.apache.lens.server.api.error.LensException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestPartitionTimelines {

  private static final String TABLE_NAME = "storage_fact";
  private static final String PART_COL = "pt";
  private static final List<Class<? extends PartitionTimeline>> TIMELINE_IMPLEMENTATIONS = Arrays.asList(
    StoreAllPartitionTimeline.class,
    EndsAndHolesPartitionTimeline.class,
    RangesPartitionTimeline.class
  );

  @DataProvider(name = "update-periods")
  public Object[][] provideUpdatePeriods() {
    return UpdatePeriodTest.provideUpdatePeriods();
  }

  @DataProvider(name = "update-periods-and-timeline-classes")
  public Object[][] provideUpdatePeriodsAndTimelineClasses() {
    UpdatePeriod[] values = UpdatePeriod.values();
    Object[][] ret = new Object[values.length * TIMELINE_IMPLEMENTATIONS.size()][2];
    for (int i = 0; i < values.length; i++) {
      for (int j = 0; j < TIMELINE_IMPLEMENTATIONS.size(); j++) {
        ret[TIMELINE_IMPLEMENTATIONS.size() * i + j] = new Object[]{
          values[i],
          TIMELINE_IMPLEMENTATIONS.get(j),
        };
      }
    }
    return ret;
  }


  @Test(dataProvider = "update-periods")
  public void testEquivalence(UpdatePeriod period) throws LensException, InvocationTargetException,
    NoSuchMethodException, InstantiationException, IllegalAccessException {
    final Random randomGenerator = new Random();
    for (int j = 0; j < 10; j++) {
      List<PartitionTimeline> timelines = Lists.newArrayList();
      for (Class<? extends PartitionTimeline> clazz : TIMELINE_IMPLEMENTATIONS) {
        timelines.add(getInstance(clazz, period));
      }
      final List<TimePartition> addedPartitions = Lists.newArrayList();
      for (int i = 0; i < 20; i++) {
        int randomInt = randomGenerator.nextInt(10) - 5;
        TimePartition part = TimePartition.of(period, TestTimePartition.timeAtDiff(TestTimePartition.NOW, period,
          randomInt));
        addedPartitions.add(part);
        for (PartitionTimeline timeline : timelines) {
          timeline.add(part);
        }
        assertSameTimelines(timelines);
      }
      assertSameTimelines(timelines);
      Collections.shuffle(addedPartitions);
      Iterator<TimePartition> iter = addedPartitions.iterator();
      while (iter.hasNext()) {
        TimePartition part = iter.next();
        iter.remove();
        if (!addedPartitions.contains(part)) {
          for (PartitionTimeline timeline : timelines) {
            timeline.drop(part);
            assertTrue(timeline.isConsistent());
          }
        }
      }
      for (PartitionTimeline timeline : timelines) {
        assertTrue(timeline.isEmpty());
      }
    }
  }

  public static void assertSameTimelines(List<PartitionTimeline> timelines) {
    List<Iterator<TimePartition>> iterators = Lists.newArrayList();
    for (PartitionTimeline timeline : timelines) {
      iterators.add(timeline.iterator());
    }

    while (iterators.get(0).hasNext()) {
      Map<Class, TimePartition> parts = Maps.newHashMap();
      for (Iterator<TimePartition> iterator : iterators) {
        assertTrue(iterator.hasNext());
        parts.put(iterator.getClass(), iterator.next());
      }
      assertEquals(new HashSet<>(parts.values()).size(), 1, "More than one values for next: " + parts.values());
    }
    for (Iterator<TimePartition> iterator : iterators) {
      assertFalse(iterator.hasNext());
    }
  }


  private <T extends PartitionTimeline> T getInstance(Class<T> clz, UpdatePeriod period) throws
    NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    return clz.getConstructor(String.class, UpdatePeriod.class, String.class)
      .newInstance(TABLE_NAME, period, PART_COL);
  }

  @Test(dataProvider = "update-periods-and-timeline-classes")
  public <T extends PartitionTimeline> void testPropertiesContract(UpdatePeriod period, Class<T> clz) throws
    LensException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    // Make two instances, one to modify, other to validate against
    T inst1 = getInstance(clz, period);
    T inst2 = getInstance(clz, period);
    // whenever we'll init from props, timeline should become empty.
    Map<String, String> props = inst1.toProperties();
    assertTrue(inst2.initFromProperties(props));
    // init from props of an empty timeline: should succeed and make the timeline empty
    assertEquals(inst1, inst2);
    assertTrue(inst1.isEmpty());
    assertTrue(inst2.isEmpty());
    // Add single partition and test for non-equivalence
    assertTrue(inst1.add(TimePartition.of(period, TestTimePartition.NOW)));
    assertFalse(inst1.equals(inst2));
    // add same parittion in other timeline, test for equality
    assertTrue(inst2.add(TimePartition.of(period, TestTimePartition.NOW)));
    assertTrue(inst1.isConsistent());
    assertTrue(inst2.isConsistent());
    assertEquals(inst1, inst2);
    // init with blank properties. Should become empty
    assertTrue(inst2.initFromProperties(props));
    assertFalse(inst1.equals(inst2));
    // init from properties of timeline with single partition.
    assertTrue(inst2.initFromProperties(inst1.toProperties()));
    assertEquals(inst1, inst2);
    // clear timelines
    inst1.initFromProperties(props);
    inst2.initFromProperties(props);
    // Make sparse partition range in one, init other from its properties. Test equality.
    for (int i = 0; i < 500; i++) {
      assertTrue(inst1.add(TimePartition.of(period, TestTimePartition.timeAtDiff(TestTimePartition.NOW, period,
        i * 2))));
    }
    assertTrue(inst1.isConsistent());
    inst2.initFromProperties(inst1.toProperties());
    assertTrue(inst2.isConsistent());
    assertEquals(inst1, inst2);
  }
}
