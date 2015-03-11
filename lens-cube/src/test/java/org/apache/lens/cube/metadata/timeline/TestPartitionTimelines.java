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

import java.util.Date;
import java.util.Map;

import org.apache.lens.api.LensException;
import org.apache.lens.cube.metadata.CubeMetastoreClient;
import org.apache.lens.cube.metadata.TimePartition;
import org.apache.lens.cube.metadata.UpdatePeriod;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPartitionTimelines {
  CubeMetastoreClient client = null;
  private static final String TABLE_NAME = "storage_fact";
  private static final UpdatePeriod PERIOD = UpdatePeriod.HOURLY;
  private static final String PART_COL = "pt";
  private static final Date DATE = new Date();

  @Test
  public void testPropertiesContractsForAllSubclasses() throws LensException {
    testPropertiesContract(StoreAllPartitionTimeline.class);
    testPropertiesContract(EndsAndHolesPartitionTimeline.class);
  }

  private <T extends PartitionTimeline> T getInstance(Class<T> clz) {
    try {
      return clz.getConstructor(CubeMetastoreClient.class, String.class, UpdatePeriod.class, String.class)
        .newInstance(client, TABLE_NAME, PERIOD, PART_COL);
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
