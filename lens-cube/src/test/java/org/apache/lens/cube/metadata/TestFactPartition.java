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

import static org.apache.lens.cube.metadata.UpdatePeriod.DAILY;
import static org.apache.lens.cube.metadata.UpdatePeriod.HOURLY;

import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.Map;

import org.testng.annotations.Test;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class TestFactPartition {
  public static final Date DATE = new Date();
  FactPartition fp1 = new FactPartition("p", DATE, DAILY, null, null, Sets.newHashSet("st1", "st2"));
  FactPartition fp2 = new FactPartition("q", DATE, HOURLY, fp1, null, Sets.newHashSet("st3", "st4"));

  @Test
  public void testGetFormattedFilter() throws Exception {
    String dailyFormat = DAILY.format(DATE);
    String hourlyFormat = HOURLY.format(DATE);
    assertEquals(fp1.getFormattedFilter("table"), "table.p = '" + dailyFormat + "'");
    assertEquals(fp2.getFormattedFilter("table2"),
      "table2.p = '" + dailyFormat + "' AND table2.q = '" + hourlyFormat + "'");
  }

  @Test
  public void testGetAllTableWeights() throws Exception {
    Map<String, Double> weights = Maps.newHashMap();
    assertEquals(fp1.getAllTableWeights(ImmutableMap.copyOf(weights)), 0.0);
    weights.put("st1", 0.2);
    weights.put("st2", 0.3);
    assertEquals(fp1.getAllTableWeights(ImmutableMap.copyOf(weights)), 0.5);
  }
}
