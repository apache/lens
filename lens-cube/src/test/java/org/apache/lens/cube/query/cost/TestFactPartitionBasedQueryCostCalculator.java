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
/*
 *
 */
package org.apache.lens.cube.query.cost;

import static org.apache.lens.cube.metadata.UpdatePeriod.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Set;

import org.apache.lens.cube.metadata.FactPartition;
import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import junit.framework.Assert;

public class TestFactPartitionBasedQueryCostCalculator {
  AbstractQueryContext queryContext;
  FactPartitionBasedQueryCostCalculator calculator = new FactPartitionBasedQueryCostCalculator();
  LensDriver driver;

  @BeforeTest
  public void setUp() {
    driver = mock(LensDriver.class);
    when(driver.getConf()).thenReturn(new Configuration());
    queryContext = mock(AbstractQueryContext.class);

    ImmutableMap<String, Double> tableWeights = new ImmutableMap.Builder<String, Double>().build();

    FactPartition fp1 = mockFactPartition(DAILY, tableWeights, 0.7);
    FactPartition fp2 = mockFactPartition(HOURLY, tableWeights, 0.8);
    FactPartition fp3 = mockFactPartition(SECONDLY, tableWeights, 0.4);
    FactPartition fp4 = mockFactPartition(MONTHLY, tableWeights, 0);

    when(queryContext.getTableWeights(driver)).thenReturn(tableWeights);

    HashMap<String, Set<?>> partitions = new HashMap<>();
    partitions.put("st1", Sets.newHashSet(fp1, fp2));
    partitions.put("st2", Sets.newHashSet(fp3, fp4));
    DriverQueryPlan plan = mock(DriverQueryPlan.class);
    when(queryContext.getDriverRewriterPlan(driver)).thenReturn(plan);
    when(plan.getPartitions()).thenReturn(partitions);
    when(calculator.getAllPartitions(queryContext, driver)).thenReturn(partitions);
  }

  private FactPartition mockFactPartition(UpdatePeriod mockPeriod, ImmutableMap<String, Double> tableWeights,
    double mockAllTableWeight) {
    FactPartition fp = mock(FactPartition.class);
    when(fp.getPeriod()).thenReturn(mockPeriod);
    when(fp.getAllTableWeights(tableWeights)).thenReturn(mockAllTableWeight);
    return fp;
  }

  @Test
  public void testCalculateCost() throws Exception {
    QueryCost cost = calculator.calculateCost(queryContext, driver);
    Assert.assertTrue(cost.getEstimatedResourceUsage() > 18.0);
    Assert.assertTrue(cost.getEstimatedResourceUsage() < 19.0);
  }

  @Test
  public void testGetAllPartitions() throws Exception {

  }
}
