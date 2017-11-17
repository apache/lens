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
package org.apache.lens.server.api.query.cost;

import static org.testng.Assert.*;

import org.apache.lens.api.query.QueryCostType;
import org.apache.lens.api.serialize.SerializationTest;
import org.apache.lens.server.api.error.LensException;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestFactPartitionBasedQueryCost {

  RangeBasedQueryCostTypeDecider costRangeQueryTypeDecider =
    new RangeBasedQueryCostTypeDecider(new QueryCostTypeRangeConf("VERY_LOW,0.0,LOW,0.1,HIGH"));
  QueryCost cost0 = new FactPartitionBasedQueryCost(0.0);
  QueryCost cost1 = new FactPartitionBasedQueryCost(0.2);
  QueryCost cost11 = new FactPartitionBasedQueryCost(0.2);
  QueryCost cost2 = new FactPartitionBasedQueryCost(0.3);
  QueryCost scost0 = new StaticQueryCost(-1.0);
  QueryCost scost1 = new StaticQueryCost(0.0);
  QueryCost scost2 = new StaticQueryCost(1.0);

  @BeforeTest
  public void beforeTest() throws LensException {
    cost0.setQueryCostType(costRangeQueryTypeDecider.decideCostType(cost0));
    cost1.setQueryCostType(costRangeQueryTypeDecider.decideCostType(cost1));
    cost11.setQueryCostType(costRangeQueryTypeDecider.decideCostType(cost11));
    cost2.setQueryCostType(costRangeQueryTypeDecider.decideCostType(cost2));
    scost0.setQueryCostType(costRangeQueryTypeDecider.decideCostType(scost0));
    scost1.setQueryCostType(costRangeQueryTypeDecider.decideCostType(scost1));
    scost2.setQueryCostType(costRangeQueryTypeDecider.decideCostType(scost2));
  }
  @Test(expectedExceptions = {IllegalArgumentException.class})
  public void testInvalid() {
    new FactPartitionBasedQueryCost(-0.5);
  }

  @Test
  public void testAdd() throws Exception {
    assertEquals(cost1.add(cost2), new FactPartitionBasedQueryCost(0.5));
  }

  @Test
  public void testGetQueryCostType() throws Exception {
    assertEquals(cost1.getQueryCostType(), QueryCostType.HIGH);
    assertEquals(cost2.getQueryCostType(), QueryCostType.HIGH);
    assertEquals(cost0.getQueryCostType(), QueryCostType.LOW);
    assertEquals(scost0.getQueryCostType(), QueryCostType.VERY_LOW);
  }

  @Test(expectedExceptions = {UnsupportedOperationException.class})
  public void testGetEstimatedExecTimeMillis() throws Exception {
    cost1.getEstimatedExecTimeMillis();
  }

  @Test
  public void testGetEstimatedResourceUsage() throws Exception {
    assertEquals(cost1.getEstimatedResourceUsage(), 0.2);
  }

  @Test
  public void testCompareTo() throws Exception {
    assertEquals(cost1.compareTo(cost2), -1);
    assertEquals(cost2.compareTo(cost1), 1);
    assertEquals(cost1.compareTo(cost11), 0);
    assertEquals(scost1.compareTo(scost0), 1);
    assertEquals(scost0.compareTo(scost1), -1);
    assertEquals(scost2.compareTo(scost0), 1);
  }

  @Test
  public void testEquals() throws Exception {
    assertTrue(cost1.equals(cost11));
    assertTrue(cost11.equals(cost1));
    assertFalse(cost1.equals(cost2));
  }

  @Test
  public void testFactPartitionBasedQueryCostIsSerializable() {
    new SerializationTest().verifySerializationAndDeserialization(new FactPartitionBasedQueryCost(Double.MAX_VALUE));
  }

  @Test
  public void testStaticQueryCostIsSerializable() {
    new SerializationTest().verifySerializationAndDeserialization(new StaticQueryCost(Double.MAX_VALUE));
  }
}
