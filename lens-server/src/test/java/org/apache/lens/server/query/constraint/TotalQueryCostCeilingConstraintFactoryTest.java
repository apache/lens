/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.query.constraint;

import static org.testng.Assert.assertEquals;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.common.ConfigBasedObjectCreationFactory;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.query.cost.FactPartitionBasedQueryCost;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

public class TotalQueryCostCeilingConstraintFactoryTest {

  @DataProvider
  public Object[][] dpTestCreate() {
    return new Object[][] {
      {"-1.0", new TotalQueryCostCeilingConstraint(Optional.<QueryCost>absent())},
      {"0.0", new TotalQueryCostCeilingConstraint(Optional.<QueryCost>of(new FactPartitionBasedQueryCost(0.0)))},
      {"90.0", new TotalQueryCostCeilingConstraint(Optional.<QueryCost>of(new FactPartitionBasedQueryCost(90.0)))},
    };
  }
  @Test(dataProvider = "dpTestCreate")
  public void testCreate(final String totalQueryCostCeilingPerUser, final QueryLaunchingConstraint expectedConstraint) {

    Configuration conf = new Configuration();
    conf.set(LensConfConstants.TOTAL_QUERY_COST_CEILING_PER_USER_KEY, totalQueryCostCeilingPerUser);

    ConfigBasedObjectCreationFactory<QueryLaunchingConstraint> fac = new TotalQueryCostCeilingConstraintFactory();
    assertEquals(fac.create(conf), expectedConstraint);
  }
}
