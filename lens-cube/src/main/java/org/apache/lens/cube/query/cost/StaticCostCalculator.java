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

import static org.apache.lens.server.api.LensConfConstants.*;

import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.cost.*;

public class StaticCostCalculator implements QueryCostCalculator {

  private QueryCost queryCost;

  @Override
  public void init(LensDriver lensDriver) throws LensException {
    QueryCostTypeDecider queryCostTypeDecider = new RangeBasedQueryCostTypeDecider(
      lensDriver.getConf().get(DRIVER_COST_TYPE_RANGES, DRIVER_QUERY_COST_TYPE_DEFAULT_RANGES));
    this.queryCost = new StaticQueryCost(lensDriver.getConf().getDouble(DRIVER_QUERY_COST, DEFAULT_DRIVER_QUERY_COST));
    this.queryCost.setQueryCostType(queryCostTypeDecider.decideCostType(this.queryCost));
  }

  @Override
  public QueryCost calculateCost(AbstractQueryContext queryContext, LensDriver driver) throws LensException {
    return this.queryCost;
  }

}
