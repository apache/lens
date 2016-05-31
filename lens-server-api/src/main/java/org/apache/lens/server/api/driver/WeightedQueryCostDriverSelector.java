/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.driver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import static org.apache.lens.server.api.LensConfConstants.DEFAULT_DRIVER_WEIGHT;
import static org.apache.lens.server.api.LensConfConstants.DRIVER_WEIGHT;

import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class WeightedQueryCostDriverSelector implements DriverSelector {
  /**
   * Returns the driver that has the minimum query cost according to predefined driver allocation ratios.
   *
   * @param ctx  the context
   * @param conf the conf
   * @return the lens driver
   */
  @Override
  public LensDriver select(final AbstractQueryContext ctx, final Configuration conf) {

    final Collection<LensDriver> drivers = ctx.getDriverContext().getDriversWithValidQueryCost();

    log.info("Candidate drivers: {}", drivers);
    if (log.isDebugEnabled()) {
      for (LensDriver driver : drivers) {
        log.debug("Cost on driver {}: {}", driver, ctx.getDriverQueryCost(driver));
        log.debug("Driver ratio: {}", driver.getConf().getDouble(DRIVER_WEIGHT, DEFAULT_DRIVER_WEIGHT));
      }
      log.debug("Driver ratio: " + DEFAULT_DRIVER_WEIGHT + " => lens.driver.weight not set for that driver.");
    }

    //The min-cost driver
    final LensDriver minCostDriver = Collections.min(drivers, new Comparator<LensDriver>() {
      @Override
      public int compare(LensDriver d1, LensDriver d2) {
        final QueryCost c1 = ctx.getDriverQueryCost(d1);
        final QueryCost c2 = ctx.getDriverQueryCost(d2);
        return c1.compareTo(c2);
      }
    });

    //The collection of minimum cost drivers
    final QueryCost minCost = ctx.getDriverQueryCost(minCostDriver);
    final ArrayList<LensDriver> eligibleDrivers = new ArrayList<>();
    for (LensDriver driver : drivers) {
      if (ctx.getDriverQueryCost(driver).equals(minCost)) {
        eligibleDrivers.add(driver);
      }
    }
    //Return the minCostDriver if there is only one min cost driver
    if (eligibleDrivers.size() == 1){
      return minCostDriver;
    }
    final Collection<LensDriver> minCostDrivers = Collections.unmodifiableCollection(eligibleDrivers);

    //The driverRatio Sum across all minimum cost drivers
    double driverRatioSum = 0;
    for (LensDriver driver : minCostDrivers) {
      driverRatioSum = driverRatioSum + driver.getConf().getDouble(DRIVER_WEIGHT, DEFAULT_DRIVER_WEIGHT);
    }

    //Weighted random allocation
    driverRatioSum = Math.random()*driverRatioSum;

    for (LensDriver driver : minCostDrivers){
      driverRatioSum -= driver.getConf().getDouble(DRIVER_WEIGHT, DEFAULT_DRIVER_WEIGHT);
      if (driverRatioSum <= 0){
        return driver;
      }
    }
    return minCostDriver;
  }
}
