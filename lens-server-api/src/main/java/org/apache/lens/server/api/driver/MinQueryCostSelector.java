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
package org.apache.lens.server.api.driver;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;

import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MinQueryCostSelector implements DriverSelector {

  /**
   * Returns the driver that has the minimum query cost.
   *
   * @param ctx  the context
   * @param conf the conf
   * @return the lens driver
   */
  @Override
  public LensDriver select(final AbstractQueryContext ctx, final Configuration conf) {

    final Collection<LensDriver> drivers = ctx.getDriverContext().getDriversWithValidQueryCost();
    log.info("Candidate drivers: {}", drivers);
    for (LensDriver driver : drivers) {
      log.debug("Cost on driver {}: {}", driver, ctx.getDriverQueryCost(driver));
    }
    return Collections.min(drivers, new Comparator<LensDriver>() {
      @Override
      public int compare(LensDriver d1, LensDriver d2) {
        final QueryCost c1 = ctx.getDriverQueryCost(d1);
        final QueryCost c2 = ctx.getDriverQueryCost(d2);
        return c1.compareTo(c2);
      }
    });
  }
}
