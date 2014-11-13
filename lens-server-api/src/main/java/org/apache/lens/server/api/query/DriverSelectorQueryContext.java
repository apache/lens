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
package org.apache.lens.server.api.query;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.lens.api.LensException;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensDriver;

import java.util.HashMap;
import java.util.Map;

public abstract class DriverSelectorQueryContext {

  /** The constant LOG */
  public static final Log LOG = LogFactory.getLog(DriverSelectorQueryContext.class);

  /** The conf. */
  @Getter @Setter
  transient protected Configuration conf;

  /** The selected driver. */
  @Getter
  @Setter
  transient protected LensDriver selectedDriver;

  /** The driver query. */
  @Getter
  @Setter
  protected String driverQuery;
  /** Map of driver to driver query */
  @Getter
  protected Map<LensDriver, String> driverQueries;

  /** Map of driver to query plan */
  @Getter
  protected Map<LensDriver, DriverQueryPlan> driverQueryPlans;

  /** Map of exceptions occurred while trying to generate plans by explain call */
  protected Map<LensDriver, Exception> driverQueryPlanGenerationErrors;

  /**
   * Sets driver queries, generates plans for each driver by calling explain with respective queries,
   * Sets driverQueryPlans
   * @param driverQueries
   * @throws LensException
   * @see #driverQueryPlans
   */
  public void setDriverQueriesAndPlans(Map<LensDriver, String> driverQueries) throws LensException {
    driverQueryPlanGenerationErrors = new HashMap<LensDriver, Exception>();
    this.driverQueries = driverQueries;
    this.driverQueryPlans = new HashMap<LensDriver, DriverQueryPlan>();
    for (LensDriver driver : driverQueries.keySet()) {
      DriverQueryPlan plan = null;
      try {
        plan = driver.explain(driverQueries.get(driver), getConf());
      } catch (Exception e) {
        LOG.error(e.getStackTrace());
        driverQueryPlanGenerationErrors.put(driver, e);
      }
      driverQueryPlans.put(driver, plan);
    }
  }

  /**
   * Return selected driver's query plan, but check for null conditions first.
   * @return DriverQueryPlan of Selected Driver
   * @throws LensException
   */
  public DriverQueryPlan getSelectedDriverQueryPlan() throws LensException {
    if(getDriverQueryPlans() == null) {
      throw new LensException("No Driver query plans. Check if re-write happened or not");
    }
    if(getSelectedDriver() == null) {
      throw new LensException("Selected Driver is NULL.");
    }
    if(getDriverQueryPlans().get(getSelectedDriver()) == null) {
      throw new LensException("Driver Query Plan of the selected driver is null",
        driverQueryPlanGenerationErrors.get(getSelectedDriver()));
    }
    return getDriverQueryPlans().get(getSelectedDriver());
  }
}
