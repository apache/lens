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

import java.util.*;

public class DriverSelectorQueryContext {

  /**
   * The constant LOG
   */
  public static final Log LOG = LogFactory.getLog(DriverSelectorQueryContext.class);


  /**
   * The selected driver.
   */
  @Getter
  @Setter
  protected LensDriver selectedDriver;

  /**
   * Map of driver to driver specific query context
   */
  @Getter
  @Setter
  protected Map<LensDriver, DriverQueryContext> driverQueryContextMap = new HashMap<LensDriver,
    DriverQueryContext>();

  public DriverSelectorQueryContext(final String userQuery, final Configuration queryConf,
    final Collection<LensDriver> drivers) {
    for (LensDriver driver : drivers) {
      DriverQueryContext ctx = new DriverQueryContext(driver);
      ctx.setDriverSpecificConf(mergeConf(driver, queryConf));
      ctx.setQuery(userQuery);
      driverQueryContextMap.put(driver, ctx);
    }
  }

  public static class DriverQueryContext {

    @Getter
    protected LensDriver driver;

    DriverQueryContext(LensDriver driver) {
      this.driver = driver;
    }

    /**
     * Map of driver to query plan
     */
    @Getter
    @Setter
    protected DriverQueryPlan driverQueryPlan;

    /**
     * driver specific query conf
     */
    @Getter
    @Setter
    protected Configuration driverSpecificConf;

    @Getter
    @Setter
    /** exceptions occurred while trying to generate plans by explain call */
    protected Exception driverQueryPlanGenerationError;

    @Getter
    @Setter
    /** driver query */
    protected String query;

  }

  /**
   * Gets the driver query conf.
   *
   * @param driver    the driver
   * @param queryConf the query conf
   * @return the final query conf
   */
  private Configuration mergeConf(LensDriver driver, Configuration queryConf) {
    Configuration conf = new Configuration(driver.getConf());
    for (Map.Entry<String, String> entry : queryConf) {
      conf.set(entry.getKey(), entry.getValue());
    }
    conf.setClassLoader(queryConf.getClassLoader());
    return conf;
  }

  /**
   * Sets driver queries, generates plans for each driver by calling explain with respective queries,
   * Sets driverQueryPlans
   *
   * @param driverQueries
   * @throws LensException
   */
  public void setDriverQueriesAndPlans(Map<LensDriver, String> driverQueries) throws LensException {
    for (LensDriver driver : driverQueries.keySet()) {
      final DriverQueryContext driverQueryContext = driverQueryContextMap.get(driver);
      driverQueryContext.setQuery(driverQueries.get(driver));
      try {
        driverQueryContext.setDriverQueryPlan(driver.explain(driverQueries.get(driver),
          driverQueryContext.getDriverSpecificConf()));
      } catch (Exception e) {
        LOG.error(e.getStackTrace());
        driverQueryContext.setDriverQueryPlanGenerationError(e);
      }
    }
  }

  /**
   * Return selected driver's query plan, but check for null conditions first.
   *
   * @return DriverQueryPlan of Selected Driver
   * @throws LensException
   */
  public DriverQueryPlan getSelectedDriverQueryPlan() throws LensException {
    final Map<LensDriver, DriverQueryContext> driverQueryCtxs = getDriverQueryContextMap();
    if (driverQueryCtxs == null) {
      throw new LensException("No Driver query ctx. Check if re-write happened or not");
    }
    if (getSelectedDriver() == null) {
      throw new LensException("Selected Driver is NULL.");
    }

    if (driverQueryCtxs.get(getSelectedDriver()) == null) {
      throw new LensException("Could not find Driver Context for selected driver " + getSelectedDriver());
    }

    if (driverQueryCtxs.get(getSelectedDriver()).getDriverQueryPlanGenerationError() != null) {
      throw new LensException("Driver Query Plan of the selected driver is null",
        driverQueryCtxs.get(getSelectedDriver()).getDriverQueryPlanGenerationError());
    }
    return driverQueryCtxs.get(getSelectedDriver()).getDriverQueryPlan();
  }

  public Configuration getSelectedDriverConf() {
    return driverQueryContextMap.get(getSelectedDriver()).getDriverSpecificConf();
  }

  public String getSelectedDriverQuery() {
    return driverQueryContextMap.get(getSelectedDriver()).getQuery();
  }

  public void setDriverConf(LensDriver driver, Configuration conf) {
    driverQueryContextMap.get(driver).setDriverSpecificConf(conf);
  }

  public void setSelectedDriverQuery(String driverQuery) {
    if (driverQueryContextMap != null && driverQueryContextMap.get(getSelectedDriver()) != null) {
      driverQueryContextMap.get(getSelectedDriver()).setQuery(driverQuery);
    }
  }

  public Collection<LensDriver> getDrivers() {
    return driverQueryContextMap.keySet();
  }

  public Collection<String> getDriverQueries() {
    List<String> queries = new ArrayList<String>();
    final Collection<DriverQueryContext> values = driverQueryContextMap.values();
    for (DriverQueryContext ctx : values) {
      if (ctx.getQuery() != null) {
        queries.add(ctx.getQuery());
      }
    }
    return queries;
  }

  public DriverQueryPlan getDriverQueryPlan(LensDriver driver) {
    return driverQueryContextMap.get(driver) != null
      ? driverQueryContextMap.get(driver).getDriverQueryPlan() : null;
  }

  public Configuration getDriverConf(LensDriver driver) {
    return driverQueryContextMap.get(driver) != null
      ? driverQueryContextMap.get(driver).getDriverSpecificConf() : null;
  }

  public String getDriverQuery(LensDriver driver) {
    return driverQueryContextMap.get(driver) != null
      ? driverQueryContextMap.get(driver).getQuery() : null;
  }
}
