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

import java.util.*;

import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DriverSelectorQueryContext {

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
  protected Map<LensDriver, DriverQueryContext> driverQueryContextMap = new HashMap<>();
  private Set<LensDriver> blackListedDrivers = Sets.newHashSet();

  public DriverSelectorQueryContext(final String userQuery, final Configuration queryConf,
    final Collection<LensDriver> drivers) {
    this(userQuery, queryConf, drivers, true);
  }

  public DriverSelectorQueryContext(final String userQuery, final Configuration queryConf,
    final Collection<LensDriver> drivers, boolean mergeConf) {
    for (LensDriver driver : drivers) {
      DriverQueryContext ctx = new DriverQueryContext(driver);
      if (mergeConf) {
        ctx.setDriverSpecificConf(mergeConf(driver, queryConf));
      } else {
        ctx.setDriverSpecificConf(queryConf);
      }
      String metricId = ctx.driverSpecificConf.get(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY);
      if (!StringUtils.isBlank(metricId)) {
        ctx.driverSpecificConf.set(LensConfConstants.QUERY_METRIC_DRIVER_STACK_NAME,
          metricId + "-" + driver.getFullyQualifiedName());
      }
      ctx.setQuery(userQuery);
      driverQueryContextMap.put(driver, ctx);
    }
  }

  public void blacklist(LensDriver selectedDriver) {
    blackListedDrivers.add(selectedDriver);
  }

  public static class DriverQueryContext {

    @Getter
    protected LensDriver driver;

    DriverQueryContext(LensDriver driver) {
      this.driver = driver;
    }

    /**
     * Driver's query cost
     */
    @Getter
    @Setter
    private QueryCost driverCost;

    /**
     * Driver's query plan
     */
    @Getter
    @Setter
    protected DriverQueryPlan driverQueryPlan;

    /**
     * Driver specific query conf
     */
    @Getter
    @Setter
    protected Configuration driverSpecificConf;

    @Getter
    @Setter
    /** exceptions occurred while rewriting query for driver */
    protected Exception driverQueryRewriteError;

    @Getter
    @Setter
    /** exceptions occurred while trying to estimate cost */
    protected Exception driverQueryCostEstimateError;

    @Getter
    @Setter
    /** exceptions occurred while trying to generate plans by explain call */
    protected Exception driverQueryPlanGenerationError;

    @Getter
    @Setter
    /** driver query */
    protected String query;

    @Getter
    @Setter
    /** final driver query - after driver rewrites the query*/
    protected String finalDriverQuery;

    @Getter
    @Setter
    /**
     * Plan generated from rewriter. This does not contain final driver plan. This contains the information
     * extracted at rewriter only
     */
    protected DriverQueryPlan rewriterPlan;
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
  void setDriverQueries(Map<LensDriver, String> driverQueries) {
    for (LensDriver driver : driverQueries.keySet()) {
      setDriverQuery(driver, driverQueries.get(driver));
    }
  }

  public void setDriverQuery(LensDriver driver, String driverQuery) {
    final DriverQueryContext driverQueryContext = driverQueryContextMap.get(driver);
    driverQueryContext.setQuery(driverQuery);
  }

  /**
   * Sets driver queries, generates plans for each driver by calling explain with respective queries,
   * Sets driverQueryPlans
   *
   * @param driverQueries
   * @throws LensException
   */
  public void setDriverQueryPlans(Map<LensDriver, String> driverQueries, AbstractQueryContext qctx)
    throws LensException {
    StringBuilder detailedFailureCause = new StringBuilder();
    String failureCause = null;
    boolean useBuilder = false;
    boolean succeededOnAtleastOneDriver = false;
    for (LensDriver driver : driverQueries.keySet()) {
      final DriverQueryContext driverQueryContext = driverQueryContextMap.get(driver);
      driverQueryContext.setQuery(driverQueries.get(driver));
      try {
        driverQueryContext.setDriverQueryPlan(driver.explain(qctx));
        succeededOnAtleastOneDriver = true;
      } catch (Exception e) {
        log.error("Setting driver plan failed for driver {}", driver, e);
        String expMsg = LensUtil.getCauseMessage(e);
        driverQueryContext.setDriverQueryPlanGenerationError(e);
        detailedFailureCause.append("\n Driver :").append(driver.getFullyQualifiedName());
        detailedFailureCause.append(" Cause :" + expMsg);
        if (failureCause != null && !failureCause.equals(expMsg)) {
          useBuilder = true;
        }
        if (failureCause == null) {
          failureCause = expMsg;
        }
      }
    }
    if (!succeededOnAtleastOneDriver) {
      throw new LensException(useBuilder ? detailedFailureCause.toString() : failureCause);
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
    return getSelectedDriver() == null ? null : driverQueryContextMap.get(getSelectedDriver()).getDriverSpecificConf();
  }

  public String getSelectedDriverQuery() {
    return getSelectedDriver() == null ? null : driverQueryContextMap.get(getSelectedDriver()).getQuery();
  }

  public void setDriverConf(LensDriver driver, Configuration conf) {
    driverQueryContextMap.get(driver).setDriverSpecificConf(conf);
  }

  public void setDriverCost(LensDriver driver, QueryCost cost) {
    driverQueryContextMap.get(driver).setDriverCost(cost);
  }

  public void setSelectedDriverQuery(String driverQuery) {
    if (driverQueryContextMap != null && driverQueryContextMap.get(getSelectedDriver()) != null) {
      driverQueryContextMap.get(getSelectedDriver()).setQuery(driverQuery);
    }
  }

  public Collection<LensDriver> getDrivers() {
    return driverQueryContextMap.keySet();
  }
  public Collection<LensDriver> getEligibleDrivers() {
    return Sets.difference(driverQueryContextMap.keySet(), blackListedDrivers);
  }

  public Collection<LensDriver> getDriversWithValidQueryCost() {

    final Set<LensDriver> eligibleDrivers = Sets.newLinkedHashSet();
    for (LensDriver driver: getEligibleDrivers()) {
      if (driverQueryContextMap.get(driver).driverCost != null) {
        eligibleDrivers.add(driver);
      }
    }
    return Collections.unmodifiableCollection(eligibleDrivers);
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

  public void setDriverRewriterPlan(LensDriver driver, DriverQueryPlan rewriterPlan) {
    if (driverQueryContextMap.get(driver) != null) {
      driverQueryContextMap.get(driver).setRewriterPlan(rewriterPlan);
    }
  }

  public DriverQueryPlan getDriverRewriterPlan(LensDriver driver) {
    return driverQueryContextMap.get(driver) != null
      ? driverQueryContextMap.get(driver).getRewriterPlan() : null;
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

  public String getFinalDriverQuery(LensDriver driver) {
    return driverQueryContextMap.get(driver) != null
      ? driverQueryContextMap.get(driver).getFinalDriverQuery() : null;
  }

  public QueryCost getDriverQueryCost(LensDriver driver) {
    return driverQueryContextMap.get(driver) != null
      ? driverQueryContextMap.get(driver).getDriverCost() : null;
  }

  public void setDriverQueryPlan(LensDriver driver, DriverQueryPlan qp) {
    driverQueryContextMap.get(driver).setDriverQueryPlan(qp);
  }

  void clearTransientStateAfterLaunch() {
    for (DriverQueryContext driverCtx : driverQueryContextMap.values()) {
      driverCtx.driverQueryPlan = null;
    }
  }

  void clearTransientStateAfterCompleted() {
    for (DriverQueryContext driverCtx : driverQueryContextMap.values()) {
      driverCtx.driverSpecificConf = null;
      driverCtx.rewriterPlan = null;
    }
  }
}
