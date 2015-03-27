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

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.query.QueryCost;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.metrics.MethodMetricsContext;
import org.apache.lens.server.api.metrics.MethodMetricsFactory;
import org.apache.lens.server.api.query.DriverSelectorQueryContext.DriverQueryContext;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import lombok.Getter;
import lombok.Setter;

public abstract class AbstractQueryContext implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * The Constant LOG
   */
  public static final Log LOG = LogFactory.getLog(AbstractQueryContext.class);

  /**
   * The user query.
   */
  @Getter
  protected String userQuery;

  /**
   * The merged Query conf.
   */
  @Getter
  @Setter
  protected transient Configuration conf;

  /**
   * The hive Conf.
   */
  protected transient HiveConf hiveConf;

  /**
   * The query conf.
   */
  @Getter
  protected LensConf lensConf;

  /**
   * The driver ctx
   */
  @Getter
  @Setter
  protected transient DriverSelectorQueryContext driverContext;

  /**
   * The selected Driver query.
   */
  protected String selectedDriverQuery;

  /**
   * The submitted user.
   */
  @Getter
  private final String submittedUser; // Logged in user.

  /**
   * The lens session identifier.
   */
  @Getter
  @Setter
  private String lensSessionIdentifier;

  /**
   * Will be set to true when the driver queries are explicitly set
   * This will help avoiding rewrites in case of system restarts.
   */
  @Getter private boolean isDriverQueryExplicitlySet = false;

  /**
   * Is olap cube query or not
   */
  @Getter
  @Setter
  private boolean olapQuery = false;

  /** Lock used to synchronize HiveConf access */
  private transient Lock hiveConfLock = new ReentrantLock();

  protected AbstractQueryContext(final String query, final String user, final LensConf qconf, final Configuration conf,
    final Collection<LensDriver> drivers, boolean mergeDriverConf) {
    if (conf.getBoolean(LensConfConstants.ENABLE_QUERY_METRICS, LensConfConstants.DEFAULT_ENABLE_QUERY_METRICS)) {
      UUID metricId = UUID.randomUUID();
      conf.set(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, metricId.toString());
      LOG.info("Generated metric id: " + metricId + " for query: " + query);
    }
    driverContext = new DriverSelectorQueryContext(query, conf, drivers, mergeDriverConf);
    userQuery = query;
    this.lensConf = qconf;
    this.conf = conf;
    this.submittedUser = user;
    // we are setting selectedDriverQuery as user query only when the drivers size is 1
    // if drivers size is more than the driver query will be set after selection over drivers
    if (drivers != null && drivers.size() == 1) {
      this.selectedDriverQuery = query;
      setSelectedDriver(drivers.iterator().next());
    }
  }

  // called after the object is constructed from serialized object
  public void initTransientState() {
    hiveConfLock = new ReentrantLock();
  }

  /**
   * Set driver queries
   *
   * @param driverQueries Map of LensDriver to driver's query
   * @throws LensException
   */
  public void setDriverQueries(Map<LensDriver, String> driverQueries) throws LensException {
    driverContext.setDriverQueries(driverQueries);
    isDriverQueryExplicitlySet = true;
  }

  /**
   * Estimate cost for each driver and set in context
   *
   * @throws LensException
   *
   */
  public void estimateCostForDrivers() throws LensException {
    Map<LensDriver, DriverEstimateRunnable> estimateRunnables = getDriverEstimateRunnables();
    for (LensDriver driver : estimateRunnables.keySet()) {
      LOG.info("Running estimate for driver " + driver);
      estimateRunnables.get(driver).run();
    }
  }

  /**
   * Get runnables wrapping estimate computation, which could be processed offline
   */
  public Map<LensDriver, DriverEstimateRunnable> getDriverEstimateRunnables() throws LensException {
    Map<LensDriver, DriverEstimateRunnable> estimateRunnables = new HashMap<LensDriver, DriverEstimateRunnable>();

    for (LensDriver driver : driverContext.getDrivers()) {
      estimateRunnables.put(driver, new DriverEstimateRunnable(this, driver));
    }

    return estimateRunnables;
  }

  /**
   * Runnable to wrap estimate computation for a driver. Failure cause and success status
   * are stored as field members
   */
  public static class DriverEstimateRunnable implements Runnable {
    private final AbstractQueryContext queryContext;
    private final LensDriver driver;

    @Getter
    private String failureCause = null;

    @Getter
    private boolean succeeded = false;


    public DriverEstimateRunnable(AbstractQueryContext queryContext,
                                  LensDriver driver) {
      this.queryContext = queryContext;
      this.driver = driver;
    }

    @Override
    public void run() {
      MethodMetricsContext estimateGauge =
        MethodMetricsFactory.createMethodGauge(queryContext.getDriverConf(driver), true, "driverEstimate");
      DriverQueryContext driverQueryContext = queryContext.getDriverContext().getDriverQueryContextMap().get(driver);
      if (driverQueryContext.getDriverQueryRewriteError() != null) {
        // skip estimate
        return;
      }

      try {
        driverQueryContext.setDriverCost(driver.estimate(queryContext));
        succeeded = true;
      } catch (Exception e) {
        String expMsg = LensUtil.getCauseMessage(e);
        driverQueryContext.setDriverQueryCostEstimateError(e);
        failureCause = new StringBuilder("Driver :")
            .append(driver.getClass().getName())
            .append(" Cause :")
            .append(expMsg)
            .toString();
        LOG.error("Setting driver cost failed for driver " + driver + " Cause: " + failureCause, e);
      } finally {
        estimateGauge.markSuccess();
      }
    }
  }

  /**
   * Wrapper method for convenience on driver context
   *
   * @return the selected driver's query
   */
  public String getSelectedDriverQuery() {
    if (selectedDriverQuery != null) {
      return selectedDriverQuery;
    } else if (driverContext != null) {
      return driverContext.getSelectedDriverQuery();
    }
    return null;
  }

  /**
   * Get driver query
   *
   * @param driver
   *
   * @return query
   */
  public String getDriverQuery(LensDriver driver) {
    return driverContext.getDriverQuery(driver);
  }

  public String getFinalDriverQuery(LensDriver driver) {
    return driverContext.getFinalDriverQuery(driver);
  }
  /**
   * Get driver conf
   *
   * @param driver
   *
   * @return Configuration
   */
  public Configuration getDriverConf(LensDriver driver) {
    return driverContext.getDriverConf(driver);
  }

  /**
   * Get query cost for the driver
   *
   * @param driver
   * @return QueryCost
   */
  public QueryCost getDriverQueryCost(LensDriver driver) {
    return driverContext.getDriverQueryCost(driver);
  }
  /**
   * Wrapper method for convenience on driver context
   *
   * @return the selected driver's conf
   */
  public Configuration getSelectedDriverConf() {
    if (driverContext != null) {
      return driverContext.getSelectedDriverConf();
    }
    return null;
  }

  /**
   * Sets the selected driver query for persistence and also in the driver context
   *
   * @param driverQuery
   */
  public void setSelectedDriverQuery(String driverQuery) {
    this.selectedDriverQuery = driverQuery;
    if (driverContext != null) {
      driverContext.setSelectedDriverQuery(driverQuery);
      isDriverQueryExplicitlySet = true;
    }
  }

  /**
   * Wrapper method for convenience on driver context
   *
   * @param driver Lens driver
   */

  public void setSelectedDriver(LensDriver driver) {
    if (driverContext != null) {
      driverContext.setSelectedDriver(driver);
      selectedDriverQuery = driverContext.getSelectedDriverQuery();
    }
  }

  /**
   * Wrapper method for convenience on driver context
   *
   * @return the selected driver
   */
  public LensDriver getSelectedDriver() {
    if (driverContext != null) {
      return driverContext.getSelectedDriver();
    }
    return null;
  }

  /**
   * Wrapper method for convenience on driver context
   *
   * @return the selected driver
   */
  public DriverQueryPlan getSelectedDriverQueryPlan() throws LensException {
    if (driverContext != null) {
      return driverContext.getSelectedDriverQueryPlan();
    }
    return null;
  }

  /**
   * Get selected driver's cost
   *
   * @return QueryCost
   * @throws LensException
   */
  public QueryCost getSelectedDriverQueryCost() throws LensException {
    if (driverContext != null) {
      return driverContext.getSelectedDriverQueryCost();
    }
    return null;
  }

  /**
   * Set exception during rewrite.
   *
   * @param driver
   * @param exp
   */
  public void setDriverRewriteError(LensDriver driver, Exception exp) {
    driverContext.driverQueryContextMap.get(driver).setDriverQueryRewriteError(exp);
  }

  /**
   * Get exception during rewrite.
   *
   * @param driver
   * @return exp
   */
  public Exception getDriverRewriteError(LensDriver driver) {
    return driverContext.driverQueryContextMap.get(driver).getDriverQueryRewriteError();
  }

  /**
   * Gets HiveConf corresponding to query conf.
   *
   * Should be called judiciously, because constructing HiveConf from conf object is costly.
   * The field is set to null after query completion. Should not be accessed after completion.
   * @return
   */
  public HiveConf getHiveConf() {
    hiveConfLock.lock();
    try {
      if (hiveConf == null) {
        hiveConf = new HiveConf(this.conf, this.getClass());
        hiveConf.setClassLoader(this.conf.getClassLoader());
      }
    } finally {
      hiveConfLock.unlock();
    }
    return hiveConf;
  }

  /**
   * Set final driver rewritten query for the driver.
   *
   * @param driver
   * @param rewrittenQuery
   */
  public void setFinalDriverQuery(LensDriver driver, String rewrittenQuery) {
    driverContext.driverQueryContextMap.get(driver).setFinalDriverQuery(rewrittenQuery);
  }

  /**
   * Set query for a given driver
   * @param driver driver instance
   * @param query query string
   * @throws LensException
   */
  public void setDriverQuery(LensDriver driver, String query) {
    driverContext.setDriverQuery(driver, query);
    isDriverQueryExplicitlySet = true;
  }

  /**
   * Get handle of the query for logging purposes
   * @return
   */
  public String getLogHandle() {
    return this.getUserQuery();
  }

  public void clearTransientStateAfterLaunch() {
    driverContext.clearTransientStateAfterLaunch();
  }

  public void clearTransientStateAfterCompleted() {
    driverContext.clearTransientStateAfterCompleted();
    hiveConf = null;
  }
}
