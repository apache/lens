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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lens.api.LensConf;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.DriverQueryPlan;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.metrics.MethodMetricsContext;
import org.apache.lens.server.api.metrics.MethodMetricsFactory;
import org.apache.lens.server.api.query.DriverSelectorQueryContext.DriverQueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractQueryContext implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * The user query.
   */
  @Getter
  protected String userQuery;
  /**
   * The replaced user query.
   */
  @Getter
  @Setter
  protected String phase1RewrittenQuery;

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
   * The selected Driver query cost
   */
  @Getter
  @Setter
  protected QueryCost selectedDriverQueryCost;

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

  private final String database;

  /** Lock used to synchronize HiveConf access */
  private transient Lock hiveConfLock = new ReentrantLock();

  protected AbstractQueryContext(final String query, final String user, final LensConf qconf, final Configuration conf,
    final Collection<LensDriver> drivers, boolean mergeDriverConf) {
    if (conf.getBoolean(LensConfConstants.ENABLE_QUERY_METRICS, LensConfConstants.DEFAULT_ENABLE_QUERY_METRICS)) {
      UUID metricId = UUID.randomUUID();
      conf.set(LensConfConstants.QUERY_METRIC_UNIQUE_ID_CONF_KEY, metricId.toString());
      log.info("Generated metric id: {} for query: {}", metricId, query);
    }
    driverContext = new DriverSelectorQueryContext(query, conf, drivers, mergeDriverConf);
    userQuery = query;
    phase1RewrittenQuery = query;
    this.lensConf = qconf;
    this.conf = conf;
    this.submittedUser = user;
    // we are setting selectedDriverQuery as user query only when the drivers size is 1
    // if drivers size is more than the driver query will be set after selection over drivers
    if (drivers != null && drivers.size() == 1) {
      this.selectedDriverQuery = query;
      setSelectedDriver(drivers.iterator().next());
    }

    // If this is created under an 'acquire' current db would be set
    if (SessionState.get() != null) {
      String currDb = SessionState.get().getCurrentDatabase();
      database = currDb == null ? "default" : currDb;
    } else {
      database = "default";
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
      log.info("Running estimate for driver {}", driver);
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

  public Map<String, Double> getTableWeights(LensDriver driver) {
    return getDriverContext().getDriverRewriterPlan(driver).getTableWeights();
  }

  public DriverQueryPlan getDriverRewriterPlan(LensDriver driver) {
    return getDriverContext().getDriverRewriterPlan(driver);
  }

  public String getQueue() {
    return getConf().get(LensConfConstants.MAPRED_JOB_QUEUE_NAME);
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

    @Getter
    private LensException cause;

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
      } catch (final LensException e) {
        this.cause = e;
        captureExceptionInformation(driverQueryContext, e);
      } catch (final Exception e) {
        captureExceptionInformation(driverQueryContext, e);
      } finally {
        estimateGauge.markSuccess();
      }
    }

    private void captureExceptionInformation(final DriverQueryContext driverQueryContext, final Exception e) {
      String expMsg = LensUtil.getCauseMessage(e);
      driverQueryContext.setDriverQueryCostEstimateError(e);
      failureCause = new StringBuilder("Driver :")
        .append(driver.getFullyQualifiedName())
        .append(" Cause :")
        .append(expMsg)
        .toString();
      log.error("Setting driver cost failed for driver {} Cause: {}", driver, failureCause, e);
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
   * @return QueryCostTO
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
   */
  public void setDriverQuery(LensDriver driver, String query) {
    driverContext.setDriverQuery(driver, query);
    isDriverQueryExplicitlySet = true;
  }

  public void setDriverCost(LensDriver driver, QueryCost cost) {
    driverContext.setDriverCost(driver, cost);
  }

  /**
   * Get handle of the query for logging purposes
   * @return
   */
  public abstract String getLogHandle();

  /**
   * Returns database set while launching query
   * @return
   */
  public String getDatabase() {
    return database == null ? "default" : database;
  }

  public void clearTransientStateAfterLaunch() {
    driverContext.clearTransientStateAfterLaunch();
  }

  public void clearTransientStateAfterCompleted() {
    driverContext.clearTransientStateAfterCompleted();
    hiveConf = null;
  }
}
