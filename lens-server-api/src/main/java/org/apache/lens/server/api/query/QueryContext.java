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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.Priority;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.DriverQueryStatus;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.query.collect.WaitingQueriesSelectionPolicy;
import org.apache.lens.server.api.query.constraint.QueryLaunchingConstraint;
import org.apache.lens.server.api.query.priority.QueryPriorityDecider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class QueryContext.
 */
@ToString
@Slf4j
public class QueryContext extends AbstractQueryContext {

  /**
   * The Constant serialVersionUID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * The query handle.
   */
  @Getter
  @Setter
  private QueryHandle queryHandle;

  /**
   * The priority.
   */
  @Getter
  private Priority priority;

  /**
   * The is persistent.
   */
  @Getter
  private final boolean isPersistent;

  /**
   * The is driver persistent.
   */
  @Getter
  private boolean isDriverPersistent;

  /**
   * The status.
   */
  @Getter
  private QueryStatus status;

  /**
   * The result set path.
   */
  @Getter
  @Setter
  private String resultSetPath;

  /**
   * The hdfsout path.
   */
  @Getter
  @Setter
  private String driverResultPath;

  /**
   * The submission time.
   */
  @Getter
  private final long submissionTime;

  /**
   * The launch time.
   */
  @Getter
  @Setter
  private long launchTime;

  /**
   * The end time.
   */
  @Getter
  @Setter
  private long endTime;

  /**
   * The closed time.
   */
  @Getter
  @Setter
  private long closedTime;

  /**
   * The driver op handle.
   */
  @Getter
  @Setter
  private String driverOpHandle;

  /**
   * The driver status.
   */
  @Getter
  final DriverQueryStatus driverStatus;

  /**
   * The query output formatter.
   */

  @Getter
  @Setter
  private QueryOutputFormatter queryOutputFormatter;

  /**
   * The finished query persisted.
   */
  @Getter
  @Setter
  private boolean finishedQueryPersisted = false;

  /**
   * The query name.
   */
  @Getter
  @Setter
  private String queryName;

  /**
   * Creates context from query
   *
   * @param query the query
   * @param user  the user
   * @param qconf The query lensconf
   * @param conf  the conf
   */
  public QueryContext(String query, String user, LensConf qconf, Configuration conf, Collection<LensDriver> drivers) {
    this(query, user, qconf, conf, drivers, null, true);
  }

  /**
   * Creates context from PreparedQueryContext
   *
   * @param prepared the prepared
   * @param user     the user
   * @param qconf    the qconf
   * @param conf     the conf
   */
  public QueryContext(PreparedQueryContext prepared, String user, LensConf qconf, Configuration conf) {
    this(prepared.getUserQuery(), user, qconf, mergeConf(prepared.getConf(), conf), prepared.getDriverContext()
      .getDriverQueryContextMap().keySet(), prepared.getDriverContext().getSelectedDriver(), true);
    setDriverContext(prepared.getDriverContext());
    setSelectedDriverQuery(prepared.getSelectedDriverQuery());
    setSelectedDriverQueryCost(prepared.getSelectedDriverQueryCost());
  }

  /**
   * Create context by passing drivers and selected driver
   *
   * @param userQuery The user query
   * @param user Submitted user
   * @param qconf The query LensConf
   * @param conf The configuration object
   * @param drivers All the drivers
   * @param selectedDriver SelectedDriver
   */
  QueryContext(String userQuery, String user, LensConf qconf, Configuration conf,
    Collection<LensDriver> drivers, LensDriver selectedDriver, boolean mergeDriverConf) {
    this(userQuery, user, qconf, conf, drivers, selectedDriver, System.currentTimeMillis(), mergeDriverConf);
  }

  /**
   * Instantiates a new query context.
   *
   * @param userQuery      the user query
   * @param user           the user
   * @param qconf          the qconf
   * @param conf           the conf
   * @param drivers        All the drivers
   * @param selectedDriver the selected driver
   * @param submissionTime the submission time
   */
  QueryContext(String userQuery, String user, LensConf qconf, Configuration conf,
    Collection<LensDriver> drivers, LensDriver selectedDriver, long submissionTime, boolean mergeDriverConf) {
    super(userQuery, user, qconf, conf, drivers, mergeDriverConf);
    this.submissionTime = submissionTime;
    this.queryHandle = new QueryHandle(UUID.randomUUID());
    this.status = new QueryStatus(0.0f, null, Status.NEW, "Query just got created", false, null, null, null);
    this.lensConf = qconf;
    this.conf = conf;
    this.isPersistent = conf.getBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_SET,
      LensConfConstants.DEFAULT_PERSISTENT_RESULT_SET);
    this.isDriverPersistent = conf.getBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER,
      LensConfConstants.DEFAULT_DRIVER_PERSISTENT_RESULT_SET);
    this.userQuery = userQuery;
    if (selectedDriver != null) {
      this.setSelectedDriver(selectedDriver);
    }
    this.lensConf = qconf;
    this.driverStatus = new DriverQueryStatus();
  }

  /**
   * Utility create method to create context with single driver.
   *
   * @param query The user query
   * @param user The submitted query
   * @param qconf The query lens conf
   * @param conf Query configuration object - merged with session
   * @param driver The driver
   * @param lensSessionPublicId The session id
   *
   * @return QueryContext object
   */
  public static QueryContext createContextWithSingleDriver(String query, String user, LensConf qconf,
    Configuration conf, LensDriver driver, String lensSessionPublicId, boolean mergeDriverConf) {
    QueryContext ctx = new QueryContext(query, user, qconf, conf, Lists.newArrayList(driver), driver, mergeDriverConf);
    ctx.setLensSessionIdentifier(lensSessionPublicId);
    return ctx;
  }

  /**
   * Merge conf.
   *
   * @param prepared the prepared
   * @param current  the current
   * @return the configuration
   */
  private static Configuration mergeConf(Configuration prepared, Configuration current) {
    Configuration conf = new Configuration(false);
    for (Map.Entry<String, String> entry : prepared) {
      conf.set(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : current) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  /**
   * Update conf.
   *
   * @param confoverlay the conf to set
   */
  public void updateConf(Map<String, String> confoverlay) {
    lensConf.getProperties().putAll(confoverlay);
    for (Map.Entry<String, String> prop : confoverlay.entrySet()) {
      this.conf.set(prop.getKey(), prop.getValue());
    }
  }

  public String getResultSetParentDir() {
    return conf.get(LensConfConstants.RESULT_SET_PARENT_DIR, LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT);
  }

  public Path getHDFSResultDir() {
    return new Path(new Path(getResultSetParentDir(), conf.get(LensConfConstants.QUERY_HDFS_OUTPUT_PATH,
      LensConfConstants.DEFAULT_HDFS_OUTPUT_PATH)), queryHandle.toString());
  }

  /**
   * To lens query.
   *
   * @return the lens query
   */
  public LensQuery toLensQuery() {
    return new LensQuery(queryHandle, userQuery, super.getSubmittedUser(), priority, isPersistent,
      getSelectedDriver() != null ? getSelectedDriver().getFullyQualifiedName() : null,
      getSelectedDriverQuery(),
      status,
      resultSetPath, driverOpHandle, lensConf, submissionTime, launchTime, driverStatus.getDriverStartTime(),
      driverStatus.getDriverFinishTime(), endTime, closedTime, queryName);
  }

  public boolean isResultAvailableInDriver() {
    // result is available in driver if driverStatus.isResultSetAvailable() - will be true for fetching inmemory
    // result set.
    // if result is persisted in driver driverStatus.isResultSetAvailable() will be false
    // so, for select queries, if result is persisted in driver, we return true sothat the result can be fetched thru
    // persistent resultset
    return isDriverPersistent() || driverStatus.isResultSetAvailable();
  }

  /**
   * Set whether result is persisted on driver to false. Set by drivers when drivers are not persisting
   *
   */
  public void unSetDriverPersistent() {
    isDriverPersistent = false;
  }

  /*
   * Introduced for Recovering finished query.
   */
  public void setStatusSkippingTransitionTest(final QueryStatus newStatus) {
    this.status = newStatus;
  }

  public synchronized void setStatus(final QueryStatus newStatus) throws LensException {
    validateTransition(newStatus);
    log.info("Updating status of {} from {} to {}", getQueryHandle(), this.status, newStatus);
    this.status = newStatus;
  }

  public String getResultHeader() {
    return getConf().get(LensConfConstants.QUERY_OUTPUT_HEADER);
  }

  public String getResultFooter() {
    return getConf().get(LensConfConstants.QUERY_OUTPUT_FOOTER);
  }

  public String getResultEncoding() {
    return conf.get(LensConfConstants.QUERY_OUTPUT_CHARSET_ENCODING, LensConfConstants.DEFAULT_OUTPUT_CHARSET_ENCODING);
  }

  public String getOuptutFileExtn() {
    return conf.get(LensConfConstants.QUERY_OUTPUT_FILE_EXTN, LensConfConstants.DEFAULT_OUTPUT_FILE_EXTN);
  }

  public boolean getCompressOutput() {
    return conf.getBoolean(LensConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION,
      LensConfConstants.DEFAULT_OUTPUT_ENABLE_COMPRESSION);
  }

  public long getMaxResultSplitRows() {
    return conf.getLong(LensConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS,
      LensConfConstants.DEFAULT_RESULT_SPLIT_MULTIPLE_MAX_ROWS);
  }

  /**
   * Split result into multiple files.
   *
   * @return true, if successful
   */
  public boolean splitResultIntoMultipleFiles() {
    return conf.getBoolean(LensConfConstants.RESULT_SPLIT_INTO_MULTIPLE,
      LensConfConstants.DEFAULT_RESULT_SPLIT_INTO_MULTIPLE);
  }

  public String getClusterUser() {
    return conf.get(LensConfConstants.SESSION_CLUSTER_USER, getSubmittedUser());
  }


  /**
   * Get query handle string
   * @return
   */
  @Override
  public String getLogHandle() {
    return getQueryHandleString();
  }

  public String getQueryHandleString() {
    return queryHandle.getHandleIdString();
  }

  public void validateTransition(final QueryStatus newStatus) throws LensException {
    if (!this.status.isValidTransition(newStatus.getStatus())) {
      throw new LensException("Invalid state transition:from[" + this.status.getStatus() + " to "
        + newStatus.getStatus() + "]");
    }
  }

  public boolean finished() {
    return this.status.finished();
  }

  public boolean launched() {
    return this.status.launched();
  }

  public boolean running() {
    return this.status.running();
  }

  public boolean queued() {
    return this.status.queued();
  }

  public ImmutableSet<QueryLaunchingConstraint> getSelectedDriverQueryConstraints() {
    return getSelectedDriver().getQueryConstraints();
  }

  public ImmutableSet<WaitingQueriesSelectionPolicy> getSelectedDriverSelectionPolicies() {
    return getSelectedDriver().getWaitingQuerySelectionPolicies();
  }

  public Priority decidePriority(LensDriver driver, QueryPriorityDecider queryPriorityDecider) throws LensException {
    // On-demand re-computation of cost, in case it's not alredy set by a previous estimate call.
    // In driver test cases, estimate doesn't happen. Hence this code path ensures cost is computed and
    // priority is set based on correct cost.
    calculateCost(driver);
    priority = queryPriorityDecider.decidePriority(getDriverQueryCost(driver));
    return priority;
  }

  private void calculateCost(LensDriver driver) throws LensException {
    if (getDriverQueryCost(driver) == null) {
      setDriverCost(driver, driver.estimate(this));
    }
  }
}
