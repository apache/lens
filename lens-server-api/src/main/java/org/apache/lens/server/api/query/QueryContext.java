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
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.api.Priority;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.api.query.QueryStatus.Status;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.DriverQueryStatus;
import org.apache.lens.server.api.driver.LensDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import lombok.Getter;
import lombok.Setter;

/**
 * The Class QueryContext.
 */
public class QueryContext extends AbstractQueryContext implements Comparable<QueryContext> {

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
  private final boolean isDriverPersistent;

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
  private String hdfsoutPath;

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
  private transient QueryOutputFormatter queryOutputFormatter;

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
   * Instantiates a new query context.
   *
   * @param query the query
   * @param user  the user
   * @param conf  the conf
   */
  public QueryContext(String query, String user, Configuration conf, Collection<LensDriver> drivers) {
    this(query, user, new LensConf(), conf, drivers, new Date().getTime());
  }

  /**
   * Instantiates a new query context.
   *
   * @param query the query
   * @param user  the user
   * @param conf  the conf
   */
  public QueryContext(String query, String user, Configuration conf, Collection<LensDriver> drivers, long
    submissionTime) {
    this(query, user, new LensConf(), conf, drivers, submissionTime);
  }

  /**
   * Instantiates a new query context.
   *
   * @param query   the query
   * @param user    the user
   * @param qconf   the qconf
   * @param conf    the conf
   * @param drivers Collection of drivers
   */
  public QueryContext(String query, String user, LensConf qconf, Configuration conf, Collection<LensDriver> drivers) {
    this(query, user, qconf, conf, drivers, null, new Date().getTime());
  }

  /**
   * Instantiates a new query context.
   *
   * @param query   the query
   * @param user    the user
   * @param qconf   the qconf
   * @param conf    the conf
   * @param drivers Collection of drivers
   */
  public QueryContext(String query, String user, LensConf qconf, Configuration conf, Collection<LensDriver> drivers,
    Long submissionTime) {
    this(query, user, qconf, conf, drivers, null, submissionTime);
  }

  /**
   * Instantiates a new query context.
   *
   * @param prepared the prepared
   * @param user     the user
   * @param conf     the conf
   */
  public QueryContext(PreparedQueryContext prepared, String user, Configuration conf) {
    this(prepared, user, new LensConf(), conf);
  }

  /**
   * Instantiates a new query context.
   *
   * @param prepared the prepared
   * @param user     the user
   * @param qconf    the qconf
   * @param conf     the conf
   */
  public QueryContext(PreparedQueryContext prepared, String user, LensConf qconf, Configuration conf) {
    this(prepared.getUserQuery(), user, qconf, mergeConf(prepared.getConf(), conf), prepared.getDriverContext()
      .getDriverQueryContextMap().keySet(),
      prepared.getDriverContext()
        .getSelectedDriver(), new Date().getTime());
    setDriverContext(prepared.getDriverContext());
    setSelectedDriverQuery(prepared.getSelectedDriverQuery());
  }

  /**
   * Instantiates a new query context.
   *
   * @param userQuery      the user query
   * @param user           the user
   * @param qconf          the qconf
   * @param conf           the conf
   * @param selectedDriver the selected driver
   * @param submissionTime the submission time
   */
  public QueryContext(String userQuery, String user, LensConf qconf, Configuration conf,
    Collection<LensDriver> drivers, LensDriver selectedDriver, long submissionTime) {
    super(userQuery, user, qconf, conf, drivers);
    this.submissionTime = submissionTime;
    this.queryHandle = new QueryHandle(UUID.randomUUID());
    this.status = new QueryStatus(0.0f, Status.NEW, "Query just got created", false, null, null);
    this.priority = Priority.NORMAL;
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

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(QueryContext other) {
    int pcomp = this.priority.compareTo(other.priority);
    if (pcomp == 0) {
      return (int) (this.submissionTime - other.submissionTime);
    } else {
      return pcomp;
    }
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
      getSelectedDriver() != null ? getSelectedDriver().getClass()
        .getCanonicalName() : null,
      getSelectedDriverQuery(),
      status,
      resultSetPath, driverOpHandle, lensConf, submissionTime, launchTime, driverStatus.getDriverStartTime(),
      driverStatus.getDriverFinishTime(), endTime, closedTime, queryName);
  }

  public boolean isResultAvailableInDriver() {
    return isDriverPersistent() || driverStatus.isResultSetAvailable();
  }

  /*
   * Introduced for Recovering finished query.
   */
  public void setStatusSkippingTransitionTest(QueryStatus newStatus) throws LensException {
    this.status = newStatus;
  }

  public synchronized void setStatus(QueryStatus newStatus) throws LensException {
    if (!this.status.isValidateTransition(newStatus.getStatus())) {
      throw new LensException("Invalid state transition:[" + this.status.getStatus() + "->" + newStatus.getStatus()
        + "]");
    }
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
}
