package org.apache.lens.server.api.query;

/*
 * #%L
 * Lens API for server and extensions
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


import lombok.Getter;
import lombok.Setter;

public class QueryContext implements Comparable<QueryContext>, Serializable {

  private static final long serialVersionUID = 1L;

  @Getter @Setter private QueryHandle queryHandle;
  @Getter final private String userQuery;
  @Getter final private String submittedUser;  // Logged in user.
  transient @Getter @Setter private Configuration conf;
  @Getter private LensConf qconf;
  @Getter private Priority priority;
  @Getter final private boolean isPersistent;
  @Getter final private boolean isDriverPersistent;
  transient @Getter @Setter private LensDriver selectedDriver;
  @Getter @Setter private String driverQuery;
  @Getter private QueryStatus status;
  @Getter @Setter private String resultSetPath;
  @Getter @Setter private String hdfsoutPath;
  @Getter final private long submissionTime;
  @Getter @Setter private long launchTime;
  @Getter @Setter private long endTime;
  @Getter @Setter private long closedTime;
  @Getter @Setter private String lensSessionIdentifier;
  @Getter @Setter private String driverOpHandle;
  @Getter final DriverQueryStatus driverStatus;
  transient @Getter @Setter private QueryOutputFormatter queryOutputFormatter;
  @Getter @Setter private boolean finishedQueryPersisted = false;
  @Getter @Setter private String queryName;

  public QueryContext(String query, String user, Configuration conf) {
    this(query, user, new LensConf(), conf, query, null, new Date().getTime());
  }

  public QueryContext(String query, String user, LensConf qconf, Configuration conf) {
    this(query, user, qconf, conf, query, null, new Date().getTime());
  }

  public QueryContext(PreparedQueryContext prepared, String user, 
      Configuration conf) {
    this(prepared, user, new LensConf(), conf);
  }

  public QueryContext(PreparedQueryContext prepared, String user, LensConf qconf,
      Configuration conf) {
    this(prepared.getUserQuery(), user, qconf, mergeConf(prepared.getConf(), conf),
        prepared.getDriverQuery(), prepared.getSelectedDriver(), new Date().getTime());
  }

  public QueryContext(String query, String user, Configuration conf, long submissionTime) {
    this(query, user, new LensConf(), conf, query, null, submissionTime);
  }

  public QueryContext(String userQuery, String user, LensConf qconf,
      Configuration conf, String driverQuery, LensDriver selectedDriver, long submissionTime) {
    this.submissionTime = submissionTime;
    this.queryHandle = new QueryHandle(UUID.randomUUID());
    this.status = new QueryStatus(0.0f, Status.NEW, "Query just got created", false, null, null);
    this.priority = Priority.NORMAL;
    this.conf = conf;
    this.isPersistent = conf.getBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_SET,
        LensConfConstants.DEFAULT_PERSISTENT_RESULT_SET);
    this.isDriverPersistent = conf.getBoolean(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER,
        LensConfConstants.DEFAULT_DRIVER_PERSISTENT_RESULT_SET);
    this.userQuery = userQuery;
    this.submittedUser = user;
    this.driverQuery = driverQuery;
    this.selectedDriver = selectedDriver;
    this.qconf = qconf;
    this.driverStatus = new DriverQueryStatus();
  }

  private static Configuration mergeConf(Configuration prepared,
      Configuration current) {
    Configuration conf = new Configuration(false);
    for (Map.Entry<String, String> entry : prepared) {
      conf.set(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : current) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  @Override
  public int compareTo(QueryContext other) {
    int pcomp = this.priority.compareTo(other.priority);
    if (pcomp == 0) {
      return (int)(this.submissionTime - other.submissionTime);
    } else {
      return pcomp;
    }
  }

  /**
   * @param confoverlay the conf to set
   */
  public void updateConf(Map<String,String> confoverlay) {
    qconf.getProperties().putAll(confoverlay);
    for (Map.Entry<String,String> prop : confoverlay.entrySet()) {
      this.conf.set(prop.getKey(), prop.getValue());
    }
  }

  public String getResultSetParentDir() {
    return conf.get(LensConfConstants.RESULT_SET_PARENT_DIR,
        LensConfConstants.RESULT_SET_PARENT_DIR_DEFAULT);
  }

  public Path getHDFSResultDir() {
    return new Path(new Path (getResultSetParentDir(), conf.get(
        LensConfConstants.QUERY_HDFS_OUTPUT_PATH,
        LensConfConstants.DEFAULT_HDFS_OUTPUT_PATH)),
        queryHandle.toString());
  }

  public LensQuery toLensQuery() {
    return new LensQuery(queryHandle, userQuery,
        submittedUser, priority, isPersistent,
        selectedDriver != null ? selectedDriver.getClass().getCanonicalName() : null,
        driverQuery, status, resultSetPath, driverOpHandle, qconf, submissionTime,
        launchTime, driverStatus.getDriverStartTime(),
        driverStatus.getDriverFinishTime(), endTime, closedTime, queryName);
  }

  public boolean isResultAvailableInDriver() {
    return isDriverPersistent()|| driverStatus.isResultSetAvailable();
  }

  /*
  Introduced for Recovering finished query.
   */
  public void setStatusSkippingTransitionTest(QueryStatus newStatus) throws LensException {
    this.status = newStatus;
  }

  public synchronized void setStatus(QueryStatus newStatus) throws LensException {
    if (!this.status.isValidateTransition(newStatus.getStatus())) {
      throw new LensException("Invalid state transition:[" + this.status.getStatus() + "->" + newStatus.getStatus() + "]");
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
    return conf.get(LensConfConstants.QUERY_OUTPUT_CHARSET_ENCODING,
        LensConfConstants.DEFAULT_OUTPUT_CHARSET_ENCODING);
  }

  public String getOuptutFileExtn() {
    return conf.get(LensConfConstants.QUERY_OUTPUT_FILE_EXTN,
        LensConfConstants.DEFAULT_OUTPUT_FILE_EXTN);
  }

  public boolean getCompressOutput() {
    return conf.getBoolean(LensConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION,
        LensConfConstants.DEFAULT_OUTPUT_ENABLE_COMPRESSION);
  }

  public long getMaxResultSplitRows() {
    return conf.getLong(LensConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS,
        LensConfConstants.DEFAULT_RESULT_SPLIT_MULTIPLE_MAX_ROWS);
  }

  public boolean splitResultIntoMultipleFiles() {
    return conf.getBoolean(LensConfConstants.RESULT_SPLIT_INTO_MULTIPLE,
        LensConfConstants.DEFAULT_RESULT_SPLIT_INTO_MULTIPLE);
  }

  public String getClusterUser() {
    return conf.get(LensConfConstants.SESSION_CLUSTER_USER, submittedUser);
  }
}
