package com.inmobi.grill.server.api.query;

/*
 * #%L
 * Grill API for server and extensions
 * %%
 * Copyright (C) 2014 Inmobi
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

import com.inmobi.grill.api.GrillConf;
import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.Priority;
import com.inmobi.grill.api.query.GrillQuery;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.api.query.QueryStatus.Status;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.DriverQueryStatus;
import com.inmobi.grill.server.api.driver.GrillDriver;

import lombok.Getter;
import lombok.Setter;

public class QueryContext implements Comparable<QueryContext>, Serializable {

  private static final long serialVersionUID = 1L;

  @Getter @Setter private QueryHandle queryHandle;
  @Getter final private String userQuery;
  @Getter final private String submittedUser;  // Logged in user.
  transient @Getter @Setter private Configuration conf;
  @Getter private GrillConf qconf;
  @Getter private Priority priority;
  @Getter final private boolean isPersistent;
  @Getter final private boolean isDriverPersistent;
  transient @Getter @Setter private GrillDriver selectedDriver;
  @Getter @Setter private String driverQuery;
  @Getter private QueryStatus status;
  @Getter @Setter private String resultSetPath;
  @Getter @Setter private String hdfsoutPath;
  @Getter final private long submissionTime;
  @Getter @Setter private long launchTime;
  @Getter @Setter private long endTime;
  @Getter @Setter private long closedTime;
  @Getter @Setter private String grillSessionIdentifier;
  @Getter @Setter private String driverOpHandle;
  @Getter final DriverQueryStatus driverStatus;
  transient @Getter @Setter private QueryOutputFormatter queryOutputFormatter;
  @Getter @Setter private boolean finishedQueryPersisted = false;
  @Getter @Setter private String queryName;

  public QueryContext(String query, String user, Configuration conf) {
    this(query, user, new GrillConf(), conf, query, null);
  }

  public QueryContext(String query, String user, GrillConf qconf, Configuration conf) {
    this(query, user, qconf, conf, query, null);
  }

  public QueryContext(PreparedQueryContext prepared, String user, 
      Configuration conf) {
    this(prepared, user, new GrillConf(), conf);
  }

  public QueryContext(PreparedQueryContext prepared, String user, GrillConf qconf,
      Configuration conf) {
    this(prepared.getUserQuery(), user, qconf, mergeConf(prepared.getConf(), conf),
        prepared.getDriverQuery(), prepared.getSelectedDriver());
  }

  private QueryContext(String userQuery, String user, GrillConf qconf,
      Configuration conf,
      String driverQuery, GrillDriver selectedDriver) {
    this.submissionTime = new Date().getTime();
    this.queryHandle = new QueryHandle(UUID.randomUUID());
    this.status = new QueryStatus(0.0f, Status.NEW, "Query just got created", false, null, null);
    this.priority = Priority.NORMAL;
    this.conf = conf;
    this.isPersistent = conf.getBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET,
        GrillConfConstants.DEFAULT_PERSISTENT_RESULT_SET);
    this.isDriverPersistent = conf.getBoolean(GrillConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER,
        GrillConfConstants.DEFAULT_DRIVER_PERSISTENT_RESULT_SET);
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
    return conf.get(GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR,
        GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR_DEFAULT);
  }

  public Path getHDFSResultDir() {
    return new Path(new Path (getResultSetParentDir(), conf.get(
        GrillConfConstants.QUERY_HDFS_OUTPUT_PATH,
        GrillConfConstants.DEFAULT_HDFS_OUTPUT_PATH)),
        queryHandle.toString());
  }

  public GrillQuery toGrillQuery() {
    return new GrillQuery(queryHandle, userQuery,
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
  public void setStatusSkippingTransitionTest(QueryStatus newStatus) throws GrillException {
    this.status = newStatus;
  }

  public synchronized void setStatus(QueryStatus newStatus) throws GrillException {
    if (!this.status.isValidateTransition(newStatus.getStatus())) {
      throw new GrillException("Invalid state transition:[" + this.status.getStatus() + "->" + newStatus.getStatus() + "]");
    }
    this.status = newStatus;
  }
  
  public String getResultHeader() {
    return getConf().get(GrillConfConstants.QUERY_OUTPUT_HEADER);
  }

  public String getResultFooter() {
    return getConf().get(GrillConfConstants.QUERY_OUTPUT_FOOTER);
  }

  public String getResultEncoding() {
    return conf.get(GrillConfConstants.QUERY_OUTPUT_CHARSET_ENCODING,
        GrillConfConstants.DEFAULT_OUTPUT_CHARSET_ENCODING);
  }

  public String getOuptutFileExtn() {
    return conf.get(GrillConfConstants.QUERY_OUTPUT_FILE_EXTN,
        GrillConfConstants.DEFAULT_OUTPUT_FILE_EXTN);
  }

  public boolean getCompressOutput() {
    return conf.getBoolean(GrillConfConstants.QUERY_OUTPUT_ENABLE_COMPRESSION,
        GrillConfConstants.DEFAULT_OUTPUT_ENABLE_COMPRESSION);
  }

  public long getMaxResultSplitRows() {
    return conf.getLong(GrillConfConstants.RESULT_SPLIT_MULTIPLE_MAX_ROWS,
        GrillConfConstants.DEFAULT_RESULT_SPLIT_MULTIPLE_MAX_ROWS);
  }

  public boolean splitResultIntoMultipleFiles() {
    return conf.getBoolean(GrillConfConstants.RESULT_SPLIT_INTO_MULTIPLE,
        GrillConfConstants.DEFAULT_RESULT_SPLIT_INTO_MULTIPLE);
  }

  public String getClusterUser() {
    return conf.get(GrillConfConstants.GRILL_SESSION_CLUSTER_USER);
  }
}
