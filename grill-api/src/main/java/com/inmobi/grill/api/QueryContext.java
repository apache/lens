package com.inmobi.grill.api;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

public class QueryContext implements Comparable<QueryContext> {

  public enum Priority {
    VERY_HIGH,
    HIGH,
    NORMAL,
    LOW,
    VERY_LOW
  }

  private final String userQuery;
  private final Date submissionTime;
  private final String submittedUser;
  private final Configuration conf;
  private QueryHandle queryHandle;
  private Priority priority;
  private boolean isPersistent;
  private GrillDriver selectedDriver;
  private String driverQuery;
  private QueryStatus status;
  private String resultSetPath;

  public QueryContext(String query, String user, Configuration conf) {
    this.userQuery = query;
    this.submissionTime = new Date();
    this.submittedUser = user;
    this.queryHandle = new QueryHandle(UUID.randomUUID());
    this.priority = Priority.NORMAL;
    this.conf = conf;
    this.isPersistent = conf.getBoolean(GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, true);
    this.driverQuery = query;
  }

  public QueryContext(PreparedQueryContext prepared, String user,
      Configuration conf) {
    this.userQuery = prepared.getUserQuery();
    this.submissionTime = new Date();
    this.submittedUser = user;
    this.queryHandle = new QueryHandle(UUID.randomUUID());
    this.priority = Priority.NORMAL;
    this.conf = mergeConf(prepared.getConf(), conf);
    this.isPersistent = this.conf.getBoolean(
        GrillConfConstants.GRILL_PERSISTENT_RESULT_SET, true);
    this.driverQuery = prepared.getDriverQuery();
    this.selectedDriver = prepared.getSelectedDriver();
  }

  @Deprecated
  public void setQueryHandle(QueryHandle handle) {
    this.queryHandle = handle;
  }

  private Configuration mergeConf(Configuration prepared,
      Configuration current) {
    Configuration conf = new Configuration(prepared);
    for (Map.Entry<String, String> entry : current) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return conf;
  }

  /**
   * @return the selectedDriver
   */
  public GrillDriver getSelectedDriver() {
    return selectedDriver;
  }

  /**
   * @param selectedDriver the selectedDriver to set
   */
  public void setSelectedDriver(GrillDriver selectedDriver) {
    this.selectedDriver = selectedDriver;
  }

  /**
   * @return the driverQuery
   */
  public String getDriverQuery() {
    return driverQuery;
  }

  /**
   * @param driverQuery the driverQuery to set
   */
  public void setDriverQuery(String driverQuery) {
    this.driverQuery = driverQuery;
  }

  /**
   * @return the status
   */
  public QueryStatus getStatus() {
    return status;
  }

  /**
   * @param status the status to set
   */
  public void setStatus(QueryStatus status) {
    this.status = status;
  }

  @Override
  public int compareTo(QueryContext other) {
    int pcomp = this.priority.compareTo(other.priority);
    if (pcomp == 0) {
      return this.submissionTime.compareTo(other.submissionTime);
    } else {
      return pcomp;
    }
  }

  /**
   * @return the queryHandle
   */
  public QueryHandle getQueryHandle() {
    return queryHandle;
  }

  /**
   * @return the submittedUser
   */
  public String getSubmittedUser() {
    return submittedUser;
  }

  /**
   * @return the userQuery
   */
  public String getUserQuery() {
    return userQuery;
  }

  /**
   * @return the conf
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * @param conf the conf to set
   */
  public void updateConf(Map<String,String> confoverlay) {
    for (Map.Entry<String,String> prop : confoverlay.entrySet()) {
      this.conf.set(prop.getKey(), prop.getValue());
    }
  }

  public Priority getPriority() {
    return priority;
  }

  public void setPriority(Priority priority) {
    this.priority = priority;
  }

  /**
   * @return the isPersistent
   */
  public boolean isPersistent() {
    return isPersistent;
  }

  /**
   * @param isPersistent the isPersistent to set
   */
  public void setPersistent(boolean isPersistent) {
    this.isPersistent = isPersistent;
  }

  public String getResultSetPersistentPath() {
    if (isPersistent) {
      return conf.get(GrillConfConstants.GRILL_RESULT_SET_PARENT_DIR);
    }
    return null;
  }

  /**
   * @return the resultSetPath
   */
  public String getResultSetPath() {
    return resultSetPath;
  }

  /**
   * @param resultSetPath the resultSetPath to set
   */
  public void setResultSetPath(String resultSetPath) {
    this.resultSetPath = resultSetPath;
  }

}