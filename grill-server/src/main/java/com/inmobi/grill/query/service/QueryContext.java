package com.inmobi.grill.query.service;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.query.service.QueryExcecutionServiceImpl.Priority;

class QueryContext implements Comparable<QueryContext> {
  private final String userQuery;
  private final Date submissionTime;
  private final String submittedUser;
  private final Configuration conf;
  private final QueryHandle queryHandle;
  private GrillDriver selectedDriver;
  private String driverQuery;
  private QueryStatus status;
  private Priority priority;

  QueryContext(String query, String user, Configuration conf) {
    this.userQuery = query;
    this.submissionTime = new Date();
    this.submittedUser = user;
    this.queryHandle = new QueryHandle(UUID.randomUUID());
    this.priority = Priority.NORMAL;
    this.conf = conf;
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
   * @return the queryHandle
   */
  public QueryHandle getQueryHandle() {
    return queryHandle;
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

}