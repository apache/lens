package com.inmobi.grill.client.api;

import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.inmobi.grill.api.QueryContext.Priority;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

@XmlRootElement
public class QueryContext {

  @XmlElement
  private QueryHandle queryHandle;
  @XmlElement
  private String userQuery;
  @XmlElement
  private Date submissionTime;
  @XmlElement
  private String submittedUser;
  @XmlElement
  private Priority priority;
  @XmlElement
  private boolean isPersistent;
  @XmlElement
  private String selectedDriverClassName;
  @XmlElement
  private String driverQuery;
  @XmlElement
  private QueryStatus status;
  @XmlElement
  private String resultSetPath;

  public QueryContext() {
    // only for JAXB
  }
  public QueryContext(QueryHandle queryHandle, String query, String user,
      Date submissionTime, Priority priority, boolean isPersistent,
      String selectedDriverClassName, String driverQuery, QueryStatus status,
      String resultSetPath) {
    this.userQuery = query;
    this.submissionTime = submissionTime;
    this.submittedUser = user;
    this.queryHandle = queryHandle;
    this.priority = priority;
    this.isPersistent = isPersistent;
    this.driverQuery = driverQuery;
    this.selectedDriverClassName = selectedDriverClassName;
    this.status = status;
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

  public Priority getPriority() {
    return priority;
  }

  /**
   * @return the isPersistent
   */

  public boolean isPersistent() {
    return isPersistent;
  }

  /**
   * @return the resultSetPath
   */
  public String getResultSetPath() {
    return resultSetPath;
  }

  /**
   * @return the driverQuery
   */
  public String getDriverQuery() {
    return driverQuery;
  }

  /**
   * @return the status
   */
  public QueryStatus getStatus() {
    return status;
  }
  /**
   * @return the submissionTime
   */
  public Date getSubmissionTime() {
    return submissionTime;
  }
  /**
   * @return the selectedDriverClassName
   */
  public String getSelectedDriverClassName() {
    return selectedDriverClassName;
  }
}