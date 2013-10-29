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
  public QueryContext(com.inmobi.grill.api.QueryContext ctx) {
    this.userQuery = ctx.getUserQuery();
    this.submissionTime = ctx.getSubmissionTime();
    this.submittedUser = ctx.getSubmittedUser();
    this.queryHandle = ctx.getQueryHandle();
    this.priority = ctx.getPriority();
    this.isPersistent = ctx.isPersistent();
    this.driverQuery = ctx.getDriverQuery();
    this.selectedDriverClassName = ctx.getSelectedDriver() == null ? null :
      ctx.getSelectedDriver().getClass().getCanonicalName();
    this.status = ctx.getStatus();
    this.resultSetPath = ctx.getResultSetPath();
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