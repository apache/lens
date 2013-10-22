package com.inmobi.grill.client.api;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "querystatus")
public class QueryStatus {
  public enum State {
    QUEUED,
    PREPARED,
    LAUNCHED,
    RUNNING,
    SUCCESSFUL,
    FAILED,
    CANCELED,
    CLOSED,
    UNKNOWN
  }
  public QueryStatus() {
    super();
  }
  @XmlElement
  private double progress;
  @XmlElement
  private String statusMessage;
  @XmlElement
  private State queryStatus;
  @XmlElement
  private boolean hasResultSet = false;

  public QueryStatus(double progress, State status, String statusMessage,
      boolean hasResultSet) {
    this.progress = progress;
    this.queryStatus = status;
    this.statusMessage = statusMessage;
    this.hasResultSet = hasResultSet;
  }
  
  /**
   * Get status of the query
   * 
   * @return {@link State}
   */
  public State getQueryStatus() {
    return queryStatus;
  }

  /**
   * Get progress of the query between 0 to 1.
   * 
   * @return progress
   */
  public double getProgress() {
    return progress;
  }

  /**
   * Get status message
   *  
   * @return string message
   */
  public String getStatusMessage() {
    return statusMessage;
  }

  /**
   * Whether query has result set
   * 
   * @return true if result set is available, false otherwise
   */
  public boolean hasResultSet() {
    return hasResultSet;
  }
  
  @Override
  public String toString() {
    return new StringBuilder(queryStatus.toString()).append(':')
        .append(progress).append(':')
        .append(hasResultSet).append(':').
        append(statusMessage).toString();
  }

  public boolean isFinished() {
    return queryStatus.equals(State.SUCCESSFUL) || queryStatus.equals(State.FAILED) ||
        queryStatus.equals(State.CANCELED);
  }
}
