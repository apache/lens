package com.inmobi.grill.api;

public class QueryStatus {
  public enum Status {
    PENDING,
    RUNNING,
    SUCCESSFUL,
    FAILED,
    CANCELED,
    CLOSED,
    UNKNOWN
  }
  
	private final double progress;
	private final String statusMessage;
	private final Status status;
	private boolean hasResultSet = false;

  public QueryStatus(double progress, Status status, String statusMessage,
      boolean hasResultSet) {
    this.progress = progress;
    this.status = status;
    this.statusMessage = statusMessage;
    this.hasResultSet = hasResultSet;
  }
  
  /**
   * Get status of the query
   * 
   * @return {@link Status}
   */
  public Status getStatus() {
    return status;
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
  	return new StringBuilder(status.toString()).append(':')
  			.append(progress).append(':')
  			.append(hasResultSet).append(':').
  			append(statusMessage).toString();
  }
}
