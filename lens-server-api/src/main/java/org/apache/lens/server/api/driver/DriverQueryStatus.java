package org.apache.lens.server.api.driver;

import java.io.Serializable;

import org.apache.lens.api.query.QueryStatus;

import lombok.Getter;
import lombok.Setter;

/**
 * The Class DriverQueryStatus.
 */
public class DriverQueryStatus implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /**
   * The Enum DriverQueryState.
   */
  public enum DriverQueryState {

    /** The new. */
    NEW,

    /** The initialized. */
    INITIALIZED,

    /** The pending. */
    PENDING,

    /** The running. */
    RUNNING,

    /** The successful. */
    SUCCESSFUL,

    /** The failed. */
    FAILED,

    /** The canceled. */
    CANCELED,

    /** The closed. */
    CLOSED
  }

  /** The progress. */
  @Getter
  @Setter
  private double progress = 0.0f;

  /** The state. */
  @Getter
  @Setter
  private DriverQueryState state = DriverQueryState.NEW;

  /** The status message. */
  @Getter
  @Setter
  private String statusMessage;

  /** The is result set available. */
  @Getter
  @Setter
  private boolean isResultSetAvailable = false;

  /** The progress message. */
  @Getter
  @Setter
  private String progressMessage;

  /** The error message. */
  @Getter
  @Setter
  private String errorMessage;

  /** The driver start time. */
  @Getter
  @Setter
  private Long driverStartTime = 0L;

  /** The driver finish time. */
  @Getter
  @Setter
  private Long driverFinishTime = 0L;

  /**
   * To query status.
   *
   * @return the query status
   */
  public QueryStatus toQueryStatus() {
    QueryStatus.Status qstate = null;
    switch (state) {
    case NEW:
    case INITIALIZED:
    case PENDING:
      qstate = QueryStatus.Status.LAUNCHED;
      break;
    case RUNNING:
      qstate = QueryStatus.Status.RUNNING;
      break;
    case SUCCESSFUL:
      qstate = QueryStatus.Status.EXECUTED;
      break;
    case FAILED:
      qstate = QueryStatus.Status.FAILED;
      break;
    case CANCELED:
      qstate = QueryStatus.Status.CANCELED;
      break;
    case CLOSED:
      qstate = QueryStatus.Status.CLOSED;
      break;
    }

    return new QueryStatus(progress, qstate, statusMessage, isResultSetAvailable, progressMessage, errorMessage);
  }

  /**
   * Creates the query status.
   *
   * @param state
   *          the state
   * @param dstatus
   *          the dstatus
   * @return the query status
   */
  public static QueryStatus createQueryStatus(QueryStatus.Status state, DriverQueryStatus dstatus) {
    return new QueryStatus(dstatus.progress, state, dstatus.statusMessage, dstatus.isResultSetAvailable,
        dstatus.progressMessage, dstatus.errorMessage);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder(state.toString()).append(':').append(statusMessage);
    if (state.equals(DriverQueryState.RUNNING)) {
      str.append(" - Progress:").append(progress).append(":").append(progressMessage);
    }
    if (state.equals(DriverQueryState.SUCCESSFUL)) {
      if (isResultSetAvailable) {
        str.append(" - Result Available");
      } else {
        str.append(" - Result Not Available");
      }
    }
    if (state.equals(DriverQueryState.FAILED)) {
      str.append(" - Cause:").append(errorMessage);
    }
    return str.toString();
  }

  public boolean isFinished() {
    return state.equals(DriverQueryState.SUCCESSFUL) || state.equals(DriverQueryState.FAILED)
        || state.equals(DriverQueryState.CANCELED) || state.equals(DriverQueryState.CLOSED);
  }

}
