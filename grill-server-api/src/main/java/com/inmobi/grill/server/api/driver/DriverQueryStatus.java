package com.inmobi.grill.server.api.driver;

import java.io.Serializable;

import com.inmobi.grill.api.query.QueryStatus;

import lombok.Getter;
import lombok.Setter;

public class DriverQueryStatus implements Serializable {
  
  private static final long serialVersionUID = 1L;

  public enum DriverQueryState {
    NEW,
    INITIALIZED,
    PENDING,
    RUNNING,
    SUCCESSFUL,
    FAILED,
    CANCELED,
    CLOSED
  }
  
  @Getter @Setter private double progress = 0.0f;
  @Getter @Setter private DriverQueryState state = DriverQueryState.NEW;
  @Getter @Setter private String statusMessage;
  @Getter @Setter private boolean isResultSetAvailable = false;
  @Getter @Setter private String progressMessage;
  @Getter @Setter private String errorMessage;
  @Getter @Setter private Long driverStartTime = 0L;
  @Getter @Setter private Long driverFinishTime = 0L;
  
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
      qstate = QueryStatus.Status.SUCCESSFUL;
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

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder(state.toString()).append(':').
    append(statusMessage);
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
    return state.equals(DriverQueryState.SUCCESSFUL) || state.equals(DriverQueryState.FAILED) ||
        state.equals(DriverQueryState.CANCELED) || state.equals(DriverQueryState.CLOSED);
  }

}
