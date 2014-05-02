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
  @Getter @Setter private Long driverStartTime;
  @Getter @Setter private Long driverFinishTime;
  
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

  public boolean isFinished() {
    return state.equals(DriverQueryState.SUCCESSFUL) || state.equals(DriverQueryState.FAILED) ||
        state.equals(DriverQueryState.CANCELED);
  }

}
