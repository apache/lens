package com.inmobi.grill.api.query;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryStatus implements Serializable {
  private static final long serialVersionUID = 1L;

  public enum Status {
    NEW,
    QUEUED,
    LAUNCHED,
    RUNNING,
    SUCCESSFUL,
    FAILED,
    CANCELED,
    CLOSED
  }
  
  @XmlElement @Getter private double progress;
  @XmlElement @Getter private Status status;
  @XmlElement @Getter private String statusMessage;
  @XmlElement @Getter private boolean isResultSetAvailable = false;
  @Getter @Setter private String progressMessage;
  @Getter @Setter private String errorMessage;

  @Override
  public String toString() {
  	return new StringBuilder(status.toString()).append(':')
  			.append(progress).append(':')
  			.append(isResultSetAvailable).append(':').
  			append(statusMessage).toString();
  }

  public boolean isFinished() {
    return status.equals(Status.SUCCESSFUL) || status.equals(Status.FAILED) ||
        status.equals(Status.CANCELED);
  }

  public static boolean isValidTransition(Status oldState, Status newState) {
    switch (oldState) {
    case NEW:
      switch (newState) {
      case QUEUED:
        return true;
      }
      break;
    case QUEUED:
      switch (newState) {
      case LAUNCHED:
      case FAILED:
      case CANCELED:
        return true;
      }
      break;
    case LAUNCHED:
      switch (newState) {
      case LAUNCHED:
      case RUNNING:
      case CANCELED:
      case FAILED:
      case SUCCESSFUL:
        return true;
      }
      break;
    case RUNNING:
      switch (newState) {
      case RUNNING:
      case CANCELED:
      case FAILED:
      case SUCCESSFUL:
        return true;
      }
      break;
    case FAILED:
    case CANCELED:
    case SUCCESSFUL:
      if (Status.CLOSED.equals(newState)) {
        return true;
      }
    default:
      // fall-through
    }
    return false;
  }

  public boolean isValidateTransition(Status newState) {
    return isValidTransition(this.status, newState);
  }

}
