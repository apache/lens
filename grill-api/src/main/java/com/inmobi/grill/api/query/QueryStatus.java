package com.inmobi.grill.api.query;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryStatus {
  public enum Status {
    QUEUED,
    LAUNCHED,
    RUNNING,
    SUCCESSFUL,
    FAILED,
    CANCELED,
    CLOSED,
    UNKNOWN
  }
  
  @XmlElement @Getter private double progress;
  @XmlElement @Getter private Status status;
  @XmlElement @Getter private String statusMessage;
  @XmlElement @Getter private boolean isResultSetAvailable = false;
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
}
