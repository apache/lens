package org.apache.lens.api.query;

/*
 * #%L
 * Lens API
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
    EXECUTED,
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
  	StringBuilder str = new StringBuilder(status.toString()).append(':').
    append(statusMessage);
  	if (status.equals(Status.RUNNING)) {
  	  str.append(" - Progress:").append(progress).append(":").append(progressMessage);
  	}
  	if (status.equals(Status.SUCCESSFUL)) {
  	  if (isResultSetAvailable) {
  	    str.append(" - Result Available");
  	  } else {
        str.append(" - Result Not Available");
  	  }
  	}
    if (status.equals(Status.FAILED)) {
      str.append(" - Cause:").append(errorMessage);
    }
    return str.toString();
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
      case EXECUTED:
        return true;
      }
      break;
    case RUNNING:
      switch (newState) {
      case RUNNING:
      case CANCELED:
      case FAILED:
      case EXECUTED:
        return true;
      }
      break;
    case EXECUTED:
      switch (newState) {
        case SUCCESSFUL:
        case FAILED:
        case CANCELED:
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
