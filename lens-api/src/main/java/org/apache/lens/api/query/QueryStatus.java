/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * 
 */
package org.apache.lens.api.query;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The Class QueryStatus.
 */
@XmlRootElement
/**
 * Instantiates a new query status.
 *
 * @param progress
 *          the progress
 * @param status
 *          the status
 * @param statusMessage
 *          the status message
 * @param isResultSetAvailable
 *          the is result set available
 * @param progressMessage
 *          the progress message
 * @param errorMessage
 *          the error message
 */
@AllArgsConstructor
/**
 * Instantiates a new query status.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryStatus implements Serializable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /**
   * The Enum Status.
   */
  public enum Status {

    /** The new. */
    NEW,

    /** The queued. */
    QUEUED,

    /** The launched. */
    LAUNCHED,

    /** The running. */
    RUNNING,

    /** The executed. */
    EXECUTED,

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
  @XmlElement
  @Getter
  private double progress;

  /** The status. */
  @XmlElement
  @Getter
  private Status status;

  /** The status message. */
  @XmlElement
  @Getter
  private String statusMessage;

  /** The is result set available. */
  @XmlElement
  @Getter
  private boolean isResultSetAvailable = false;

  /** The progress message. */
  @Getter
  @Setter
  private String progressMessage;

  /** The error message. */
  @Getter
  @Setter
  private String errorMessage;

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder(status.toString()).append(':').append(statusMessage);
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
    return status.equals(Status.SUCCESSFUL) || status.equals(Status.FAILED) || status.equals(Status.CANCELED);
  }

  /**
   * Checks if is valid transition.
   *
   * @param oldState
   *          the old state
   * @param newState
   *          the new state
   * @return true, if is valid transition
   */
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

  /**
   * Checks if is validate transition.
   *
   * @param newState
   *          the new state
   * @return true, if is validate transition
   */
  public boolean isValidateTransition(Status newState) {
    return isValidTransition(this.status, newState);
  }

}
