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
 * "AS IS"  BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
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
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.lens.api.ToYAMLString;
import org.apache.lens.api.result.LensErrorTO;

import lombok.*;

/**
 * The Class QueryStatus.
 */
@XmlRootElement
/**
 * Instantiates a new query status.
 *
 * @param progress
 *          the progress
 * @param queueNumber
 *          the queue number
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
public class QueryStatus extends ToYAMLString implements Serializable {

  /**
   * The Constant serialVersionUID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * The Enum Status.
   */
  @XmlType
  @XmlEnum
  public enum Status {

    /**
     * The new.
     */
    NEW,

    /**
     * The queued.
     */
    QUEUED,

    /**
     * The launched.
     */
    LAUNCHED,

    /**
     * The running.
     */
    RUNNING,

    /**
     * The executed.
     */
    EXECUTED,

    /**
     * The successful.
     */
    SUCCESSFUL,

    /**
     * The failed.
     */
    FAILED,

    /**
     * The canceled.
     */
    CANCELED,

    /**
     * The closed.
     */
    CLOSED
  }

  /**
   * The progress.
   */
  @XmlElement
  @Getter
  private double progress;

  /**
   * Queue number of a query when it is in waiting state.
   */
  @Getter
  @Setter
  private Integer queueNumber;

  /**
   * The status.
   */
  @XmlElement
  @Getter
  private Status status;

  /**
   * The status message.
   */
  @XmlElement
  @Getter
  private String statusMessage;

  /**
   * The is result set available.
   */
  @XmlElement
  @Getter
  private boolean isResultSetAvailable = false;

  /**
   * The progress message.
   */
  @Getter
  @Setter
  private String progressMessage;

  /**
   * The error message.
   */
  @Getter
  @Setter
  private String errorMessage;

  @XmlElement
  private LensErrorTO lensErrorTO;

  public boolean finished() {
    return status.equals(Status.SUCCESSFUL) || status.equals(Status.FAILED) || status.equals(Status.CANCELED);
  }

  public boolean successful() {
    return status.equals(Status.SUCCESSFUL);
  }


  public boolean launched() {
    return status.equals(Status.LAUNCHED);
  }

  public boolean running() {
    return status.equals(Status.RUNNING);
  }

  public boolean queued() {
    return status.equals(Status.QUEUED);
  }

  public boolean failed() {
    return status.equals(Status.FAILED);
  }


  /**
   * Checks if is valid transition.
   *
   * @param oldState the old state
   * @param newState the new state
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
      case EXECUTED:
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
   * @param newState the new state
   * @return true, if is validate transition
   */
  public boolean isValidTransition(final Status newState) {
    return isValidTransition(this.status, newState);
  }

  public Integer getErrorCode() {
    return (this.lensErrorTO != null) ? this.lensErrorTO.getCode() : null;
  }

  public String getLensErrorTOErrorMsg() {
    return (this.lensErrorTO != null) ? this.lensErrorTO.getMessage() : null;
  }
}
