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
package org.apache.lens.server.api.driver;

import java.io.Serializable;

import org.apache.lens.api.query.QueryStatus;

import lombok.Data;

/**
 * The Class DriverQueryStatus.
 */
@Data
public class DriverQueryStatus implements Serializable {

  /**
   * The Constant serialVersionUID.
   */
  private static final long serialVersionUID = 1L;

  public boolean failed() {
    return state == DriverQueryState.FAILED;
  }


  /**
   * The Enum DriverQueryState.
   */
  public enum DriverQueryState {

    /**
     * The new.
     */
    NEW(0),

    /**
     * The initialized.
     */
    INITIALIZED(1),

    /**
     * The pending.
     */
    PENDING(2),

    /**
     * The running.
     */
    RUNNING(3),

    /**
     * The successful.
     */
    SUCCESSFUL(4),

    /**
     * The failed.
     */
    FAILED(4),

    /**
     * The canceled.
     */
    CANCELED(4),

    /**
     * The closed.
     */
    CLOSED(5);

    private int order;

    DriverQueryState(int order) {
      this.order = order;
    }

    public int getOrder() {
      return order;
    }
  }

  /**
   * The progress.
   */
  private double progress;

  /**
   * The state.
   */
  private DriverQueryState state;

  /**
   * The status message.
   */
  private String statusMessage;

  /**
   * The is result set available.
   */
  private boolean isResultSetAvailable;

  /**
   * The progress message.
   */
  private String progressMessage;

  /**
   * The error message.
   */
  private String errorMessage;

  /**
   * The driver start time.
   */
  private Long driverStartTime;

  /**
   * The driver finish time.
   */
  private Long driverFinishTime;

  {
    clear();
  }
  public void clear() {
    progress = 0.0f;
    state = DriverQueryState.NEW;
    statusMessage = null;
    isResultSetAvailable = false;
    progressMessage = null;
    errorMessage = null;
    driverStartTime = 0L;
    driverFinishTime = 0L;
  }
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
      qstate = QueryStatus.Status.FAILING;
      break;
    case CANCELED:
      qstate = QueryStatus.Status.CANCELED;
      break;
    case CLOSED:
      qstate = QueryStatus.Status.CLOSED;
      break;
    }

    return new QueryStatus(progress, null, qstate, statusMessage, isResultSetAvailable, progressMessage,
            errorMessage, null);
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

  public boolean isSuccessful() {
    return state.equals(DriverQueryState.SUCCESSFUL);
  }
  public boolean isCanceled() {
    return state.equals(DriverQueryState.CANCELED);
  }
  public boolean isFailed() {
    return state.equals(DriverQueryState.FAILED);
  }
  public boolean isRunning() {
    return state.equals(DriverQueryState.RUNNING);
  }
}
