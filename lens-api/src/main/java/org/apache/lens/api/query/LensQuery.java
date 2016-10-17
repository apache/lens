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

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.Priority;
import org.apache.lens.api.ToYAMLString;

import lombok.*;

/**
 * This class provides all the details about the query represented by the {@link LensQuery#queryHandle}
 */
@XmlRootElement
/**
 * Instantiates a new lens query.
 *
 * @param queryHandle
 *          the query handle
 * @param userQuery
 *          the user query
 * @param submittedUser
 *          the submitted user
 * @param priority
 *          the priority
 * @param isPersistent
 *          the is persistent
 * @param selectedDriverName
 *          the selected driver class name
 * @param driverQuery
 *          the driver query
 * @param status
 *          the status
 * @param resultSetPath
 *          the result set path
 * @param driverOpHandle
 *          the driver op handle
 * @param queryConf
 *          the query conf
 * @param submissionTime
 *          the submission time
 * @param launchTime
 *          the launch time
 * @param driverStartTime
 *          the driver start time
 * @param driverFinishTime
 *          the driver finish time
 * @param finishTime
 *          the finish time
 * @param closedTime
 *          the closed time
 * @param queryName
 *          the query name
 */
@AllArgsConstructor
/**
 * Instantiates a new lens query.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode(of = "queryHandle", callSuper = false)
public class LensQuery extends ToYAMLString {

  /**
   * The query handle that represents this query uniquely
   */
  @XmlElement
  @Getter
  private QueryHandle queryHandle;

  /**
   * The the query submitted by the user
   */
  @XmlElement
  @Getter
  private String userQuery;

  /**
   * The user who submitted the query.
   */
  @XmlElement
  @Getter
  private String submittedUser;

  /**
   * The priority of the query.
   */
  @XmlElement
  @Getter
  private Priority priority;

  /**
   * Is true if query's result would be persisted by server.
   */
  @XmlElement
  @Getter
  private boolean isPersistent;

  /**
   * Name of the driver which executed the query (Example: hive/testDriver, jdbc/prodDriver etc)
   */
  @XmlElement
  @Getter
  private String selectedDriverName;

  /**
   * The driver query.
   * It is the final query (derived form user query) that was submitted by the driver for execution.
   */
  @XmlElement
  @Getter
  private String driverQuery;

  /**
   * The status of this query.
   * Note: {@link QueryStatus#getStatus()} method can be used to get the {@link QueryStatus.Status} enum that defines
   * the current state of the query. Also other utility methods are available to check the status of the query like
   * {@link QueryStatus#queued()}, {@link QueryStatus#successful()}, {@link QueryStatus#finished()},
   * {@link QueryStatus#failed()} and {@link QueryStatus#running()}
   */
  @XmlElement
  @Getter
  private QueryStatus status;

  /**
   * The result set path for this query if the query output was persisted by the server.
   */
  @XmlElement
  @Getter
  private String resultSetPath;

  /**
   * The operation handle associated with the driver, if any.
   */
  @XmlElement
  @Getter
  private String driverOpHandle;

  /**
   * The query conf that was used for executing this query.
   */
  @XmlElement
  @Getter
  private LensConf queryConf;

  /**
   * The submission time.
   */
  @XmlElement
  @Getter
  private long submissionTime;

  /**
   * The query launch time. This will be submission time + time spent by query waiting in the queue
   */
  @XmlElement
  @Getter
  private long launchTime;

  /**
   * The query execution start time on driver. This will >= launch time.
   */
  @XmlElement
  @Getter
  private long driverStartTime;

  /**
   * The the query execution end time on driver.
   */
  @XmlElement
  @Getter
  private long driverFinishTime;

  /**
   * The query finish time on server. This will be driver finish time + any extra time spent by server (like
   * formatting the result)
   */
  @XmlElement
  @Getter
  private long finishTime;

  /**
   * The the query close time when the query is purged by the server and no more operations are pending for it.
   * Note: not supported as of now.
   */
  @XmlElement
  @Getter
  private long closedTime;

  /**
   * The query name, if any.
   */
  @XmlElement
  @Getter
  private String queryName;

  @XmlElement
  @Getter
  private List<FailedAttempt> failedAttempts;

  /**
   * @return error code in case of query failures
   */
  public Integer getErrorCode() {
    return (this.status != null) ? this.status.getErrorCode() : null;
  }

  /**
   * @return error message in case of query failures
   */
  public String getErrorMessage() {
    return (this.status != null) ? this.status.getLensErrorTOErrorMsg() : null;
  }

  /**
   * @return the query handle string
   */
  public String getQueryHandleString() {
    return (this.queryHandle != null) ? this.queryHandle.getHandleIdString() : null;
  }
}
