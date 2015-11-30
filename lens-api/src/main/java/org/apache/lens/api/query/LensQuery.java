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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.Priority;

import lombok.*;

/**
 * The Class LensQuery.
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
@EqualsAndHashCode
public class LensQuery {

  /**
   * The query handle.
   */
  @XmlElement
  @Getter
  private QueryHandle queryHandle;

  /**
   * The user query.
   */
  @XmlElement
  @Getter
  private String userQuery;

  /**
   * The submitted user.
   */
  @XmlElement
  @Getter
  private String submittedUser;

  /**
   * The priority.
   */
  @XmlElement
  @Getter
  private Priority priority;

  /**
   * The is persistent.
   */
  @XmlElement
  @Getter
  private boolean isPersistent;

  /**
   * The selected driver class name.
   */
  @XmlElement
  @Getter
  private String selectedDriverName;

  /**
   * The driver query.
   */
  @XmlElement
  @Getter
  private String driverQuery;

  /**
   * The status.
   */
  @XmlElement
  @Getter
  private QueryStatus status;

  /**
   * The result set path.
   */
  @XmlElement
  @Getter
  private String resultSetPath;

  /**
   * The driver op handle.
   */
  @XmlElement
  @Getter
  private String driverOpHandle;

  /**
   * The query conf.
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
   * The launch time.
   */
  @XmlElement
  @Getter
  private long launchTime;

  /**
   * The driver start time.
   */
  @XmlElement
  @Getter
  private long driverStartTime;

  /**
   * The driver finish time.
   */
  @XmlElement
  @Getter
  private long driverFinishTime;

  /**
   * The finish time.
   */
  @XmlElement
  @Getter
  private long finishTime;

  /**
   * The closed time.
   */
  @XmlElement
  @Getter
  private long closedTime;

  /**
   * The query name.
   */
  @XmlElement
  @Getter
  private String queryName;

  public Integer getErrorCode() {
    return (this.status != null) ? this.status.getErrorCode() : null;
  }

  public String getErrorMessage() {
    return (this.status != null) ? this.status.getLensErrorTOErrorMsg() : null;
  }

  public String getQueryHandleString() {
    return (this.queryHandle != null) ? this.queryHandle.getHandleIdString() : null;
  }

  public boolean queued() {
    return this.status.queued();
  }
}
