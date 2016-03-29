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
package org.apache.lens.server.api.query;

import java.util.Collection;
import java.util.Iterator;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.Priority;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.server.api.driver.LensDriver;
import org.apache.lens.server.api.query.collect.WaitingQueriesSelectionPolicy;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableSet;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Class to represent the Finished lens query which is serialized to database.
 */

/*
 * (non-Javadoc)
 *
 * @see java.lang.Object#hashCode()
 */
@EqualsAndHashCode(exclude = "selectedDriver")
/*
 * (non-Javadoc)
 *
 * @see java.lang.Object#toString()
 */
@ToString
public class FinishedLensQuery {

  /**
   * The handle.
   */
  @Getter
  @Setter
  private String handle;

  /**
   * The user query.
   */
  @Getter
  @Setter
  private String userQuery;

  /**
   * The submitter.
   */
  @Getter
  @Setter
  private String submitter;

  /**
   * The submission time.
   */
  @Getter
  @Setter
  private long submissionTime;

  /**
   * The start time.
   */
  @Getter
  @Setter
  private long startTime;

  /**
   * The end time.
   */
  @Getter
  @Setter
  private long endTime;

  /**
   * The result.
   */
  @Getter
  @Setter
  private String result;

  /**
   * The status.
   */
  @Getter
  @Setter
  private String status;

  /**
   * The metadata.
   */
  @Getter
  @Setter
  private String metadata;

  /**
   * The rows.
   */
  @Getter
  @Setter
  private Integer rows;

  /**
   * The file size.
   */
  @Getter
  @Setter
  private Long fileSize;

  /**
   * The error message.
   */
  @Getter
  @Setter
  private String errorMessage;

  /**
   * The driver start time.
   */
  @Getter
  @Setter
  private long driverStartTime;

  /**
   * The driver end time.
   */
  @Getter
  @Setter
  private long driverEndTime;

  /**
   * The query name.
   */
  @Getter
  @Setter
  private String queryName;

  /**
   * The selected driver's fully qualified name.
   */
  @Getter
  @Setter
  private String driverName;

  @Getter
  private LensDriver selectedDriver;

  @Getter
  @Setter
  private String priority;

  /**
   * Instantiates a new finished lens query.
   */
  public FinishedLensQuery() {

  }

  /**
   * Instantiates a new finished lens query.
   *
   * @param ctx the ctx
   */
  public FinishedLensQuery(QueryContext ctx) {
    this.handle = ctx.getQueryHandle().toString();
    this.userQuery = ctx.getUserQuery();
    this.submitter = ctx.getSubmittedUser();
    this.submissionTime = ctx.getSubmissionTime();
    this.startTime = ctx.getLaunchTime();
    this.endTime = ctx.getEndTime();
    this.result = ctx.getResultSetPath();
    this.status = ctx.getStatus().getStatus().name();
    this.errorMessage = ctx.getStatus().getErrorMessage();
    this.driverStartTime = ctx.getDriverStatus().getDriverStartTime();
    this.driverEndTime = ctx.getDriverStatus().getDriverFinishTime();
    if (ctx.getQueryName() != null) {
      this.queryName = ctx.getQueryName().toLowerCase();
    }
    this.selectedDriver = ctx.getSelectedDriver();
    if (null != ctx.getSelectedDriver()) {
      this.driverName = ctx.getSelectedDriver().getFullyQualifiedName();
    }
    //Priority can be null in case no driver is fit to execute a query and launch fails.
    if (null != ctx.getPriority()) {
      this.priority = ctx.getPriority().toString();
    }
  }

  public QueryContext toQueryContext(Configuration conf, Collection<LensDriver> drivers) {

    if (null == selectedDriver && null != driverName) {
      selectedDriver = getDriverFromName(drivers);
    }

    QueryContext qctx =
      new QueryContext(userQuery, submitter, new LensConf(), conf, drivers, selectedDriver, submissionTime,
        false);

    qctx.setQueryHandle(QueryHandle.fromString(handle));
    qctx.setLaunchTime(this.startTime);
    qctx.setEndTime(getEndTime());
    qctx.setStatusSkippingTransitionTest(new QueryStatus(0.0, null, QueryStatus.Status.valueOf(getStatus()),
        getErrorMessage() == null ? "" : getErrorMessage(), getResult() != null, null, null, null));
    qctx.getDriverStatus().setDriverStartTime(getDriverStartTime());
    qctx.getDriverStatus().setDriverFinishTime(getDriverEndTime());
    qctx.setResultSetPath(getResult());
    qctx.setQueryName(getQueryName());
    if (getPriority() != null) {
      qctx.setPriority(Priority.valueOf(getPriority()));
    }
    return qctx;
  }

  private LensDriver getDriverFromName(Collection<LensDriver> drivers) {
    Iterator<LensDriver> iterator = drivers.iterator();
    while (iterator.hasNext()) {
      LensDriver driver = iterator.next();
      if (driverName.equals(driver.getFullyQualifiedName())) {
        return driver;
      }
    }
    return null;
  }

  public ImmutableSet<WaitingQueriesSelectionPolicy> getDriverSelectionPolicies() {
    return this.selectedDriver.getWaitingQuerySelectionPolicies();
  }
}
