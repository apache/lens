package com.inmobi.grill.server.api.query;
/*
 * #%L
 * Grill Hive Driver
 * %%
 * Copyright (C) 2014 Inmobi
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

import com.inmobi.grill.server.api.query.QueryContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Class to represent the Finished grill query which is serialized to database.
 */
@EqualsAndHashCode
@ToString
public class FinishedGrillQuery {

  @Getter
  @Setter
  private String handle;

  @Getter
  @Setter
  private String userQuery;

  @Getter
  @Setter
  private String submitter;

  @Getter
  @Setter
  private long startTime;

  @Getter
  @Setter
  private long endTime;


  @Getter
  @Setter
  private String result;

  @Getter
  @Setter
  private String status;

  @Getter
  @Setter
  private String metadata;

  @Getter
  @Setter
  private int rows;

  @Getter
  @Setter
  private String errorMessage;

  @Getter
  @Setter
  private long driverStartTime;

  @Getter
  @Setter
  private long driverEndTime;

  @Getter
  @Setter
  private String metadataClass;

  @Getter
  @Setter
  private String queryName;

  public FinishedGrillQuery() {

  }

  public FinishedGrillQuery(QueryContext ctx) {
    this.handle = ctx.getQueryHandle().toString();
    this.userQuery = ctx.getUserQuery();
    this.submitter = ctx.getSubmittedUser();
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
  }

}
