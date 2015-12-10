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
package org.apache.lens.api.query;

import org.apache.lens.api.LensSessionHandle;

import lombok.Data;
/**
 * POJO for an instance of SchedulerJob.
 */
@Data
public class SchedulerJobInstanceInfo {

  /**
   * @param id new id for the instance of scheduler job.
   * @return unique id for this instance of scheduler job.
   */
  private SchedulerJobInstanceHandle id;

  /**
   * @param jobId new id for the scheduler job.
   * @return id for the scheduler job to which this instance belongs.
   */
  private SchedulerJobHandle jobId;

  /**
   * @param sessionHandle new session handle.
   * @return session handle for this instance.
   */
  private LensSessionHandle sessionHandle;

  /**
   * @param startTime start time to be set for the instance.
   * @return actual start time of this instance.
   */
  private long startTime;

  /**
   * @param endTime end time to be set for the instance.
   * @return actual finish time of this instance.
   */
  private long endTime;

  /**
   * @param resultPath result path to be set.
   * @return result path of this instance.
   */
  private String resultPath;

  /**
   * @param query query to be set
   * @return query of this instance.
   */
  private String query;

  /**
   * @param status status to be set.
   * @return status of this instance.
   */
  private String status;

  /**
   * @param createdOn time to be set as created_on time for the instance.
   * @return created_on time of this instance.
   */
  private long createdOn;

}
