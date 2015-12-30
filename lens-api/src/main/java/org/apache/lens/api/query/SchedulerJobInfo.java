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

import org.apache.lens.api.scheduler.XJob;

import lombok.Data;
/**
 * POJO to represent the <code>job</code> table in the database.
 */
@Data
public class SchedulerJobInfo {

  /**
   * ID of the job.
   * Each job has a unique id which can be used to query it.
   *
   * @param id new value for ID.
   * @return ID for the current job.
   */
  private SchedulerJobHandle id;

  /**
   * Definition of the job scheduled.
   */
  private XJob job;

  /**
   * @param userName userName to be set.
   * @return name of the user who scheduled this job.
   */
  private String userName;

  /**
   * @param status status of this job.
   * @return current status of this job
   */
  private String status;

  /**
   * @param createdOn time to be set as createdOn.
   * @return time when this job was submitted.
   */
  private long createdOn;

  /**
   * @param modifiedOn time to be set as modifiedOn time for this job.
   * @return last modified time for this job
   */
  private long modifiedOn;

}
