/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.api.scheduler;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.LensSessionHandle;
import org.apache.lens.api.query.QueryHandle;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@EqualsAndHashCode
@NoArgsConstructor
@XmlRootElement
public class SchedulerJobInstanceRun {

  /**
   * @param handle : Instance handle
   * @return the handle
   */
  private SchedulerJobInstanceHandle handle;

  /**
   * @param runId : run number of the instance run. Highest run number will represent the latest run.
   * @return the runId
   */
  private int runId;

  /**
   * @param sessionHandle new session handle.
   * @return session handle for this instance run.
   */
  private LensSessionHandle sessionHandle;
  /**
   * @param startTime start time to be set for the instance run.
   * @return actual start time of this instance run .
   */
  private long startTime;

  /**
   * @param endTime end time to be set for the instance run.
   * @return actual finish time of this instance run.
   */
  private long endTime;

  /**
   * @param resultPath result path to be set.
   * @return result path of this instance run.
   */
  private String resultPath;

  /**
   * @param queryHandle query to be set
   * @return queryHandle of this instance run.
   */
  private QueryHandle queryHandle;

  /**
   * @param instanceState state to be set.
   * @return status of this instance.
   */
  private SchedulerJobInstanceState instanceState;

}
