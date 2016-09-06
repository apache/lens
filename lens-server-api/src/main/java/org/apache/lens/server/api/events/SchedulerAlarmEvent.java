/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lens.server.api.events;

import org.apache.lens.api.scheduler.SchedulerJobHandle;
import org.apache.lens.api.scheduler.SchedulerJobInstanceHandle;

import org.joda.time.DateTime;

import lombok.Data;

/**
 * This event is triggered by the AlarmService whenever a scheduled query needs to be scheduled.
 */
@Data
public class SchedulerAlarmEvent extends LensEvent {

  /**
   * jobHandle for which the alarm needs to be triggered.
   */
  private SchedulerJobHandle jobHandle;
  private DateTime nominalTime;
  private EventType type;
  private SchedulerJobInstanceHandle previousInstance;

  public SchedulerAlarmEvent(SchedulerJobHandle jobHandle, DateTime nominalTime, EventType type,
    SchedulerJobInstanceHandle previousInstance) {
    super(nominalTime.getMillis());
    this.jobHandle = jobHandle;
    this.nominalTime = nominalTime;
    this.type = type;
    this.previousInstance = previousInstance;
  }

  @Override
  public String getEventId() {
    return jobHandle.getHandleIdString();
  }

  @Override
  public String toString() {
    return "Job Handle : " + jobHandle + ", Nominal Time :" + nominalTime + ", type : " + type;
  }

  /**
   * Event type to know what kind of operations we want.
   */
  public static enum EventType {
    SCHEDULE, EXPIRE
  }
}
