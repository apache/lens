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

/**
 * All events(actions) which can happen on an instance of <code>SchedulerJob</code>.
 */
public enum SchedulerJobInstanceEvent {
  ON_PREPARE, // an instance is first considered by the scheduler.
  ON_TIME_OUT,
  ON_CONDITIONS_MET,
  ON_CONDITIONS_NOT_MET,
  ON_RUN,
  ON_SUCCESS,
  ON_FAILURE,
  ON_RERUN,
  ON_KILL
}
