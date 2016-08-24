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
package org.apache.lens.server.error;

import org.apache.lens.server.api.LensErrorInfo;

public enum LensSchedulerErrorCode {
  CANT_SUBMIT_JOB(5001, 0),
  INVALID_EVENT_FOR_JOB(5002, 0),
  INVALID_EVENT_FOR_JOB_INSTANCE(5003, 0),
  CURRENT_USER_IS_NOT_SAME_AS_OWNER(5004, 0),
  JOB_IS_NOT_SCHEDULED(5005, 0),
  JOB_INSTANCE_IS_NOT_YET_RUN(5006, 0),
  CANT_UPDATE_RESOURCE_WITH_HANDLE(5007, 0),
  FAILED_ALARM_SERVICE_OPERATION(5008, 0);

  private final LensErrorInfo errorInfo;

  LensSchedulerErrorCode(final int code, final int weight) {
    this.errorInfo = new LensErrorInfo(code, weight, name());
  }

  public LensErrorInfo getLensErrorInfo() {
    return this.errorInfo;
  }
}
