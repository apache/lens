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
package org.apache.lens.ml.api;

import java.util.Date;

import org.apache.lens.api.LensSessionHandle;

/**
 * Interface MLProcess for Process which go through ML LifeCycle of Submitting, Polling and Completion.
 */

public interface MLProcess {

  String getId();

  void setId(String id);

  Date getStartTime();

  void setStartTime(Date time);

  Date getFinishTime();

  void setFinishTime(Date time);

  Status getStatus();

  void setStatus(Status status);

  LensSessionHandle getLensSessionHandle();

  void setLensSessionHandle(LensSessionHandle lensSessionHandle);
}
