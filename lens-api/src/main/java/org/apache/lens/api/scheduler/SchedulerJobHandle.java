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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lens.api.scheduler;

import java.io.Serializable;
import java.util.UUID;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.lens.api.UUIDAdapter;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

/**
 * Handle for <code>SchedulerJob</code>.
 */
@XmlRootElement
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class SchedulerJobHandle implements Serializable {

  /**
   * The Constant serialVersionUID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * The handle id.
   */
  @XmlElement
  @Getter
  @XmlJavaTypeAdapter(UUIDAdapter.class)
  @NonNull
  private UUID handleId;

  /**
   * Default constructor
   */
  public SchedulerJobHandle() {
    this(UUID.randomUUID());
  }

  /**
   * From string.
   *
   * @param handle the handle for scheduler job
   * @return the handle for
   */
  public static SchedulerJobHandle fromString(@NonNull String handle) {
    return new SchedulerJobHandle(UUID.fromString(handle));
  }

  public String getHandleIdString() {
    return handleId.toString();
  }

  /**
   * String representation of the SchedulerJobHandle.
   *
   * @return string representation of the handleId
   */
  @Override
  public String toString() {
    return getHandleIdString();
  }

}
