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

import lombok.*;

/**
 * Handle for <code>SchedulerJobInstance</code>
 */
@XmlRootElement
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class SchedulerJobInstanceHandle implements Serializable {

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

  public SchedulerJobInstanceHandle() {
    this(UUID.randomUUID());
  }

  /**
   * From string.
   *
   * @param handle the handle
   * @return the <code>SchedulerJobInstance</code>'s handle
   */
  public static SchedulerJobInstanceHandle fromString(@NonNull String handle) {
    return new SchedulerJobInstanceHandle(UUID.fromString(handle));
  }

  /**
   * Returns handle id as a string.
   *
   * @return handleId as a string.
   */
  public String getHandleIdString() {
    return handleId.toString();
  }

  /**
   * String representation of the SchedulerJobInstanceHandle.
   *
   * @return the handleID as a string
   */
  @Override
  public String toString() {
    return getHandleIdString();
  }

}
