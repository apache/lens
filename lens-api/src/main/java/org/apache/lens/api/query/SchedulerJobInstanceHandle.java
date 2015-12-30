/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lens.api.query;

import java.io.Serializable;
import java.util.UUID;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.StringUtils;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Handle for <code>SchedulerJobInstance</code>
 */
@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
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
  private UUID handleId;

  /**
   * From string.
   *
   * @param handle the handle
   * @return the <code>SchedulerJobInstance</code>'s handle
   */
  public static SchedulerJobInstanceHandle fromString(String handle) {
    return new SchedulerJobInstanceHandle(UUID.fromString(handle));
  }

  /**
   * Returns handle id as a string.
   * @return handleId as a string.
   */
  public String getHandleIdString() {
    if (handleId == null) {
      return StringUtils.EMPTY;
    }
    return handleId.toString();
  }

  /**
   * String representation of the SchedulerJobInstanceHandle.
   * @return the handleID as a string
   */
  @Override
  public String toString() {
    return getHandleIdString();
  }

}
