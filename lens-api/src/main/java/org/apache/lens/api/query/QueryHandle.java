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
/*
 *
 */
package org.apache.lens.api.query;

import java.io.Serializable;
import java.util.UUID;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.lens.api.UUIDAdapter;

import org.apache.commons.lang.StringUtils;

import lombok.*;

/**
 * The Class QueryHandle.
 */
@XmlRootElement
/**
 * Instantiates a new query handle.
 *
 * @param handleId
 *          the handle id
 */
@AllArgsConstructor
/**
 * Instantiates a new query handle.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
/*
 * (non-Javadoc)
 *
 * @see java.lang.Object#hashCode()
 */
@EqualsAndHashCode(callSuper = false)
public class QueryHandle extends QuerySubmitResult implements Serializable {

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
  private UUID handleId;

  /**
   * From string.
   *
   * @param handle the handle
   * @return the query handle
   */
  public static QueryHandle fromString(String handle) {
    return new QueryHandle(UUID.fromString(handle));
  }

  public String getHandleIdString() {
    if (handleId == null) {
      return StringUtils.EMPTY;
    }
    return handleId.toString();
  }
}
