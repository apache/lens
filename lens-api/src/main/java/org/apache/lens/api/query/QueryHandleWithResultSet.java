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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The Class QueryHandleWithResultSet.
 */
@XmlRootElement
/**
 * Instantiates a new query handle with result set.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryHandleWithResultSet extends QuerySubmitResult {

  /**
   * The query handle.
   */
  @XmlElement
  @Getter
  private QueryHandle queryHandle;

  /**
   * The result.
   */
  @Getter
  @Setter
  private QueryResult result;

  /**
   * The result metadata
   */
  @Getter
  @Setter
  private QueryResultSetMetadata resultMetadata;

  /**
   * The status.
   */
  @Getter
  @Setter
  private QueryStatus status;

  /**
   * Instantiates a new query handle with result set.
   *
   * @param handle the handle
   */
  public QueryHandleWithResultSet(QueryHandle handle) {
    this.queryHandle = handle;
  }
}
