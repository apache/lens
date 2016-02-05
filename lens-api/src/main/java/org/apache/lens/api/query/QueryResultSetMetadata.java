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

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.lens.api.ToYAMLString;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * The Class QueryResultSetMetadata.
 */
@XmlRootElement
/**
 * Instantiates a new query result set metadata.
 *
 * @param columns
 *          the columns
 */
@AllArgsConstructor
/**
 * Instantiates a new query result set metadata.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QueryResultSetMetadata extends ToYAMLString {

  /**
   * The columns.
   */
  @XmlElementWrapper
  @XmlElement
  @Getter
  private List<ResultColumn> columns;
}
