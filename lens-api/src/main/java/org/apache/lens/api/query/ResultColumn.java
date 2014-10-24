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
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * The Class ResultColumn.
 */
@XmlRootElement
/**
 * Instantiates a new result column.
 *
 * @param name
 *          the name
 * @param type
 *          the type
 */
@AllArgsConstructor
/**
 * Instantiates a new result column.
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ResultColumn {

  /** The name. */
  @XmlElement
  @Getter
  private String name;

  /** The type. */
  @XmlElement
  @Getter
  private ResultColumnType type;

  /**
   * Instantiates a new result column.
   *
   * @param name
   *          the name
   * @param type
   *          the type
   */
  public ResultColumn(String name, String type) {
    this(name, ResultColumnType.valueOf(type.toUpperCase()));
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new StringBuilder(name).append(':').append(type).toString();
  }
}
