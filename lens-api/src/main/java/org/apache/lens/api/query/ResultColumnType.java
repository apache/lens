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

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * The Enum ResultColumnType.
 */
@XmlRootElement
@XmlEnum
public enum ResultColumnType {

  /**
   * The boolean.
   */
  BOOLEAN, // boolean
  /**
   * The tinyint.
   */
  TINYINT, // short
  /**
   * The smallint.
   */
  SMALLINT, // short
  /**
   * The int.
   */
  INT, // int
  /**
   * The bigint.
   */
  BIGINT, // long
  /**
   * The float.
   */
  FLOAT, // float
  /**
   * The double.
   */
  DOUBLE, // double
  /**
   * The string.
   */
  STRING, // string
  /**
   * The timestamp.
   */
  TIMESTAMP, // Date
  /**
   * The binary.
   */
  BINARY, // byte[]
  /**
   * The array.
   */
  ARRAY, // List
  /**
   * The map.
   */
  MAP, // Map
  /**
   * The struct.
   */
  STRUCT,

  /**
   * The uniontype.
   */
  UNIONTYPE,

  /**
   * The user defined.
   */
  USER_DEFINED,

  /**
   * The decimal.
   */
  DECIMAL,

  /**
   * The null.
   */
  NULL,

  /**
   * The date.
   */
  DATE,

  /**
   * The varchar.
   */
  VARCHAR,

  /**
   * The char.
   */
  CHAR;
}
