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
package org.apache.lens.cube.error;

public enum LensCubeErrorCode {

  SYNTAX_ERROR(3001),
  COLUMN_UNAVAILABLE_IN_TIME_RANGE(3002),
  FIELDS_CANNOT_BE_QUERIED_TOGETHER(3003),
  NO_REF_COL_AVAILABLE(3004),
  MORE_THAN_ONE_CUBE(3005),
  NEITHER_CUBE_NOR_DIMENSION(3006),
  NO_TIMERANGE_FILTER(3007),
  NOT_A_TIMED_DIMENSION(3008),
  WRONG_TIME_RANGE_FORMAT(3009),
  NULL_DATE_VALUE(3010),
  INVALID_TIME_UNIT(3011),
  ALL_COLUMNS_NOT_SUPPORTED(3012),
  AMBIGOUS_DIM_COLUMN(3013),
  AMBIGOUS_CUBE_COLUMN(3014),
  COLUMN_NOT_FOUND(3015),
  NOT_A_CUBE_COLUMN(3016),
  NO_CANDIDATE_FACT_AVAILABLE(3017),
  NO_JOIN_CONDITION_AVAIABLE(3018),
  NO_STORAGE_TABLE_AVAIABLE(3019),
  NO_DEFAULT_AGGREGATE(3020),
  INVALID_TIME_RANGE(3021),
  FROM_AFTER_TO(3022),
  NO_JOIN_PATH(3023),
  JOIN_TARGET_NOT_CUBE_TABLE(3024),
  NO_FACT_HAS_COLUMN(3025),
  NO_CANDIDATE_DIM_STORAGE_TABLES(3026),
  NO_DIM_HAS_COLUMN(3027),
  NO_CANDIDATE_DIM_AVAILABLE(3028),
  CANNOT_USE_TIMERANGE_WRITER(3029),
  EXPRESSION_NOT_IN_ANY_FACT(3030);

  public int getValue() {
    return this.errorCode;
  }

  LensCubeErrorCode(final int code) {
    this.errorCode = code;
  }

  private final int errorCode;
}
