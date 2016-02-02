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

import org.apache.lens.server.api.LensErrorInfo;

public enum LensCubeErrorCode {
  // Error codes less than 3100 are errors encountered while submitting a query
  // Error codes same for drivers
  SYNTAX_ERROR(3001, 0),
  FIELDS_CANNOT_BE_QUERIED_TOGETHER(3002, 0),
  MORE_THAN_ONE_CUBE(3003, 0),
  NEITHER_CUBE_NOR_DIMENSION(3004, 0),
  NO_TIMERANGE_FILTER(3005, 0),
  NOT_A_TIMED_DIMENSION(3006, 0),
  WRONG_TIME_RANGE_FORMAT(3007, 0),
  NULL_DATE_VALUE(3008, 0),
  INVALID_TIME_UNIT(3009, 0),
  ALL_COLUMNS_NOT_SUPPORTED(3010, 0),
  AMBIGOUS_DIM_COLUMN(3011, 0),
  AMBIGOUS_CUBE_COLUMN(3012, 0),
  NOT_A_CUBE_COLUMN(3013, 0),
  INVALID_TIME_RANGE(3014, 0),
  FROM_AFTER_TO(3015, 0),
  JOIN_TARGET_NOT_CUBE_TABLE(3016, 0),
  // Error codes different for drivers
  CANNOT_USE_TIMERANGE_WRITER(3017, 100),
  NO_DEFAULT_AGGREGATE(3018, 200),
  EXPRESSION_NOT_IN_ANY_FACT(3019, 300),
  NO_JOIN_CONDITION_AVAILABLE(3020, 400),
  NO_JOIN_PATH(3021, 500),
  COLUMN_UNAVAILABLE_IN_TIME_RANGE(3022, 600),
  NO_DIM_HAS_COLUMN(3023, 700),
  NO_FACT_HAS_COLUMN(3024, 800),
  NO_REF_COL_AVAILABLE(3025, 900),
  COLUMN_NOT_FOUND(3026, 1000),
  NO_CANDIDATE_DIM_AVAILABLE(3027, 1100),
  NO_CANDIDATE_FACT_AVAILABLE(3028, 1200),
  NO_CANDIDATE_DIM_STORAGE_TABLES(3029, 1300),
  NO_STORAGE_TABLE_AVAILABLE(3030, 1400),
  STORAGE_UNION_DISABLED(3031, 1500),
  COULD_NOT_PARSE_EXPRESSION(3032, 1500),
  QUERIED_TABLE_NOT_FOUND(3033, 0),
  // Error codes greater than 3100 are errors while doing a metastore operation.
  ERROR_IN_ENTITY_DEFINITION(3101, 100),
  TIMELINE_ABSENT(3102, 100),
  EXPRESSION_NOT_PARSABLE(3103, 1500);

  public LensErrorInfo getLensErrorInfo() {
    return this.errorInfo;
  }

  LensCubeErrorCode(final int code, final int weight) {
    this.errorInfo = new LensErrorInfo(code, weight, name());
  }

  private final LensErrorInfo errorInfo;

}
