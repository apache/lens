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
package org.apache.lens.ml;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * Instantiates a new ML test report.
 */
@NoArgsConstructor
public class MLTestReport implements Serializable {

  /** The test table. */
  @Getter
  @Setter
  private String testTable;

  /** The output table. */
  @Getter
  @Setter
  private String outputTable;

  /** The output column. */
  @Getter
  @Setter
  private String outputColumn;

  /** The label column. */
  @Getter
  @Setter
  private String labelColumn;

  /** The feature columns. */
  @Getter
  @Setter
  private List<String> featureColumns;

  /** The algorithm. */
  @Getter
  @Setter
  private String algorithm;

  /** The model id. */
  @Getter
  @Setter
  private String modelID;

  /** The report id. */
  @Getter
  @Setter
  private String reportID;

  /** The query id. */
  @Getter
  @Setter
  private String queryID;

  /** The test output path. */
  @Getter
  @Setter
  private String testOutputPath;

  /** The prediction result column. */
  @Getter
  @Setter
  private String predictionResultColumn;

  /** The lens query id. */
  @Getter
  @Setter
  private String lensQueryID;
}
