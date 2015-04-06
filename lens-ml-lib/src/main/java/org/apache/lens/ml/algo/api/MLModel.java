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
package org.apache.lens.ml.algo.api;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Instantiates a new ML model.
 */
@NoArgsConstructor
@ToString
public abstract class MLModel<PREDICTION> implements Serializable {

  /** The id. */
  @Getter
  @Setter
  private String id;

  /** The created at. */
  @Getter
  @Setter
  private Date createdAt;

  /** The algo name. */
  @Getter
  @Setter
  private String algoName;

  /** The table. */
  @Getter
  @Setter
  private String table;

  /** The params. */
  @Getter
  @Setter
  private List<String> params;

  /** The label column. */
  @Getter
  @Setter
  private String labelColumn;

  /** The feature columns. */
  @Getter
  @Setter
  private List<String> featureColumns;

  /**
   * Predict.
   *
   * @param args the args
   * @return the prediction
   */
  public abstract PREDICTION predict(Object... args);
}
