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
package org.apache.lens.api.ml;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * The Class ModelMetadata.
 */
@XmlRootElement
/**
 * Instantiates a new model metadata.
 *
 * @param modelID
 *          the model id
 * @param table
 *          the table
 * @param algorithm
 *          the algorithm
 * @param params
 *          the params
 * @param createdAt
 *          the created at
 * @param modelPath
 *          the model path
 * @param labelColumn
 *          the label column
 * @param features
 *          the features
 */
@AllArgsConstructor
/**
 * Instantiates a new model metadata.
 */
@NoArgsConstructor
public class ModelMetadata {

  /** The model id. */
  @XmlElement
  @Getter
  private String modelID;

  /** The table. */
  @XmlElement
  @Getter
  private String table;

  /** The algorithm. */
  @XmlElement
  @Getter
  private String algorithm;

  /** The params. */
  @XmlElement
  @Getter
  private String params;

  /** The created at. */
  @XmlElement
  @Getter
  private String createdAt;

  /** The model path. */
  @XmlElement
  @Getter
  private String modelPath;

  /** The label column. */
  @XmlElement
  @Getter
  private String labelColumn;

  /** The features. */
  @XmlElement
  @Getter
  private String features;

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append("Algorithm: ").append(algorithm).append('\n');
    builder.append("Model ID: ").append(modelID).append('\n');
    builder.append("Training table: ").append(table).append('\n');
    builder.append("Features: ").append(features).append('\n');
    builder.append("Labelled Column: ").append(labelColumn).append('\n');
    builder.append("Training params: ").append(params).append('\n');
    builder.append("Created on: ").append(createdAt).append('\n');
    builder.append("Model saved at: ").append(modelPath).append('\n');
    return builder.toString();
  }
}
