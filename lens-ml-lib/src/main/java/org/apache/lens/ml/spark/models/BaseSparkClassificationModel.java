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
package org.apache.lens.ml.spark.models;

import org.apache.lens.ml.ClassifierBaseModel;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * The Class BaseSparkClassificationModel.
 *
 * @param <MODEL>
 *          the generic type
 */
public class BaseSparkClassificationModel<MODEL extends ClassificationModel> extends ClassifierBaseModel {

  /** The model id. */
  private final String modelId;

  /** The spark model. */
  private final MODEL sparkModel;

  /**
   * Instantiates a new base spark classification model.
   *
   * @param modelId
   *          the model id
   * @param model
   *          the model
   */
  public BaseSparkClassificationModel(String modelId, MODEL model) {
    this.modelId = modelId;
    this.sparkModel = model;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLModel#predict(java.lang.Object[])
   */
  @Override
  public Double predict(Object... args) {
    return sparkModel.predict(Vectors.dense(getFeatureVector(args)));
  }

  @Override
  public String getId() {
    return modelId;
  }

}
