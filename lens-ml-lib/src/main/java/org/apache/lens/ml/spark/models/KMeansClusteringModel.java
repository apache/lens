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

import org.apache.lens.ml.MLModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * The Class KMeansClusteringModel.
 */
public class KMeansClusteringModel extends MLModel<Integer> {

  /** The model. */
  private final KMeansModel model;

  /** The model id. */
  private final String modelId;

  /**
   * Instantiates a new k means clustering model.
   *
   * @param modelId
   *          the model id
   * @param model
   *          the model
   */
  public KMeansClusteringModel(String modelId, KMeansModel model) {
    this.model = model;
    this.modelId = modelId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLModel#predict(java.lang.Object[])
   */
  @Override
  public Integer predict(Object... args) {
    // Convert the params to array of double
    double[] arr = new double[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i] != null) {
        arr[i] = (Double) args[i];
      } else {
        arr[i] = 0d;
      }
    }

    return model.predict(Vectors.dense(arr));
  }
}
