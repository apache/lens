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
package org.apache.lens.ml.algo.spark.kmeans;

import java.util.List;
import java.util.Map;

import org.apache.lens.ml.algo.api.TrainedModel;
import org.apache.lens.ml.api.Feature;
import org.apache.lens.server.api.error.LensException;

import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * The Class KMeansClusteringModel.
 */
public class KMeansClusteringModel implements TrainedModel<Integer> {

  /**
   * The model.
   */
  private final KMeansModel kMeansModel;

  private List<Feature> featureList;

  /**
   * Instantiates a new k   means clustering model.
   *
   * @param model the model
   */
  public KMeansClusteringModel(List<Feature> featureList, KMeansModel model) {
    this.kMeansModel = model;
    this.featureList = featureList;
  }


  @Override
  public Integer predict(Map<String, String> featureVector) throws LensException {
    double[] featureArray = new double[featureList.size()];
    int i = 0;
    for (Feature feature : featureList) {
      String featureValue = featureVector.get(feature.getName());
      if (featureValue == null || featureValue.isEmpty()) {
        throw new LensException("Error while predicting: input featureVector doesn't contain all required features : "
          + "Feature Name: " + feature.getName());
      } else {
        featureArray[i++] = Double.parseDouble(featureValue);
      }
    }
    return kMeansModel.predict(Vectors.dense(featureArray));
  }
}
