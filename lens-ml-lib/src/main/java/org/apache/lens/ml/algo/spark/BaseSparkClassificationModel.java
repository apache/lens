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
package org.apache.lens.ml.algo.spark;

import java.util.List;
import java.util.Map;

import org.apache.lens.ml.algo.lib.ClassifierBaseModel;
import org.apache.lens.ml.api.Feature;
import org.apache.lens.server.api.error.LensException;

import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * The class BaseSparkClassificationModel
 *
 * @param <MODEL>
 */
public class BaseSparkClassificationModel<MODEL extends ClassificationModel> extends ClassifierBaseModel {


  /**
   * The spark model.
   */
  private final MODEL sparkModel;
  private List<Feature> featureList;

  /**
   * initializes BaseSparkClassificationModel
   *
   * @param featureList
   * @param model
   */
  public BaseSparkClassificationModel(List<Feature> featureList, MODEL model) {
    this.sparkModel = model;
    this.featureList = featureList;
  }


  @Override
  public Double predict(Map<String, String> featureVector) throws LensException {
    String[] featureArray = new String[featureList.size()];
    int i = 0;
    for (Feature feature : featureList) {
      String featureValue = featureVector.get(feature.getName());
      if (featureValue == null || featureValue.isEmpty()) {
        throw new LensException("Error while predicting: input featureVector doesn't contain all required features : "
          + "Feature Name: " + feature.getName());
      } else {
        featureArray[i++] = featureVector.get(feature.getName());
      }
    }
    return sparkModel.predict(Vectors.dense(getFeatureVector(featureArray)));
  }

}
