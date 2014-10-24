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

/**
 * Return a single double value as a prediction. This is useful in classifiers where the classifier returns a single
 * class label as a prediction.
 */
public abstract class ClassifierBaseModel extends MLModel<Double> {

  /**
   * Gets the feature vector.
   *
   * @param args
   *          the args
   * @return the feature vector
   */
  public final double[] getFeatureVector(Object[] args) {
    double[] features = new double[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i] instanceof Double) {
        features[i] = (Double) args[i];
      } else if (args[i] instanceof String) {
        features[i] = Double.parseDouble((String) args[i]);
      } else {
        features[i] = 0.0;
      }
    }
    return features;
  }
}
