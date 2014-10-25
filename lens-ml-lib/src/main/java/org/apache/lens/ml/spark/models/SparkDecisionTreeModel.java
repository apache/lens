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

import org.apache.lens.ml.spark.DoubleValueMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.rdd.RDD;

/**
 * This class is created because the Spark decision tree model doesn't extend ClassificationModel.
 */
public class SparkDecisionTreeModel implements ClassificationModel {

  /** The model. */
  private final DecisionTreeModel model;

  /**
   * Instantiates a new spark decision tree model.
   *
   * @param model
   *          the model
   */
  public SparkDecisionTreeModel(DecisionTreeModel model) {
    this.model = model;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.spark.mllib.classification.ClassificationModel#predict(org.apache.spark.rdd.RDD)
   */
  @Override
  public RDD<Object> predict(RDD<Vector> testData) {
    return model.predict(testData);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.spark.mllib.classification.ClassificationModel#predict(org.apache.spark.mllib.linalg.Vector)
   */
  @Override
  public double predict(Vector testData) {
    return model.predict(testData);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.spark.mllib.classification.ClassificationModel#predict(org.apache.spark.api.java.JavaRDD)
   */
  @Override
  public JavaRDD<Double> predict(JavaRDD<Vector> testData) {
    return model.predict(testData.rdd()).toJavaRDD().map(new DoubleValueMapper());
  }
}
