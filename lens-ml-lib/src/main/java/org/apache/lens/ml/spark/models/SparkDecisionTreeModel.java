/*
 * #%L
 * Lens ML Lib
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package org.apache.lens.ml.spark.models;

import org.apache.lens.ml.spark.DoubleValueMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.rdd.RDD;

/**
 * This class is created because the Spark decision tree model doesn't extend ClassificationModel
 */
public class SparkDecisionTreeModel implements ClassificationModel {

  private final DecisionTreeModel model;

  public SparkDecisionTreeModel(DecisionTreeModel model) {
    this.model = model;
  }

  @Override
  public RDD<Object> predict(RDD<Vector> testData) {
    return model.predict(testData);
  }

  @Override
  public double predict(Vector testData) {
    return model.predict(testData);
  }

  @Override
  public JavaRDD<Double> predict(JavaRDD<Vector> testData) {
    return model.predict(testData.rdd()).toJavaRDD().map(new DoubleValueMapper());
  }
}
