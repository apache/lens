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
package org.apache.lens.ml.spark.trainers;

import org.apache.lens.api.LensException;
import org.apache.lens.ml.spark.models.BaseSparkClassificationModel;
import org.apache.lens.ml.spark.models.SVMClassificationModel;
import org.apache.lens.ml.Algorithm;
import org.apache.lens.ml.TrainerParam;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.util.Map;

/**
 * The Class SVMTrainer.
 */
@Algorithm(name = "spark_svm", description = "Spark SVML classifier trainer")
public class SVMTrainer extends BaseSparkTrainer {

  /** The min batch fraction. */
  @TrainerParam(name = "minBatchFraction", help = "Fraction for batched learning", defaultValue = "1.0d")
  private double minBatchFraction;

  /** The reg param. */
  @TrainerParam(name = "regParam", help = "regularization parameter for gradient descent", defaultValue = "1.0d")
  private double regParam;

  /** The step size. */
  @TrainerParam(name = "stepSize", help = "Iteration step size", defaultValue = "1.0d")
  private double stepSize;

  /** The iterations. */
  @TrainerParam(name = "iterations", help = "Number of iterations", defaultValue = "100")
  private int iterations;

  /**
   * Instantiates a new SVM trainer.
   *
   * @param name
   *          the name
   * @param description
   *          the description
   */
  public SVMTrainer(String name, String description) {
    super(name, description);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.spark.trainers.BaseSparkTrainer#parseTrainerParams(java.util.Map)
   */
  @Override
  public void parseTrainerParams(Map<String, String> params) {
    minBatchFraction = getParamValue("minBatchFraction", 1.0);
    regParam = getParamValue("regParam", 1.0);
    stepSize = getParamValue("stepSize", 1.0);
    iterations = getParamValue("iterations", 100);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.spark.trainers.BaseSparkTrainer#trainInternal(java.lang.String, org.apache.spark.rdd.RDD)
   */
  @Override
  protected BaseSparkClassificationModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD)
      throws LensException {
    SVMModel svmModel = SVMWithSGD.train(trainingRDD, iterations, stepSize, regParam, minBatchFraction);
    return new SVMClassificationModel(modelId, svmModel);
  }
}
