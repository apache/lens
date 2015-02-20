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
package org.apache.lens.ml.spark.algos;

import java.util.Map;

import org.apache.lens.api.LensException;
import org.apache.lens.ml.AlgoParam;
import org.apache.lens.ml.Algorithm;
import org.apache.lens.ml.spark.models.BaseSparkClassificationModel;
import org.apache.lens.ml.spark.models.SVMClassificationModel;

import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

/**
 * The Class SVMAlgo.
 */
@Algorithm(name = "spark_svm", description = "Spark SVML classifier algo")
public class SVMAlgo extends BaseSparkAlgo {

  /** The min batch fraction. */
  @AlgoParam(name = "minBatchFraction", help = "Fraction for batched learning", defaultValue = "1.0d")
  private double minBatchFraction;

  /** The reg param. */
  @AlgoParam(name = "regParam", help = "regularization parameter for gradient descent", defaultValue = "1.0d")
  private double regParam;

  /** The step size. */
  @AlgoParam(name = "stepSize", help = "Iteration step size", defaultValue = "1.0d")
  private double stepSize;

  /** The iterations. */
  @AlgoParam(name = "iterations", help = "Number of iterations", defaultValue = "100")
  private int iterations;

  /**
   * Instantiates a new SVM algo.
   *
   * @param name        the name
   * @param description the description
   */
  public SVMAlgo(String name, String description) {
    super(name, description);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.spark.algos.BaseSparkAlgo#parseAlgoParams(java.util.Map)
   */
  @Override
  public void parseAlgoParams(Map<String, String> params) {
    minBatchFraction = getParamValue("minBatchFraction", 1.0);
    regParam = getParamValue("regParam", 1.0);
    stepSize = getParamValue("stepSize", 1.0);
    iterations = getParamValue("iterations", 100);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.spark.algos.BaseSparkAlgo#trainInternal(java.lang.String, org.apache.spark.rdd.RDD)
   */
  @Override
  protected BaseSparkClassificationModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD)
    throws LensException {
    SVMModel svmModel = SVMWithSGD.train(trainingRDD, iterations, stepSize, regParam, minBatchFraction);
    return new SVMClassificationModel(modelId, svmModel);
  }
}
