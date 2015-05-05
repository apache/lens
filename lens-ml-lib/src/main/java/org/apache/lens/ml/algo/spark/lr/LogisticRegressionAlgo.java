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
package org.apache.lens.ml.algo.spark.lr;

import java.util.Map;

import org.apache.lens.ml.algo.api.AlgoParam;
import org.apache.lens.ml.algo.api.Algorithm;
import org.apache.lens.ml.algo.spark.BaseSparkAlgo;
import org.apache.lens.ml.algo.spark.BaseSparkClassificationModel;
import org.apache.lens.server.api.error.LensException;

import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

/**
 * The Class LogisticRegressionAlgo.
 */
@Algorithm(name = "spark_logistic_regression", description = "Spark logistic regression algo")
public class LogisticRegressionAlgo extends BaseSparkAlgo {

  /** The iterations. */
  @AlgoParam(name = "iterations", help = "Max number of iterations", defaultValue = "100")
  private int iterations;

  /** The step size. */
  @AlgoParam(name = "stepSize", help = "Step size", defaultValue = "1.0d")
  private double stepSize;

  /** The min batch fraction. */
  @AlgoParam(name = "minBatchFraction", help = "Fraction for batched learning", defaultValue = "1.0d")
  private double minBatchFraction;

  /**
   * Instantiates a new logistic regression algo.
   *
   * @param name        the name
   * @param description the description
   */
  public LogisticRegressionAlgo(String name, String description) {
    super(name, description);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.spark.algos.BaseSparkAlgo#parseAlgoParams(java.util.Map)
   */
  @Override
  public void parseAlgoParams(Map<String, String> params) {
    iterations = getParamValue("iterations", 100);
    stepSize = getParamValue("stepSize", 1.0d);
    minBatchFraction = getParamValue("minBatchFraction", 1.0d);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.spark.algos.BaseSparkAlgo#trainInternal(java.lang.String, org.apache.spark.rdd.RDD)
   */
  @Override
  protected BaseSparkClassificationModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD)
    throws LensException {
    LogisticRegressionModel lrModel = LogisticRegressionWithSGD.train(trainingRDD, iterations, stepSize,
      minBatchFraction);
    return new LogitRegressionClassificationModel(modelId, lrModel);
  }
}
