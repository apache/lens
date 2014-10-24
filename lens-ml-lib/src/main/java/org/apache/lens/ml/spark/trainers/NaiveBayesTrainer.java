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
import org.apache.lens.ml.spark.models.NaiveBayesClassificationModel;
import org.apache.lens.ml.Algorithm;
import org.apache.lens.ml.TrainerParam;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.util.Map;

/**
 * The Class NaiveBayesTrainer.
 */
@Algorithm(name = "spark_naive_bayes", description = "Spark Naive Bayes classifier trainer")
public class NaiveBayesTrainer extends BaseSparkTrainer {

  /** The lambda. */
  @TrainerParam(name = "lambda", help = "Lambda parameter for naive bayes learner", defaultValue = "1.0d")
  private double lambda = 1.0;

  /**
   * Instantiates a new naive bayes trainer.
   *
   * @param name
   *          the name
   * @param description
   *          the description
   */
  public NaiveBayesTrainer(String name, String description) {
    super(name, description);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.spark.trainers.BaseSparkTrainer#parseTrainerParams(java.util.Map)
   */
  @Override
  public void parseTrainerParams(Map<String, String> params) {
    lambda = getParamValue("lambda", 1.0d);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.spark.trainers.BaseSparkTrainer#trainInternal(java.lang.String, org.apache.spark.rdd.RDD)
   */
  @Override
  protected BaseSparkClassificationModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD)
      throws LensException {
    return new NaiveBayesClassificationModel(modelId, NaiveBayes.train(trainingRDD, lambda));
  }
}
