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
import org.apache.lens.ml.spark.models.DecisionTreeClassificationModel;
import org.apache.lens.ml.spark.models.SparkDecisionTreeModel;
import org.apache.lens.ml.Algorithm;
import org.apache.lens.ml.TrainerParam;
import org.apache.lens.ml.spark.trainers.BaseSparkTrainer;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree$;
import org.apache.spark.mllib.tree.configuration.Algo$;
import org.apache.spark.mllib.tree.impurity.Entropy$;
import org.apache.spark.mllib.tree.impurity.Gini$;
import org.apache.spark.mllib.tree.impurity.Impurity;
import org.apache.spark.mllib.tree.impurity.Variance$;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.rdd.RDD;
import scala.Enumeration;

import java.util.Map;

/**
 * The Class DecisionTreeTrainer.
 */
@Algorithm(name = "spark_decision_tree", description = "Spark Decision Tree classifier trainer")
public class DecisionTreeTrainer extends BaseSparkTrainer {

  /** The algo. */
  @TrainerParam(name = "algo", help = "Decision tree algorithm. Allowed values are 'classification' and 'regression'")
  private Enumeration.Value algo;

  /** The decision tree impurity. */
  @TrainerParam(name = "impurity", help = "Impurity measure used by the decision tree. "
      + "Allowed values are 'gini', 'entropy' and 'variance'")
  private Impurity decisionTreeImpurity;

  /** The max depth. */
  @TrainerParam(name = "maxDepth", help = "Max depth of the decision tree. Integer values expected.", defaultValue = "100")
  private int maxDepth;

  /**
   * Instantiates a new decision tree trainer.
   *
   * @param name
   *          the name
   * @param description
   *          the description
   */
  public DecisionTreeTrainer(String name, String description) {
    super(name, description);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.spark.trainers.BaseSparkTrainer#parseTrainerParams(java.util.Map)
   */
  @Override
  public void parseTrainerParams(Map<String, String> params) {
    String dtreeAlgoName = params.get("algo");
    if ("classification".equalsIgnoreCase(dtreeAlgoName)) {
      algo = Algo$.MODULE$.Classification();
    } else if ("regression".equalsIgnoreCase(dtreeAlgoName)) {
      algo = Algo$.MODULE$.Regression();
    }

    String impurity = params.get("impurity");
    if ("gini".equals(impurity)) {
      decisionTreeImpurity = Gini$.MODULE$;
    } else if ("entropy".equals(impurity)) {
      decisionTreeImpurity = Entropy$.MODULE$;
    } else if ("variance".equals(impurity)) {
      decisionTreeImpurity = Variance$.MODULE$;
    }

    maxDepth = getParamValue("maxDepth", 100);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.spark.trainers.BaseSparkTrainer#trainInternal(java.lang.String, org.apache.spark.rdd.RDD)
   */
  @Override
  protected BaseSparkClassificationModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD)
      throws LensException {
    DecisionTreeModel model = DecisionTree$.MODULE$.train(trainingRDD, algo, decisionTreeImpurity, maxDepth);
    return new DecisionTreeClassificationModel(modelId, new SparkDecisionTreeModel(model));
  }
}
