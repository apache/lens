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

@Algorithm(
  name = "spark_naive_bayes",
  description = "Spark Naive Bayes classifier trainer"
)
public class NaiveBayesTrainer extends BaseSparkTrainer {
  @TrainerParam(name = "lambda", help = "Lambda parameter for naive bayes learner",
  defaultValue = "1.0d")
  private double lambda = 1.0;

  public NaiveBayesTrainer(String name, String description) {
    super(name, description);
  }

  @Override
  public void parseTrainerParams(Map<String, String> params) {
    lambda = getParamValue("lambda", 1.0d);
  }

  @Override
  protected BaseSparkClassificationModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD) throws LensException {
    return new NaiveBayesClassificationModel(modelId, NaiveBayes.train(trainingRDD, lambda));
  }
}
