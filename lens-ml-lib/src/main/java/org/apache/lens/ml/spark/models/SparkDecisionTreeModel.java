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
