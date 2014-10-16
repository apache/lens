package org.apache.lens.ml.spark.models;

import org.apache.lens.ml.ClassifierBaseModel;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.linalg.Vectors;

public class BaseSparkClassificationModel<MODEL extends ClassificationModel> extends ClassifierBaseModel {
  private final String modelId;
  private final MODEL sparkModel;

  public BaseSparkClassificationModel(String modelId, MODEL model) {
    this.modelId = modelId;
    this.sparkModel = model;
  }

  @Override
  public Double predict(Object... args) {
    return sparkModel.predict(Vectors.dense(getFeatureVector(args)));
  }

  @Override
  public String getId() {
    return modelId;
  }

}