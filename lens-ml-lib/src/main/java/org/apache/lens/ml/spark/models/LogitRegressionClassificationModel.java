package org.apache.lens.ml.spark.models;

import org.apache.spark.mllib.classification.LogisticRegressionModel;

/**
 * The Class LogitRegressionClassificationModel.
 */
public class LogitRegressionClassificationModel extends BaseSparkClassificationModel<LogisticRegressionModel> {

  /**
   * Instantiates a new logit regression classification model.
   *
   * @param modelId
   *          the model id
   * @param model
   *          the model
   */
  public LogitRegressionClassificationModel(String modelId, LogisticRegressionModel model) {
    super(modelId, model);
  }
}
