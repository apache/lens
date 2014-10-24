package org.apache.lens.ml.spark.models;

import org.apache.spark.mllib.classification.SVMModel;

/**
 * The Class SVMClassificationModel.
 */
public class SVMClassificationModel extends BaseSparkClassificationModel<SVMModel> {

  /**
   * Instantiates a new SVM classification model.
   *
   * @param modelId
   *          the model id
   * @param model
   *          the model
   */
  public SVMClassificationModel(String modelId, SVMModel model) {
    super(modelId, model);
  }
}
