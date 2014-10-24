package org.apache.lens.ml.spark.models;

import org.apache.spark.mllib.classification.NaiveBayesModel;

/**
 * The Class NaiveBayesClassificationModel.
 */
public class NaiveBayesClassificationModel extends BaseSparkClassificationModel<NaiveBayesModel> {

  /**
   * Instantiates a new naive bayes classification model.
   *
   * @param modelId
   *          the model id
   * @param model
   *          the model
   */
  public NaiveBayesClassificationModel(String modelId, NaiveBayesModel model) {
    super(modelId, model);
  }
}
