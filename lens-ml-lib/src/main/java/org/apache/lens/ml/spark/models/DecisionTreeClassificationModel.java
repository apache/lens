package org.apache.lens.ml.spark.models;

/**
 * The Class DecisionTreeClassificationModel.
 */
public class DecisionTreeClassificationModel extends BaseSparkClassificationModel<SparkDecisionTreeModel> {

  /**
   * Instantiates a new decision tree classification model.
   *
   * @param modelId
   *          the model id
   * @param model
   *          the model
   */
  public DecisionTreeClassificationModel(String modelId, SparkDecisionTreeModel model) {
    super(modelId, model);
  }
}
