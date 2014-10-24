package org.apache.lens.ml.spark.models;

import org.apache.lens.ml.ClassifierBaseModel;
import org.apache.spark.mllib.classification.ClassificationModel;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * The Class BaseSparkClassificationModel.
 *
 * @param <MODEL>
 *          the generic type
 */
public class BaseSparkClassificationModel<MODEL extends ClassificationModel> extends ClassifierBaseModel {

  /** The model id. */
  private final String modelId;

  /** The spark model. */
  private final MODEL sparkModel;

  /**
   * Instantiates a new base spark classification model.
   *
   * @param modelId
   *          the model id
   * @param model
   *          the model
   */
  public BaseSparkClassificationModel(String modelId, MODEL model) {
    this.modelId = modelId;
    this.sparkModel = model;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLModel#predict(java.lang.Object[])
   */
  @Override
  public Double predict(Object... args) {
    return sparkModel.predict(Vectors.dense(getFeatureVector(args)));
  }

  @Override
  public String getId() {
    return modelId;
  }

}
