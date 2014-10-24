package org.apache.lens.ml.spark.models;

import org.apache.lens.ml.MLModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * The Class KMeansClusteringModel.
 */
public class KMeansClusteringModel extends MLModel<Integer> {

  /** The model. */
  private final KMeansModel model;

  /** The model id. */
  private final String modelId;

  /**
   * Instantiates a new k means clustering model.
   *
   * @param modelId
   *          the model id
   * @param model
   *          the model
   */
  public KMeansClusteringModel(String modelId, KMeansModel model) {
    this.model = model;
    this.modelId = modelId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLModel#predict(java.lang.Object[])
   */
  @Override
  public Integer predict(Object... args) {
    // Convert the params to array of double
    double[] arr = new double[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i] != null) {
        arr[i] = (Double) args[i];
      } else {
        arr[i] = 0d;
      }
    }

    return model.predict(Vectors.dense(arr));
  }
}
