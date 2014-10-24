package org.apache.lens.ml;

/**
 * Prediction type used when the model prediction is of complex types. For example, in forecasting the predictions are a
 * series of timestamp, and value pairs.
 *
 * @param <LABELTYPE>
 *          the generic type
 * @param <PREDICTIONTYPE>
 *          the generic type
 */
public interface LabelledPrediction<LABELTYPE, PREDICTIONTYPE> {
  public LABELTYPE getLabel();

  public PREDICTIONTYPE getPrediction();
}
