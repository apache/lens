package org.apache.lens.ml;

/**
 * Prediction type used when the model prediction is of complex types. For example,
 * in forecasting the predictions are a series of timestamp, and value pairs.
 * @param <LABELTYPE>
 * @param <PREDICTIONTYPE>
 */
public interface LabelledPrediction<LABELTYPE, PREDICTIONTYPE> {
  public LABELTYPE getLabel();
  public PREDICTIONTYPE getPrediction();
}
