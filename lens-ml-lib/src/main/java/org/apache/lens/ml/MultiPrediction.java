package org.apache.lens.ml;

import java.util.List;

/**
 * The Interface MultiPrediction.
 */
public interface MultiPrediction {
  public List<LabelledPrediction> getPredictions();
}
