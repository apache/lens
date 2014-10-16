package org.apache.lens.ml;

import java.util.List;

public interface MultiPrediction {
  public List<LabelledPrediction> getPredictions();
}
