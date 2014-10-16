/*
 * #%L
 * Lens ML Lib
 * %%
 * Copyright (C) 2014 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package org.apache.lens.ml;


import java.util.List;

public class ForecastingModel extends MLModel<MultiPrediction> {

  @Override
  public MultiPrediction predict(Object... args) {
    return new ForecastingPredictions(null);
  }

  public static class ForecastingPredictions implements MultiPrediction {
    private final List<LabelledPrediction> values;

    public ForecastingPredictions(List<LabelledPrediction> values) {
      this.values = values;
    }

    @Override
    public List<LabelledPrediction> getPredictions() {
      return values;
    }
  }

  public static class ForecastingLabel implements LabelledPrediction<Long, Double> {
    private final Long timestamp;
    private final double value;

    public ForecastingLabel(long timestamp, double value) {
      this.timestamp = timestamp;
      this.value = value;
    }

    @Override
    public Long getLabel() {
      return timestamp;
    }

    @Override
    public Double getPrediction() {
      return value;
    }
  }
}
