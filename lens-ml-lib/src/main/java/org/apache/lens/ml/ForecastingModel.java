/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.ml;

import java.util.List;

/**
 * The Class ForecastingModel.
 */
public class ForecastingModel extends MLModel<MultiPrediction> {

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLModel#predict(java.lang.Object[])
   */
  @Override
  public MultiPrediction predict(Object... args) {
    return new ForecastingPredictions(null);
  }

  /**
   * The Class ForecastingPredictions.
   */
  public static class ForecastingPredictions implements MultiPrediction {

    /** The values. */
    private final List<LabelledPrediction> values;

    /**
     * Instantiates a new forecasting predictions.
     *
     * @param values
     *          the values
     */
    public ForecastingPredictions(List<LabelledPrediction> values) {
      this.values = values;
    }

    @Override
    public List<LabelledPrediction> getPredictions() {
      return values;
    }
  }

  /**
   * The Class ForecastingLabel.
   */
  public static class ForecastingLabel implements LabelledPrediction<Long, Double> {

    /** The timestamp. */
    private final Long timestamp;

    /** The value. */
    private final double value;

    /**
     * Instantiates a new forecasting label.
     *
     * @param timestamp
     *          the timestamp
     * @param value
     *          the value
     */
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
