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
package org.apache.lens.ml.spark;

import com.google.common.base.Preconditions;
import org.apache.lens.ml.spark.FeatureFunction;
import org.apache.lens.ml.spark.FeatureValueMapper;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.log4j.Logger;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

/**
 * A feature function that directly maps an HCatRecord to a feature vector. Each column becomes a feature in the vector,
 * with the value of the feature obtained using the value mapper for that column
 */
public class ColumnFeatureFunction extends FeatureFunction {

  /** The Constant LOG. */
  public static final Logger LOG = Logger.getLogger(ColumnFeatureFunction.class);

  /** The feature value mappers. */
  private final FeatureValueMapper[] featureValueMappers;

  /** The feature positions. */
  private final int[] featurePositions;

  /** The label column pos. */
  private final int labelColumnPos;

  /** The num features. */
  private final int numFeatures;

  /** The default labeled point. */
  private final LabeledPoint defaultLabeledPoint;

  /**
   * Feature positions and value mappers are parallel arrays. featurePositions[i] gives the position of ith feature in
   * the HCatRecord, and valueMappers[i] gives the value mapper used to map that feature to a Double value
   * 
   * @param featurePositions
   *          position number of feature column in the HCatRecord
   * @param valueMappers
   *          mapper for each column position
   * @param labelColumnPos
   *          position of the label column
   * @param numFeatures
   *          number of features in the feature vector
   * @param defaultLabel
   *          default lable to be used for null records
   */
  public ColumnFeatureFunction(int featurePositions[], FeatureValueMapper valueMappers[], int labelColumnPos,
      int numFeatures, double defaultLabel) {
    Preconditions.checkNotNull(valueMappers, "Value mappers argument is required");
    Preconditions.checkNotNull(featurePositions, "Feature positions are required");
    Preconditions.checkArgument(valueMappers.length == featurePositions.length,
        "Mismatch between number of value mappers and feature positions");

    this.featurePositions = featurePositions;
    this.featureValueMappers = valueMappers;
    this.labelColumnPos = labelColumnPos;
    this.numFeatures = numFeatures;
    defaultLabeledPoint = new LabeledPoint(defaultLabel, Vectors.dense(new double[numFeatures]));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.spark.FeatureFunction#call(scala.Tuple2)
   */
  @Override
  public LabeledPoint call(Tuple2<WritableComparable, HCatRecord> tuple) throws Exception {
    HCatRecord record = tuple._2();

    if (record == null) {
      LOG.info("@@@ Null record");
      return defaultLabeledPoint;
    }

    double features[] = new double[numFeatures];

    for (int i = 0; i < numFeatures; i++) {
      int featurePos = featurePositions[i];
      features[i] = featureValueMappers[i].call(record.get(featurePos));
    }

    double label = featureValueMappers[labelColumnPos].call(record.get(labelColumnPos));
    return new LabeledPoint(label, Vectors.dense(features));
  }
}
