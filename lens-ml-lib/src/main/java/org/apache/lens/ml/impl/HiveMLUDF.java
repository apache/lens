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
package org.apache.lens.ml.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lens.ml.algo.api.TrainedModel;
import org.apache.lens.ml.api.MLConfConstants;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.mapred.JobConf;

/**
 * Generic UDF to laod ML Models saved in HDFS and apply the model on list of columns passed as argument.
 * The feature list is expected to be key value pair. i.e. feature_name, feature_value
 */
@Description(name = "predict",
  value = "_FUNC_(algorithm, modelID, features...) - Run prediction algorithm with given "
    + "algorithm name, model ID and input feature columns")
public final class HiveMLUDF extends GenericUDF {

  /**
   * The Constant LOG.
   */
  public static final Log LOG = LogFactory.getLog(HiveMLUDF.class);
  /**
   * The conf.
   */
  private JobConf conf;
  /**
   * The soi.
   */
  private StringObjectInspector soi;
  /**
   * The doi.
   */
  private LazyDoubleObjectInspector doi;
  /**
   * The model.
   */
  private TrainedModel model;

  private HiveMLUDF() {
  }

  /**
   * Currently we only support double as the return value.
   *
   * @param objectInspectors the object inspectors
   * @return the object inspector
   * @throws UDFArgumentException the UDF argument exception
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
    // We require algo name, model id, modelInstance id and at least one feature name value pair
    String usage = "algo_name model_id, modelInstance_id [feature_name, feature_value]+ .";
    if (objectInspectors.length < 5) {
      throw new UDFArgumentLengthException(
        "Algo name, model ID, modelInstance ID and at least one feature name value pair should be passed to "
          + MLConfConstants.UDF_NAME + ". " + usage);
    }

    int numberOfFeatures = objectInspectors.length;
    if (numberOfFeatures % 2 == 0) {
      throw new UDFArgumentException(
        "The feature list should be even in length since it's key value pair. i.e. feature_name, feature_value" + ". "
          + usage);
    }

    LOG.info(MLConfConstants.UDF_NAME + " initialized");
    return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF#evaluate(org.apache.hadoop.hive.ql.udf.generic.GenericUDF.
   * DeferredObject[])
   */
  @Override
  public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    String algorithm = soi.getPrimitiveJavaObject(deferredObjects[0].get());
    String modelId = soi.getPrimitiveJavaObject(deferredObjects[1].get());
    String modelInstanceId = soi.getPrimitiveJavaObject(deferredObjects[2].get());
    Map<String, String> features = new HashMap();

    for (int i = 3; i < deferredObjects.length; i += 2) {
      try {
        String key = soi.getPrimitiveJavaObject(deferredObjects[i].get());
        LazyDouble lazyDouble = (LazyDouble) deferredObjects[i + 1].get();
        Double value = (lazyDouble == null) ? 0d : doi.get(lazyDouble);
        LOG.debug("key: " + key + ", value " + value);
        features.put(key, String.valueOf(value));
      } catch (Exception e) {
        LOG.error("Error Parsing feature pair");
        throw new HiveException(e.getMessage());
      }
    }

    try {
      if (model == null) {
        model = ModelLoader.loadModel(conf, algorithm, modelId, modelInstanceId);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }

    try {
      Object object = model.predict(features);
      return object;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF#getDisplayString(java.lang.String[])
   */
  @Override
  public String getDisplayString(String[] strings) {
    return MLConfConstants.UDF_NAME;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF#configure(org.apache.hadoop.hive.ql.exec.MapredContext)
   */
  @Override
  public void configure(MapredContext context) {
    super.configure(context);
    conf = context.getJobConf();
    soi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    doi = LazyPrimitiveObjectInspectorFactory.LAZY_DOUBLE_OBJECT_INSPECTOR;
    LOG.info(
      MLConfConstants.UDF_NAME + " configured. Model base dir path: " + conf.get(ModelLoader.MODEL_PATH_BASE_DIR));
  }
}
