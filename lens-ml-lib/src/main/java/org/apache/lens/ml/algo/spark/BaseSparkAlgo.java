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
package org.apache.lens.ml.algo.spark;

import java.lang.reflect.Field;
import java.util.*;

import org.apache.lens.api.LensConf;
import org.apache.lens.ml.algo.api.Algorithm;
import org.apache.lens.ml.algo.api.TrainedModel;
import org.apache.lens.ml.api.AlgoParam;
import org.apache.lens.ml.api.DataSet;
import org.apache.lens.ml.api.Feature;
import org.apache.lens.ml.api.Model;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

/**
 * The Class BaseSparkAlgo.
 */
public abstract class BaseSparkAlgo implements Algorithm {

  /**
   * The Constant LOG.
   */
  public static final Log LOG = LogFactory.getLog(BaseSparkAlgo.class);

  /**
   * The name.
   */
  private final String name;

  /**
   * The description.
   */
  private final String description;

  /**
   * The spark context.
   */
  protected JavaSparkContext sparkContext;

  /**
   * The params.
   */
  protected Map<String, String> params;

  /**
   * The conf.
   */
  protected transient LensConf conf;

  /**
   * The training fraction.
   */
  @AlgoParam(name = "trainingFraction", help = "% of dataset to be used for training", defaultValue = "0")
  protected double trainingFraction;
  /**
   * The label.
   */
  @AlgoParam(name = "label", help = "column name, feature name which is used as a training label for supervised "
    + "learning")
  protected Feature label;
  /**
   * The partition filter.
   */
  @AlgoParam(name = "partition", help = "Partition filter used to create create HCatInputFormats")
  protected String partitionFilter;
  /**
   * The features.
   */
  @AlgoParam(name = "feature", help = "sample features containing feature name and column name")
  protected List<Feature> features;
  /**
   * The use training fraction.
   */
  private boolean useTrainingFraction;

  /**
   * Instantiates a new base spark algo.
   *
   * @param name        the name
   * @param description the description
   */
  public BaseSparkAlgo(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public void setSparkContext(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  @Override
  public LensConf getConf() {
    return conf;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.MLAlgo#configure(org.apache.lens.api.LensConf)
   */
  @Override
  public void configure(LensConf configuration) {
    this.conf = configuration;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lens.ml.TrainedModel#train(Model model, String dataTable)
   */
  @Override
  public TrainedModel train(Model model, DataSet dataTable) throws LensException {
    parseParams(model.getAlgoSpec().getAlgoParams());
    features = model.getFeatureSpec();
    String database = dataTable.getDatabase();
    if (database.isEmpty()) {
      if (SessionState.get() != null) {
        database = SessionState.get().getCurrentDatabase();
      } else {
        database = "default";
      }
    }


    TableTrainingSpec.TableTrainingSpecBuilder builder = TableTrainingSpec.newBuilder().hiveConf(toHiveConf(conf))
      .database(database).table(dataTable.getTableName()).partitionFilter(partitionFilter)
      .featureColumns(model.getFeatureSpec())
      .labelColumn(model.getLabelSpec());
    if (useTrainingFraction) {
      builder.trainingFraction(trainingFraction);
    }

    TableTrainingSpec spec = builder.build();
    LOG.info("Training " + " with " + features.size() + " features");

    spec.createRDDs(sparkContext);

    RDD<LabeledPoint> trainingRDD = spec.getTrainingRDD();
    BaseSparkClassificationModel<?> trainedModel = trainInternal(trainingRDD);
    return trainedModel;
  }

  /**
   * To hive conf.
   *
   * @param conf the conf
   * @return the hive conf
   */
  protected HiveConf toHiveConf(LensConf conf) {
    HiveConf hiveConf = new HiveConf();
    for (String key : conf.getProperties().keySet()) {
      hiveConf.set(key, conf.getProperties().get(key));
    }
    return hiveConf;
  }

  /**
   * Parses the params.
   *
   * @param args the args
   */

  public void parseParams(Map<String, String> args) {

    params = new HashMap();

    if (args.containsKey("trainingFraction")) {
      String trainingFractionStr = args.get("trainingFraction");
      try {
        trainingFraction = Double.parseDouble(trainingFractionStr);
        useTrainingFraction = true;
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Invalid training fraction", nfe);
      }
    }
    if (args.containsKey("partition") || args.containsKey("p")) {
      partitionFilter = args.containsKey("partition") ? args.get("partition") : args.get("p");
    }

    parseAlgoParams(args);
  }

  /**
   * Gets the param value.
   *
   * @param param      the param
   * @param defaultVal the default val
   * @return the param value
   */
  public double getParamValue(String param, double defaultVal) {
    if (params.containsKey(param)) {
      try {
        return Double.parseDouble(params.get(param));
      } catch (NumberFormatException nfe) {
        LOG.warn("Couldn't parse param value: " + param + " as double.");
      }
    }
    return defaultVal;
  }

  /**
   * Gets the param value.
   *
   * @param param      the param
   * @param defaultVal the default val
   * @return the param value
   */
  public int getParamValue(String param, int defaultVal) {
    if (params.containsKey(param)) {
      try {
        return Integer.parseInt(params.get(param));
      } catch (NumberFormatException nfe) {
        LOG.warn("Couldn't parse param value: " + param + " as integer.");
      }
    }
    return defaultVal;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, String> getArgUsage() {
    Map<String, String> usage = new LinkedHashMap<String, String>();
    Class<?> clz = this.getClass();
    usage.put("Algorithm Name", name);
    usage.put("Algorithm Description", description);

    // Get all algo params including base algo params
    while (clz != null) {
      for (Field field : clz.getDeclaredFields()) {
        AlgoParam param = field.getAnnotation(AlgoParam.class);
        if (param != null) {
          usage.put("[param] " + param.name(), param.help() + " Default Value = " + param.defaultValue());
        }
      }

      if (clz.equals(BaseSparkAlgo.class)) {
        break;
      }
      clz = clz.getSuperclass();
    }
    return usage;
  }

  /**
   * Parses the algo params.
   *
   * @param params the params
   */
  public abstract void parseAlgoParams(Map<String, String> params);

  /**
   * Train internal.
   *
   * @param trainingRDD the training rdd
   * @return the base spark classification model
   * @throws LensException the lens exception
   */
  protected abstract BaseSparkClassificationModel trainInternal(RDD<LabeledPoint> trainingRDD)
    throws LensException;

  @Override
  public List<AlgoParam> getParams() {
    ArrayList<AlgoParam> paramList = new ArrayList();
    for (Field field : this.getClass().getDeclaredFields()) {
      AlgoParam param = field.getAnnotation(AlgoParam.class);
      if (param != null) {
        paramList.add(param);
      }
    }
    return paramList;
  }
}
