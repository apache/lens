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
package org.apache.lens.ml.spark.trainers;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;
import org.apache.lens.ml.spark.TableTrainingSpec;
import org.apache.lens.ml.spark.models.BaseSparkClassificationModel;
import org.apache.lens.ml.Algorithm;
import org.apache.lens.ml.MLModel;
import org.apache.lens.ml.MLTrainer;
import org.apache.lens.ml.TrainerParam;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.lang.reflect.Field;
import java.util.*;

/**
 * The Class BaseSparkTrainer.
 */
public abstract class BaseSparkTrainer implements MLTrainer {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(BaseSparkTrainer.class);

  /** The name. */
  private final String name;

  /** The description. */
  private final String description;

  /** The spark context. */
  protected JavaSparkContext sparkContext;

  /** The params. */
  protected Map<String, String> params;

  /** The conf. */
  protected transient LensConf conf;

  /** The training fraction. */
  @TrainerParam(name = "trainingFraction", help = "% of dataset to be used for training", defaultValue = "0")
  protected double trainingFraction;

  /** The use training fraction. */
  private boolean useTrainingFraction;

  /** The label. */
  @TrainerParam(name = "label", help = "Name of column which is used as a training label for supervised learning")
  protected String label;

  /** The partition filter. */
  @TrainerParam(name = "partition", help = "Partition filter used to create create HCatInputFormats")
  protected String partitionFilter;

  /** The features. */
  @TrainerParam(name = "feature", help = "Column name(s) which are to be used as sample features")
  protected List<String> features;

  /**
   * Instantiates a new base spark trainer.
   *
   * @param name
   *          the name
   * @param description
   *          the description
   */
  public BaseSparkTrainer(String name, String description) {
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
   * @see org.apache.lens.ml.MLTrainer#configure(org.apache.lens.api.LensConf)
   */
  @Override
  public void configure(LensConf configuration) {
    this.conf = configuration;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLTrainer#train(org.apache.lens.api.LensConf, java.lang.String, java.lang.String,
   * java.lang.String, java.lang.String[])
   */
  @Override
  public MLModel train(LensConf conf, String db, String table, String modelId, String... params) throws LensException {
    parseParams(params);
    LOG.info("Training " + " with " + features.size() + " features");
    TableTrainingSpec.TableTrainingSpecBuilder builder = TableTrainingSpec.newBuilder().hiveConf(toHiveConf(conf))
        .database(db).table(table).partitionFilter(partitionFilter).featureColumns(features).labelColumn(label);

    if (useTrainingFraction) {
      builder.trainingFraction(trainingFraction);
    }

    TableTrainingSpec spec = builder.build();
    spec.createRDDs(sparkContext);

    RDD<LabeledPoint> trainingRDD = spec.getTrainingRDD();
    BaseSparkClassificationModel model = trainInternal(modelId, trainingRDD);
    model.setTable(table);
    model.setParams(Arrays.asList(params));
    model.setLabelColumn(label);
    model.setFeatureColumns(features);
    return model;
  }

  /**
   * To hive conf.
   *
   * @param conf
   *          the conf
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
   * @param args
   *          the args
   */
  public void parseParams(String[] args) {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException("Invalid number of params " + args.length);
    }

    params = new LinkedHashMap<String, String>();

    for (int i = 0; i < args.length; i += 2) {
      if ("f".equalsIgnoreCase(args[i]) || "feature".equalsIgnoreCase(args[i])) {
        if (features == null) {
          features = new ArrayList<String>();
        }
        features.add(args[i + 1]);
      } else if ("l".equalsIgnoreCase(args[i]) || "label".equalsIgnoreCase(args[i])) {
        label = args[i + 1];
      } else {
        params.put(args[i].replaceAll("\\-+", ""), args[i + 1]);
      }
    }

    if (params.containsKey("trainingFraction")) {
      // Get training Fraction
      String trainingFractionStr = params.get("trainingFraction");
      try {
        trainingFraction = Double.parseDouble(trainingFractionStr);
        useTrainingFraction = true;
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("Invalid training fraction", nfe);
      }
    }

    if (params.containsKey("partition") || params.containsKey("p")) {
      partitionFilter = params.containsKey("partition") ? params.get("partition") : params.get("p");
    }

    parseTrainerParams(params);
  }

  /**
   * Gets the param value.
   *
   * @param param
   *          the param
   * @param defaultVal
   *          the default val
   * @return the param value
   */
  public double getParamValue(String param, double defaultVal) {
    if (params.containsKey(param)) {
      try {
        return Double.parseDouble(params.get(param));
      } catch (NumberFormatException nfe) {
      }
    }
    return defaultVal;
  }

  /**
   * Gets the param value.
   *
   * @param param
   *          the param
   * @param defaultVal
   *          the default val
   * @return the param value
   */
  public int getParamValue(String param, int defaultVal) {
    if (params.containsKey(param)) {
      try {
        return Integer.parseInt(params.get(param));
      } catch (NumberFormatException nfe) {
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
    // Put class name and description as well as part of the usage
    Algorithm algorithm = clz.getAnnotation(Algorithm.class);
    if (algorithm != null) {
      usage.put("Algorithm Name", algorithm.name());
      usage.put("Algorithm Description", algorithm.description());
    }

    // Get all trainer params including base trainer params
    while (clz != null) {
      for (Field field : clz.getDeclaredFields()) {
        TrainerParam param = field.getAnnotation(TrainerParam.class);
        if (param != null) {
          usage.put("[param] " + param.name(), param.help() + " Default Value = " + param.defaultValue());
        }
      }

      if (clz.equals(BaseSparkTrainer.class)) {
        break;
      }
      clz = clz.getSuperclass();
    }
    return usage;
  }

  /**
   * Parses the trainer params.
   *
   * @param params
   *          the params
   */
  public abstract void parseTrainerParams(Map<String, String> params);

  /**
   * Train internal.
   *
   * @param modelId
   *          the model id
   * @param trainingRDD
   *          the training rdd
   * @return the base spark classification model
   * @throws LensException
   *           the lens exception
   */
  protected abstract BaseSparkClassificationModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD)
      throws LensException;
}
