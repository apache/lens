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

public abstract class BaseSparkTrainer implements MLTrainer {
  public static final Log LOG = LogFactory.getLog(BaseSparkTrainer.class);

  private final String name;
  private final String description;

  protected JavaSparkContext sparkContext;
  protected Map<String, String> params;
  protected transient LensConf conf;

  @TrainerParam(name = "trainingFraction", help = "% of dataset to be used for training",
      defaultValue = "0")
  protected double trainingFraction;

  private boolean useTrainingFraction;

  @TrainerParam(name = "label", help = "Name of column which is used as a training label for supervised learning")
  protected String label;

  @TrainerParam(name = "partition", help = "Partition filter used to create create HCatInputFormats")
  protected String partitionFilter;

  @TrainerParam(name = "feature", help = "Column name(s) which are to be used as sample features")
  protected List<String> features;

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

  @Override
  public void configure(LensConf configuration) {
    this.conf = configuration;
  }


  @Override
  public MLModel train(LensConf conf, String db, String table, String modelId, String... params)
      throws LensException {
    parseParams(params);
    LOG.info("Training " + " with " + features.size() + " features");
    TableTrainingSpec.TableTrainingSpecBuilder builder =
        TableTrainingSpec.newBuilder()
        .hiveConf(toHiveConf(conf))
        .database(db)
        .table(table)
        .partitionFilter(partitionFilter)
        .featureColumns(features)
        .labelColumn(label);

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

  protected HiveConf toHiveConf(LensConf conf) {
    HiveConf hiveConf = new HiveConf();
    for (String key : conf.getProperties().keySet()) {
      hiveConf.set(key, conf.getProperties().get(key));
    }
    return hiveConf;
  }

  public void parseParams(String[] args) {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException("Invalid number of params " + args.length);
    }

    params = new LinkedHashMap<String, String>();

    for (int i = 0; i < args.length; i+=2) {
      if ("f".equalsIgnoreCase(args[i]) || "feature".equalsIgnoreCase(args[i])) {
        if (features == null) {
          features = new ArrayList<String>();
        }
        features.add(args[i+1]);
      } else if ("l".equalsIgnoreCase(args[i]) || "label".equalsIgnoreCase(args[i])) {
        label = args[i+1];
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

  public double getParamValue(String param, double defaultVal) {
    if (params.containsKey(param)) {
      try {
        return Double.parseDouble(params.get(param));
      } catch (NumberFormatException nfe) {
      }
    }
    return defaultVal;
  }

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
          usage.put("[param] " + param.name(), param.help() + " Default Value = "
              + param.defaultValue());
        }
      }

      if (clz.equals(BaseSparkTrainer.class)) {
        break;
      }
      clz = clz.getSuperclass();
    }
    return usage;
  }

  public abstract void parseTrainerParams(Map<String, String> params);
  protected abstract BaseSparkClassificationModel trainInternal(String modelId, RDD<LabeledPoint> trainingRDD) throws LensException;
}
