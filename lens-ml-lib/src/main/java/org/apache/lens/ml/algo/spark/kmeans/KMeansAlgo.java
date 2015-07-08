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
package org.apache.lens.ml.algo.spark.kmeans;

import java.util.ArrayList;
import java.util.List;

import org.apache.lens.api.LensConf;
import org.apache.lens.ml.algo.api.Algorithm;
import org.apache.lens.ml.algo.api.TrainedModel;
import org.apache.lens.ml.algo.lib.AlgoArgParser;
import org.apache.lens.ml.algo.spark.HiveTableRDD;
import org.apache.lens.ml.api.AlgoParam;
import org.apache.lens.ml.api.DataSet;
import org.apache.lens.ml.api.Feature;
import org.apache.lens.ml.api.Model;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;


/**
 * The Class KMeansAlgo.
 */
public class KMeansAlgo implements Algorithm {

  static String description = "Spark K means algo";
  static String name = "spark_k_means";

  /**
   * The conf.
   */
  private transient LensConf conf;

  /**
   * The spark context.
   */
  private JavaSparkContext sparkContext;

  /**
   * The part filter.
   */
  @AlgoParam(name = "partition", help = "Partition filter to be used while constructing table RDD")
  private String partFilter = null;

  /**
   * The k.
   */
  @AlgoParam(name = "k", help = "Number of cluster")
  private int k;

  /**
   * The max iterations.
   */
  @AlgoParam(name = "maxIterations", help = "Maximum number of iterations", defaultValue = "100")
  private int maxIterations = 100;

  /**
   * The runs.
   */
  @AlgoParam(name = "runs", help = "Number of parallel run", defaultValue = "1")
  private int runs = 1;

  /**
   * The initialization mode.
   */
  @AlgoParam(name = "initializationMode",
    help = "initialization model, either \"random\" or \"k-means||\" (default).", defaultValue = "k-means||")
  private String initializationMode = "k-means||";

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
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

  @Override
  public LensConf getConf() {
    return conf;
  }


  /**
   * To hive conf.
   *
   * @param conf the conf
   * @return the hive conf
   */
  private HiveConf toHiveConf(LensConf conf) {
    HiveConf hiveConf = new HiveConf();
    for (String key : conf.getProperties().keySet()) {
      hiveConf.set(key, conf.getProperties().get(key));
    }
    return hiveConf;
  }

  @Override
  public List<AlgoParam> getParams() {
    return null;
  }

  @Override
  public TrainedModel train(Model model, DataSet trainingDataSet) throws LensException {
    AlgoArgParser.parseArgs(this, model.getAlgoSpec().getAlgoParams());
    List<String> features = new ArrayList<>();
    for (Feature feature : model.getFeatureSpec()) {
      features.add(feature.getDataColumn());
    }

    final int[] featurePositions = new int[features.size()];
    final int NUM_FEATURES = features.size();

    JavaPairRDD<WritableComparable, HCatRecord> rdd = null;

    try {
      // Map feature names to positions
      Table tbl = Hive.get(toHiveConf(conf)).getTable(trainingDataSet.getDatabase(), trainingDataSet.getTableName());
      List<FieldSchema> allCols = tbl.getAllCols();
      int f = 0;
      for (int i = 0; i < tbl.getAllCols().size(); i++) {
        String colName = allCols.get(i).getName();
        if (features.contains(colName)) {
          featurePositions[f++] = i;
        }
      }

      rdd = HiveTableRDD.createHiveTableRDD(sparkContext, toHiveConf(conf), trainingDataSet.getDatabase(),
        trainingDataSet.getTableName(), partFilter);
      JavaRDD<Vector> trainableRDD = rdd.map(new Function<Tuple2<WritableComparable, HCatRecord>, Vector>() {
        @Override
        public Vector call(Tuple2<WritableComparable, HCatRecord> v1) throws Exception {
          HCatRecord hCatRecord = v1._2();
          double[] arr = new double[NUM_FEATURES];
          for (int i = 0; i < NUM_FEATURES; i++) {
            Object val = hCatRecord.get(featurePositions[i]);
            arr[i] = val == null ? 0d : (Double) val;
          }
          return Vectors.dense(arr);
        }
      });

      KMeansModel kMeansModel = KMeans.train(trainableRDD.rdd(), k, maxIterations, runs, initializationMode);
      return new KMeansClusteringModel(model.getFeatureSpec(), kMeansModel);
    } catch (Exception e) {
      throw new LensException(
        "KMeans algo failed for " + trainingDataSet.getDatabase() + "." + trainingDataSet.getTableName(), e);
    }
  }
}
