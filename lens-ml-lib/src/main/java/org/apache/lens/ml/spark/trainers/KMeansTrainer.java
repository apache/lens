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
import org.apache.lens.ml.*;
import org.apache.lens.ml.spark.HiveTableRDD;
import org.apache.lens.ml.spark.models.KMeansClusteringModel;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.lens.ml.Algorithm;
import org.apache.lens.ml.MLTrainer;
import org.apache.lens.ml.TrainerParam;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.util.List;

/**
 * The Class KMeansTrainer.
 */
@Algorithm(name = "spark_kmeans_trainer", description = "Spark MLLib KMeans trainer")
public class KMeansTrainer implements MLTrainer {

  /** The conf. */
  private transient LensConf conf;

  /** The spark context. */
  private JavaSparkContext sparkContext;

  /** The part filter. */
  @TrainerParam(name = "partition", help = "Partition filter to be used while constructing table RDD")
  private String partFilter = null;

  /** The k. */
  @TrainerParam(name = "k", help = "Number of cluster")
  private int k;

  /** The max iterations. */
  @TrainerParam(name = "maxIterations", help = "Maximum number of iterations", defaultValue = "100")
  private int maxIterations = 100;

  /** The runs. */
  @TrainerParam(name = "runs", help = "Number of parallel run", defaultValue = "1")
  private int runs = 1;

  /** The initialization mode. */
  @TrainerParam(name = "initializationMode", help = "initialization model, either \"random\" or \"k-means||\" (default).", defaultValue = "k-means||")
  private String initializationMode = "k-means||";

  @Override
  public String getName() {
    return getClass().getAnnotation(Algorithm.class).name();
  }

  @Override
  public String getDescription() {
    return getClass().getAnnotation(Algorithm.class).description();
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

  @Override
  public LensConf getConf() {
    return conf;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lens.ml.MLTrainer#train(org.apache.lens.api.LensConf, java.lang.String, java.lang.String,
   * java.lang.String, java.lang.String[])
   */
  @Override
  public MLModel train(LensConf conf, String db, String table, String modelId, String... params) throws LensException {
    List<String> features = TrainerArgParser.parseArgs(this, params);
    final int featurePositions[] = new int[features.size()];
    final int NUM_FEATURES = features.size();

    JavaPairRDD<WritableComparable, HCatRecord> rdd = null;
    try {
      // Map feature names to positions
      Table tbl = Hive.get(toHiveConf(conf)).getTable(db, table);
      List<FieldSchema> allCols = tbl.getAllCols();
      int f = 0;
      for (int i = 0; i < tbl.getAllCols().size(); i++) {
        String colName = allCols.get(i).getName();
        if (features.contains(colName)) {
          featurePositions[f++] = i;
        }
      }

      rdd = HiveTableRDD.createHiveTableRDD(sparkContext, toHiveConf(conf), db, table, partFilter);
      JavaRDD<Vector> trainableRDD = rdd.map(new Function<Tuple2<WritableComparable, HCatRecord>, Vector>() {
        @Override
        public Vector call(Tuple2<WritableComparable, HCatRecord> v1) throws Exception {
          HCatRecord hCatRecord = v1._2();
          double arr[] = new double[NUM_FEATURES];
          for (int i = 0; i < NUM_FEATURES; i++) {
            Object val = hCatRecord.get(featurePositions[i]);
            arr[i] = val == null ? 0d : (Double) val;
          }
          return Vectors.dense(arr);
        }
      });

      KMeansModel model = KMeans.train(trainableRDD.rdd(), k, maxIterations, runs, initializationMode);
      return new KMeansClusteringModel(modelId, model);
    } catch (Exception e) {
      throw new LensException("KMeans trainer failed for " + db + "." + table, e);
    }
  }

  /**
   * To hive conf.
   *
   * @param conf
   *          the conf
   * @return the hive conf
   */
  private HiveConf toHiveConf(LensConf conf) {
    HiveConf hiveConf = new HiveConf();
    for (String key : conf.getProperties().keySet()) {
      hiveConf.set(key, conf.getProperties().get(key));
    }
    return hiveConf;
  }
}
