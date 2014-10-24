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
import org.apache.lens.api.LensException;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The Class TableTrainingSpec.
 */
public class TableTrainingSpec implements Serializable {

  /** The Constant LOG. */
  public static final Log LOG = LogFactory.getLog(TableTrainingSpec.class);

  /** The training rdd. */
  @Getter
  private transient RDD<LabeledPoint> trainingRDD;

  /** The testing rdd. */
  @Getter
  private transient RDD<LabeledPoint> testingRDD;

  /** The database. */
  @Getter
  private String database;

  /** The table. */
  @Getter
  private String table;

  /** The partition filter. */
  @Getter
  private String partitionFilter;

  /** The feature columns. */
  @Getter
  private List<String> featureColumns;

  /** The label column. */
  @Getter
  private String labelColumn;

  /** The conf. */
  @Getter
  transient private HiveConf conf;

  // By default all samples are considered for training
  /** The split training. */
  private boolean splitTraining;

  /** The training fraction. */
  private double trainingFraction = 1.0;

  /** The label pos. */
  int labelPos;

  /** The feature positions. */
  int[] featurePositions;

  /** The num features. */
  int numFeatures;

  /** The labeled rdd. */
  transient JavaRDD<LabeledPoint> labeledRDD;

  /**
   * New builder.
   *
   * @return the table training spec builder
   */
  public static TableTrainingSpecBuilder newBuilder() {
    return new TableTrainingSpecBuilder();
  }

  /**
   * The Class TableTrainingSpecBuilder.
   */
  public static class TableTrainingSpecBuilder {

    /** The spec. */
    final TableTrainingSpec spec;

    /**
     * Instantiates a new table training spec builder.
     */
    public TableTrainingSpecBuilder() {
      spec = new TableTrainingSpec();
    }

    /**
     * Hive conf.
     *
     * @param conf
     *          the conf
     * @return the table training spec builder
     */
    public TableTrainingSpecBuilder hiveConf(HiveConf conf) {
      spec.conf = conf;
      return this;
    }

    /**
     * Database.
     *
     * @param db
     *          the db
     * @return the table training spec builder
     */
    public TableTrainingSpecBuilder database(String db) {
      spec.database = db;
      return this;
    }

    /**
     * Table.
     *
     * @param table
     *          the table
     * @return the table training spec builder
     */
    public TableTrainingSpecBuilder table(String table) {
      spec.table = table;
      return this;
    }

    /**
     * Partition filter.
     *
     * @param partFilter
     *          the part filter
     * @return the table training spec builder
     */
    public TableTrainingSpecBuilder partitionFilter(String partFilter) {
      spec.partitionFilter = partFilter;
      return this;
    }

    /**
     * Label column.
     *
     * @param labelColumn
     *          the label column
     * @return the table training spec builder
     */
    public TableTrainingSpecBuilder labelColumn(String labelColumn) {
      spec.labelColumn = labelColumn;
      return this;
    }

    /**
     * Feature columns.
     *
     * @param featureColumns
     *          the feature columns
     * @return the table training spec builder
     */
    public TableTrainingSpecBuilder featureColumns(List<String> featureColumns) {
      spec.featureColumns = featureColumns;
      return this;
    }

    /**
     * Builds the.
     *
     * @return the table training spec
     */
    public TableTrainingSpec build() {
      return spec;
    }

    /**
     * Training fraction.
     *
     * @param trainingFraction
     *          the training fraction
     * @return the table training spec builder
     */
    public TableTrainingSpecBuilder trainingFraction(double trainingFraction) {
      Preconditions.checkArgument(trainingFraction >= 0 && trainingFraction <= 1.0,
          "Training fraction shoule be between 0 and 1");
      spec.trainingFraction = trainingFraction;
      spec.splitTraining = true;
      return this;
    }
  }

  /**
   * The Class DataSample.
   */
  public static class DataSample implements Serializable {

    /** The labeled point. */
    private final LabeledPoint labeledPoint;

    /** The sample. */
    private final double sample;

    /**
     * Instantiates a new data sample.
     *
     * @param labeledPoint
     *          the labeled point
     */
    public DataSample(LabeledPoint labeledPoint) {
      sample = Math.random();
      this.labeledPoint = labeledPoint;
    }
  }

  /**
   * The Class TrainingFilter.
   */
  public static class TrainingFilter implements Function<DataSample, Boolean> {

    /** The training fraction. */
    private double trainingFraction;

    /**
     * Instantiates a new training filter.
     *
     * @param fraction
     *          the fraction
     */
    public TrainingFilter(double fraction) {
      trainingFraction = fraction;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    @Override
    public Boolean call(DataSample v1) throws Exception {
      return v1.sample <= trainingFraction;
    }
  }

  /**
   * The Class TestingFilter.
   */
  public static class TestingFilter implements Function<DataSample, Boolean> {

    /** The training fraction. */
    private double trainingFraction;

    /**
     * Instantiates a new testing filter.
     *
     * @param fraction
     *          the fraction
     */
    public TestingFilter(double fraction) {
      trainingFraction = fraction;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    @Override
    public Boolean call(DataSample v1) throws Exception {
      return v1.sample > trainingFraction;
    }
  }

  /**
   * The Class GetLabeledPoint.
   */
  public static class GetLabeledPoint implements Function<DataSample, LabeledPoint> {

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    @Override
    public LabeledPoint call(DataSample v1) throws Exception {
      return v1.labeledPoint;
    }
  }

  /**
   * Validate.
   *
   * @return true, if successful
   */
  boolean validate() {
    List<FieldSchema> columns;
    try {
      Hive metastoreClient = Hive.get(conf);
      Table tbl = (database == null) ? metastoreClient.getTable(table) : metastoreClient.getTable(database, table);
      columns = tbl.getAllCols();
    } catch (HiveException exc) {
      LOG.error("Error getting table info " + toString(), exc);
      return false;
    }

    boolean valid = false;
    if (columns != null && !columns.isEmpty()) {
      // Check labeled column
      List<String> columnNames = new ArrayList<String>();
      for (FieldSchema col : columns) {
        columnNames.add(col.getName());
      }

      // Need at least one feature column and one label column
      valid = columnNames.contains(labelColumn) && columnNames.size() > 1;

      if (valid) {
        labelPos = columnNames.indexOf(labelColumn);

        // Check feature columns
        if (featureColumns == null || featureColumns.isEmpty()) {
          // feature columns are not provided, so all columns except label column are feature columns
          featurePositions = new int[columnNames.size() - 1];
          int p = 0;
          for (int i = 0; i < columnNames.size(); i++) {
            if (i == labelPos) {
              continue;
            }
            featurePositions[p++] = i;
          }

          columnNames.remove(labelPos);
          featureColumns = columnNames;
        } else {
          // Feature columns were provided, verify all feature columns are present in the table
          valid = columnNames.containsAll(featureColumns);
          if (valid) {
            // Get feature positions
            featurePositions = new int[featureColumns.size()];
            for (int i = 0; i < featureColumns.size(); i++) {
              featurePositions[i] = columnNames.indexOf(featureColumns.get(i));
            }
          }
        }
        numFeatures = featureColumns.size();
      }
    }

    return valid;
  }

  /**
   * Creates the rd ds.
   *
   * @param sparkContext
   *          the spark context
   * @throws LensException
   *           the lens exception
   */
  public void createRDDs(JavaSparkContext sparkContext) throws LensException {
    // Validate the spec
    if (!validate()) {
      throw new LensException("Table spec not valid: " + toString());
    }

    // Get the RDD for table
    JavaPairRDD<WritableComparable, HCatRecord> tableRDD;
    try {
      tableRDD = HiveTableRDD.createHiveTableRDD(sparkContext, conf, database, table, partitionFilter);
    } catch (IOException e) {
      throw new LensException(e);
    }

    // Map into trainable RDD
    // TODO: Figure out a way to use custom value mappers
    FeatureValueMapper[] valueMappers = new FeatureValueMapper[numFeatures];
    final DoubleValueMapper doubleMapper = new DoubleValueMapper();
    for (int i = 0; i < numFeatures; i++) {
      valueMappers[i] = doubleMapper;
    }

    ColumnFeatureFunction trainPrepFunction = new ColumnFeatureFunction(featurePositions, valueMappers, labelPos,
        numFeatures, 0);
    labeledRDD = tableRDD.map(trainPrepFunction);

    if (splitTraining) {
      // We have to split the RDD between a training RDD and a testing RDD
      LOG.info("Splitting RDD for table " + database + "." + table + " with split fraction " + trainingFraction);
      JavaRDD<DataSample> sampledRDD = labeledRDD.map(new Function<LabeledPoint, DataSample>() {
        @Override
        public DataSample call(LabeledPoint v1) throws Exception {
          return new DataSample(v1);
        }
      });

      trainingRDD = sampledRDD.filter(new TrainingFilter(trainingFraction)).map(new GetLabeledPoint()).rdd();
      testingRDD = sampledRDD.filter(new TestingFilter(trainingFraction)).map(new GetLabeledPoint()).rdd();
    } else {
      LOG.info("Using same RDD for train and test");
      trainingRDD = testingRDD = labeledRDD.rdd();
    }
    LOG.info("Generated RDDs");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return StringUtils.join(new String[] { database, table, partitionFilter, labelColumn }, ",");
  }
}
